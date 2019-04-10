package delete

import (
	"os"

	"github.com/weaveworks/eksctl/pkg/cfn/manager"

	"github.com/kris-nova/logger"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	api "github.com/weaveworks/eksctl/pkg/apis/eksctl.io/v1alpha4"
	"github.com/weaveworks/eksctl/pkg/authconfigmap"
	"github.com/weaveworks/eksctl/pkg/ctl/cmdutils"
	"github.com/weaveworks/eksctl/pkg/drain"
	"github.com/weaveworks/eksctl/pkg/eks"
)

var (
	updateAuthConfigMap  bool
	deleteNodeGroupDrain bool

	includeNodeGroups []string
	excludeNodeGroups []string

	deleteOnlyMissingNodeGroups bool
)

func deleteNodeGroupCmd(g *cmdutils.Grouping) *cobra.Command {
	p := &api.ProviderConfig{}
	cfg := api.NewClusterConfig()
	ng := cfg.NewNodeGroup()

	cmd := &cobra.Command{
		Use:     "nodegroup",
		Short:   "Delete a nodegroup",
		Aliases: []string{"ng"},
		Run: func(cmd *cobra.Command, args []string) {
			if err := doDeleteNodeGroup(p, cfg, ng, cmdutils.GetNameArg(args), cmd); err != nil {
				logger.Critical("%s\n", err.Error())
				os.Exit(1)
			}
		},
	}

	group := g.New(cmd)

	group.InFlagSet("General", func(fs *pflag.FlagSet) {
		fs.StringVar(&cfg.Metadata.Name, "cluster", "", "EKS cluster name")
		cmdutils.AddRegionFlag(fs, p)
		fs.StringVarP(&ng.Name, "name", "n", "", "Name of the nodegroup to delete")
		cmdutils.AddConfigFileFlag(&clusterConfigFile, fs)
		cmdutils.AddNodeGroupFilterFlags(&includeNodeGroups, &excludeNodeGroups, fs)
		fs.BoolVar(&dryRun, "dry-run", dryRun, "Do not delete any nodegroups, only show what would be done")
		fs.BoolVar(&deleteOnlyMissingNodeGroups, "only-missing", false, "Only delete nodegroups that are missing from the given config file")
		cmdutils.AddUpdateAuthConfigMap(&updateAuthConfigMap, fs, "Remove nodegroup IAM role from aws-auth configmap")
		fs.BoolVar(&deleteNodeGroupDrain, "drain", true, "Drain and cordon all nodes in the nodegroup before deletion")
		cmdutils.AddWaitFlag(&wait, fs, "deletion of all resources")
	})

	cmdutils.AddCommonFlagsForAWS(group, p, true)

	group.AddTo(cmd)

	return cmd
}

func doDeleteNodeGroup(p *api.ProviderConfig, cfg *api.ClusterConfig, ng *api.NodeGroup, nameArg string, cmd *cobra.Command) error {
	ngFilter := cmdutils.NewNodeGroupFilter()

	if err := cmdutils.NewDeleteNodeGroupLoader(p, cfg, ng, clusterConfigFile, nameArg, cmd, ngFilter, includeNodeGroups, excludeNodeGroups).Load(); err != nil {
		return err
	}

	ctl := eks.New(p, cfg)

	if err := ctl.CheckAuth(); err != nil {
		return err
	}

	if err := ctl.GetCredentials(cfg); err != nil {
		return errors.Wrapf(err, "getting credentials for cluster %q", cfg.Metadata.Name)
	}

	clientSet, err := ctl.NewStdClientSet(cfg)
	if err != nil {
		return err
	}

	stackManager := ctl.NewStackManager(cfg)

	if err := collectMissingNodeGroups(ngFilter, stackManager, &cfg.NodeGroups); err != nil {
		return err
	}

	ngSubset, _ := ngFilter.MatchAll(cfg.NodeGroups)
	ngCount := ngSubset.Len()

	ngFilter.LogInfo(cfg.NodeGroups)

	if updateAuthConfigMap {
		cmdutils.LogIntendedAction(dryRun, "delete %d nodegroups from auth ConfigMap in cluster %q", ngCount, cfg.Metadata.Name)
		err := ngFilter.ForEach(cfg.NodeGroups, func(_ int, ng *api.NodeGroup) error {
			if dryRun {
				return nil
			}
			if ng.IAM == nil || ng.IAM.InstanceRoleARN == "" {
				if err := ctl.GetNodeGroupIAM(cfg, ng); err != nil {
					return errors.Wrapf(err, "getting instance role ARN for node group %q", ng.Name)
				}
			}
			if err := authconfigmap.RemoveNodeGroup(clientSet, ng); err != nil {
				return err
			}
			return nil
		})
		if err != nil {
			return err
		}
	}

	if deleteNodeGroupDrain {
		cmdutils.LogIntendedAction(dryRun, "drain %d nodegroups in cluster %q", ngCount, cfg.Metadata.Name)
		err := ngFilter.ForEach(cfg.NodeGroups, func(_ int, ng *api.NodeGroup) error {
			if dryRun {
				return nil
			}
			if err := drain.NodeGroup(clientSet, ng, ctl.Provider.WaitTimeout(), false); err != nil {
				return err
			}
			return nil
		})
		if err != nil {
			return err
		}
	}

	cmdutils.LogIntendedAction(dryRun, "delete %d nodegroups from cluster %q", ngCount, cfg.Metadata.Name)

	{
		tasks, err := stackManager.NewTasksToDeleteNodeGroups(ngSubset, wait, nil)
		if err != nil {
			return err
		}
		tasks.DryRun = dryRun
		logger.Info(tasks.Describe())
		if errs := tasks.DoAllSync(); len(errs) > 0 {
			return handleErrors(errs, "nodegroup(s)")
		}
		cmdutils.LogCompletedAction(dryRun, "deleted %d nodegroups from cluster %q", ngCount, cfg.Metadata.Name)
	}

	return nil
}

func collectMissingNodeGroups(ngFilter *cmdutils.NodeGroupFilter, stackManager *manager.StackCollection, nodeGroups *[]*api.NodeGroup) error {
	if clusterConfigFile != "" {
		remoteNodeGroupNames, err := stackManager.ListNodeGroupStacks()
		if err != nil {
			return err
		}
		logger.Debug("remoteNodeGroupNames = %v", remoteNodeGroupNames)
		for _, remoteNodeGroupName := range remoteNodeGroupNames {
			missing := true
			for _, localNodeGroup := range *nodeGroups {
				if localNodeGroup.Name == remoteNodeGroupName {
					missing = false
				}
			}
			if missing {
				logger.Info("nodegroup %q present in the cluster, but missing from config file %q", remoteNodeGroupName, clusterConfigFile)
				*nodeGroups = append(*nodeGroups, &api.NodeGroup{Name: remoteNodeGroupName})
				if deleteOnlyMissingNodeGroups {
					ngFilter.IncludeNames.Insert(remoteNodeGroupName)
				}
			}
		}
	}
	logger.Debug("ngFilter.IncludeNames = %v", ngFilter.IncludeNames.List())
	return nil
}
