package cmdutils

import (
	"fmt"
	"strings"

	"github.com/gobwas/glob"
	"github.com/kris-nova/logger"
	"github.com/pkg/errors"

	"k8s.io/apimachinery/pkg/util/sets"

	api "github.com/weaveworks/eksctl/pkg/apis/eksctl.io/v1alpha4"
	"github.com/weaveworks/eksctl/pkg/cfn/manager"
)

// NodeGroupFilter holds filter configuration
type NodeGroupFilter struct {
	ExcludeAll bool // highest priority

	// include filters take presidence
	IncludeNames     sets.String
	includeGlobs     []glob.Glob
	includeGlobsSpec string

	ExcludeNames     sets.String
	excludeGlobs     []glob.Glob
	excludeGlobsSpec string
}

// NewNodeGroupFilter create new NodeGroupFilter instance
func NewNodeGroupFilter() *NodeGroupFilter {
	return &NodeGroupFilter{
		ExcludeAll:   false,
		IncludeNames: sets.NewString(),
		ExcludeNames: sets.NewString(),
	}
}

// SetFilterGlobs sets globs for inclusion and exclusion rules
func (f *NodeGroupFilter) SetFilterGlobs(includeGlobExprs, excludeGlobExprs []string, nodeGroups []*api.NodeGroup) error {
	if err := f.SetIncludeFilterGlobs(includeGlobExprs, nodeGroups); err != nil {
		return err
	}
	return f.SetExcludeFilterGlobs(excludeGlobExprs)
}

// SetIncludeFilterGlobs sets globs for inclusion rules
func (f *NodeGroupFilter) SetIncludeFilterGlobs(globExprs []string, nodeGroups []*api.NodeGroup) error {
	for _, expr := range globExprs {
		compiledExpr, err := glob.Compile(expr)
		if err != nil {
			return errors.Wrapf(err, "parsing glob filter %q", expr)
		}
		f.includeGlobs = append(f.includeGlobs, compiledExpr)
	}
	f.includeGlobsSpec = strings.Join(globExprs, ",")
	return f.includeGlobsMatchAnything(nodeGroups)
}

func (f *NodeGroupFilter) includeGlobsMatchAnything(nodeGroups []*api.NodeGroup) error {
	if len(f.includeGlobs) == 0 {
		return nil
	}
	for _, ng := range nodeGroups {
		ok := false
		f.matchGlobs(ng.Name, f.includeGlobs, &ok)
		if ok {
			return nil
		}
	}
	return fmt.Errorf("no nodegroups match include filter specification: %q", f.includeGlobsSpec)
}

// SetExcludeFilterGlobs sets globs for exclusion rules
func (f *NodeGroupFilter) SetExcludeFilterGlobs(globExprs []string) error {
	for _, expr := range globExprs {
		compiledExpr, err := glob.Compile(expr)
		if err != nil {
			return errors.Wrapf(err, "parsing glob filter %q", expr)
		}
		f.excludeGlobs = append(f.excludeGlobs, compiledExpr)
	}
	f.excludeGlobsSpec = strings.Join(globExprs, ",")
	return nil // exclude filter doesn't have to match anything, so we don't validate it
}

// SetExcludeExistingFilter uses stackManager to list existing nodegroup stacks and configures
// the filter accordingly
func (f *NodeGroupFilter) SetExcludeExistingFilter(stackManager *manager.StackCollection) error {
	if f.ExcludeAll {
		return nil
	}

	existing, err := stackManager.ListNodeGroupStacks()
	if err != nil {
		return err
	}

	f.ExcludeNames.Insert(existing...)

	for _, name := range existing {
		isAlsoIncluded := false
		f.matchGlobs(name, f.includeGlobs, &isAlsoIncluded)
		if isAlsoIncluded {
			return fmt.Errorf("existing nodegroup %q should be excluded, but matches include fileter: %q", name, f.includeGlobsSpec)
		}
	}
	return nil
}

func (*NodeGroupFilter) matchGlobs(name string, exprs []glob.Glob, result *bool) {
	for _, compiledExpr := range exprs {
		if compiledExpr.Match(name) {
			*result = true
			return
		}
	}
}

// Match given nodegroup against the filter and returns
// true or false if it has to be included or excluded
func (f *NodeGroupFilter) Match(name string) bool {
	if f.ExcludeAll {
		return false // force exclude
	}

	hasIncludeRules := f.IncludeNames.Len()+len(f.includeGlobs) != 0
	hasExcludeRules := f.ExcludeNames.Len()+len(f.excludeGlobs) != 0

	if !hasIncludeRules && !hasExcludeRules {
		return true // empty rules - include
	}

	mustInclude := false // override when rules overlap

	if hasIncludeRules {
		mustInclude = f.IncludeNames.Has(name)
		f.matchGlobs(name, f.includeGlobs, &mustInclude)
		if !hasExcludeRules {
			// empty exclusion rules - strict inclusion mode
			return mustInclude
		}
	}

	if hasExcludeRules {
		exclude := f.ExcludeNames.Has(name)
		f.matchGlobs(name, f.excludeGlobs, &exclude)
		if exclude && !mustInclude {
			// no inclusion rules to override
			return false
		}
	}

	return true // biased to include
}

// MatchAll nodegroups against the filter and return two sets of names
func (f *NodeGroupFilter) MatchAll(nodeGroups []*api.NodeGroup) (sets.String, sets.String) {
	included, excluded := sets.NewString(), sets.NewString()
	if f.ExcludeAll {
		for _, ng := range nodeGroups {
			excluded.Insert(ng.Name)
		}
		return included, excluded
	}
	for _, ng := range nodeGroups {
		if f.Match(ng.Name) {
			included.Insert(ng.Name)
		} else {
			excluded.Insert(ng.Name)
		}
	}
	return included, excluded
}

// LogInfo prints out a user-friendly message about how filter was applied
func (f *NodeGroupFilter) LogInfo(nodeGroups []*api.NodeGroup) {
	logMsg := func(ngSubset sets.String, status string) {
		count := ngSubset.Len()
		list := strings.Join(ngSubset.List(), ", ")
		subject := "nodegroups (%s) were"
		if count == 1 {
			subject = "nodegroup (%s) was"
		}
		logger.Info("%d "+subject+" %s", count, list, status)
	}

	included, excluded := f.MatchAll(nodeGroups)
	if excluded.Len() > 0 {
		logMsg(excluded, "excluded")
	}
	if included.Len() > 0 {
		logMsg(included, "included")
	}
}

// ForEach iterates over each nodegroup that is included by the filter and calls iterFn
func (f *NodeGroupFilter) ForEach(nodeGroups []*api.NodeGroup, iterFn func(i int, ng *api.NodeGroup) error) error {
	for i, ng := range nodeGroups {
		if f.Match(ng.Name) {
			if err := iterFn(i, ng); err != nil {
				return err
			}
		}
	}
	return nil
}

// ValidateNodeGroupsAndSetDefaults is calls api.ValidateNodeGroup & api.SetNodeGroupDefaults for
// all nodegroups that match the filter
func (f *NodeGroupFilter) ValidateNodeGroupsAndSetDefaults(nodeGroups []*api.NodeGroup) error {
	return f.ForEach(nodeGroups, func(i int, ng *api.NodeGroup) error {
		if err := api.ValidateNodeGroup(i, ng); err != nil {
			return err
		}
		if err := api.SetNodeGroupDefaults(i, ng); err != nil {
			return err
		}
		return nil
	})
}
