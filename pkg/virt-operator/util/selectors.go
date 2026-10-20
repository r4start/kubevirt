package util

import (
	"errors"
	"fmt"
	"maps"
	"slices"

	corev1 "k8s.io/api/core/v1"
)

func AndNodeSelectorTerms(firstList, secondList []corev1.NodeSelectorTerm) []corev1.NodeSelectorTerm {
	out := make([]corev1.NodeSelectorTerm, 0, len(firstList)*len(secondList))
	for _, first := range firstList {
		for _, second := range secondList {
			combined := corev1.NodeSelectorTerm{
				MatchExpressions: make([]corev1.NodeSelectorRequirement, 0, len(first.MatchExpressions)+len(second.MatchExpressions)),
				MatchFields:      make([]corev1.NodeSelectorRequirement, 0, len(first.MatchFields)+len(second.MatchFields)),
			}
			combined.MatchExpressions = append(combined.MatchExpressions, first.MatchExpressions...)
			combined.MatchExpressions = append(combined.MatchExpressions, second.MatchExpressions...)
			combined.MatchFields = append(combined.MatchFields, first.MatchFields...)
			combined.MatchFields = append(combined.MatchFields, second.MatchFields...)
			out = append(out, combined)
		}
	}
	return out
}

type treeNode struct {
	children map[string]*treeNode
}

func newTreeNode() *treeNode {
	return &treeNode{children: make(map[string]*treeNode)}
}

type NodeSelectorTerms struct {
	orderedKeys []string
	root        *treeNode
}

func NewNodeSelectorTerms(partitionKeys []string) (*NodeSelectorTerms, error) {
	if len(partitionKeys) == 0 {
		return nil, errors.New("partition keys shouldn't be empty")
	}

	orderedKeys := slices.Clone(partitionKeys)
	slices.Sort(orderedKeys)
	seen := make(map[string]struct{}, len(orderedKeys))
	for _, key := range orderedKeys {
		if _, exists := seen[key]; exists {
			return nil, fmt.Errorf("duplicate partition key %s", key)
		}
		seen[key] = struct{}{}
	}

	return &NodeSelectorTerms{orderedKeys: orderedKeys, root: newTreeNode()}, nil
}

func (tree *NodeSelectorTerms) Insert(terms map[string]string) error {
	if len(terms) != len(tree.orderedKeys) {
		return errors.New("node selector terms don't conform with partition keys")
	}

	current := tree.root
	for _, key := range tree.orderedKeys {
		value, exists := terms[key]
		if !exists {
			return fmt.Errorf("terms doesn't contain %s key", key)
		}
		node, exists := current.children[value]
		if !exists {
			node = newTreeNode()
			current.children[value] = node
		}
		current = node
	}
	return nil
}

func (tree *NodeSelectorTerms) Terms() ([]corev1.NodeSelectorTerm, error) {
	if len(tree.root.children) == 0 {
		return nil, nil
	}

	type termData struct {
		node   *treeNode
		prefix []string
		level  int
	}
	toProcess := []termData{{node: tree.root}}
	var terms []corev1.NodeSelectorTerm

	for len(toProcess) > 0 {
		last := len(toProcess) - 1
		current := toProcess[last]
		toProcess = toProcess[:last]

		values := slices.Collect(maps.Keys(current.node.children))
		if len(values) == 0 {
			continue
		}
		slices.Sort(values)

		for _, value := range values {
			prefix := append(slices.Clone(current.prefix), value)
			toProcess = append(toProcess, termData{
				node: current.node.children[value], prefix: prefix, level: current.level + 1,
			})
		}

		selector := corev1.NodeSelectorTerm{MatchExpressions: []corev1.NodeSelectorRequirement{{
			Key: tree.orderedKeys[current.level], Operator: corev1.NodeSelectorOpNotIn, Values: values,
		}}}
		for i := 0; i < current.level; i++ {
			selector.MatchExpressions = append(selector.MatchExpressions, corev1.NodeSelectorRequirement{
				Key: tree.orderedKeys[i], Operator: corev1.NodeSelectorOpIn, Values: []string{current.prefix[i]},
			})
		}
		terms = append(terms, selector)
	}

	return terms, nil
}
