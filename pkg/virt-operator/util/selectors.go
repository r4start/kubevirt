package util

import (
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
