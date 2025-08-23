package meta

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func NewObjectMeta(resourceName, namespace, runSuffix string) metav1.ObjectMeta {
	obj := metav1.ObjectMeta{
		Name: resourceName,
		Labels: map[string]string{
			"app.kubernetes.io/name":       "rafdir",
			"app.kubernetes.io/instance":   resourceName,
			"app.kubernetes.io/component":  "resticprofile",
			"app.kubernetes.io/part-of":    "rafdir",
			"app.kubernetes.io/managed-by": "rafdir",
			"rafdir/runSuffix":             runSuffix,
		},
	}
	if namespace != "" {
		obj.Namespace = namespace
	}
	return obj
}
