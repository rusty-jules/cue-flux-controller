package controllers

import (
	"context"
	"testing"
	"time"

	"github.com/fluxcd/pkg/apis/meta"
	sourcev1 "github.com/fluxcd/source-controller/api/v1beta1"
	. "github.com/onsi/gomega"
	cuev1alpha1 "github.com/phoban01/cue-flux-controller/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func TestCueInstanceReconciler_BuildInstancePolicy(t *testing.T) {
	g := NewWithT(t)
	id := "builder-" + randStringRunes(5)

	err := createNamespace(id)
	g.Expect(err).NotTo(HaveOccurred(), "failed to create test namespace")

	err = createKubeConfigSecret(id)
	g.Expect(err).NotTo(HaveOccurred(), "failed to create kubeconfig secret")

	deployNamespace := "cue-build-" + randStringRunes(4)
	err = createNamespace(deployNamespace)
	g.Expect(err).NotTo(HaveOccurred())

	artifactFile := "instance-" + randStringRunes(5)
	artifactChecksum, err := createArtifact(testServer, "testdata/policy", artifactFile)
	g.Expect(err).ToNot(HaveOccurred())

	repositoryName := types.NamespacedName{
		Name:      randStringRunes(5),
		Namespace: id,
	}

	err = applyGitRepository(repositoryName, artifactFile, "main/"+artifactChecksum)
	g.Expect(err).NotTo(HaveOccurred())

	cueInstanceKey := types.NamespacedName{
		Name:      "inst-" + randStringRunes(5),
		Namespace: id,
	}

	tagName := "podinfo" + randStringRunes(5)

	cueInstance := &cuev1alpha1.CueInstance{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cueInstanceKey.Name,
			Namespace: cueInstanceKey.Namespace,
		},
		Spec: cuev1alpha1.CueInstanceSpec{
			Interval:   metav1.Duration{Duration: reconciliationInterval},
			ModuleRoot: "./testdata/policy",
			Path:       ".",
			Policy:     cuev1alpha1.PolicyRuleDeny,
			Tags: []cuev1alpha1.TagVar{
				{
					Name:  "name",
					Value: tagName,
				},
				{
					Name:  "namespace",
					Value: deployNamespace,
				},
				{
					Name:  "port",
					Value: "80",
				},
			},
			KubeConfig: &cuev1alpha1.KubeConfig{
				SecretRef: meta.LocalObjectReference{
					Name: "kubeconfig",
				},
			},
			SourceRef: cuev1alpha1.CrossNamespaceSourceReference{
				Name:      repositoryName.Name,
				Namespace: repositoryName.Namespace,
				Kind:      sourcev1.GitRepositoryKind,
			},
		},
	}

	g.Expect(k8sClient.Create(context.TODO(), cueInstance)).To(Succeed())

	var obj cuev1alpha1.CueInstance
	g.Eventually(func() bool {
		_ = k8sClient.Get(context.Background(), client.ObjectKeyFromObject(cueInstance), &obj)
		return obj.Status.LastAppliedRevision == "main/"+artifactChecksum
	}, timeout, time.Second).Should(BeFalse())

	obj.Spec.Policy = cuev1alpha1.PolicyRuleIgnore
	g.Expect(k8sClient.Update(context.TODO(), &obj)).To(Succeed())

	g.Eventually(func() bool {
		var obj cuev1alpha1.CueInstance
		_ = k8sClient.Get(context.Background(), client.ObjectKeyFromObject(cueInstance), &obj)
		return obj.Status.LastAppliedRevision == "main/"+artifactChecksum
	}, timeout, time.Second).Should(BeTrue())
}
