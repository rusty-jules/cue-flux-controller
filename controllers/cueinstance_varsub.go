/*
Copyright 2022 The Flux authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package controllers

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	"cuelang.org/go/cue"
	"github.com/drone/envsubst"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	cuev1alpha1 "github.com/phoban01/cue-flux-controller/api/v1alpha1"
)

const (
	// varsubRegex is the regular expression used to validate
	// the var names before substitution
	varsubRegex   = "^[_[:alpha:]][_[:alpha:][:digit:]]*$"
	DisabledValue = "disabled"
)

const (
    specField               = "spec"
	postBuildField          = "postBuild"
	substituteFromField     = "substituteFrom"
	substituteAnnotationKey = "cue.contrib.flux.io/substitute"
)

// SubstituteVariables replaces the vars with their values in the specified resource.
// If a resource is labeled or annotated with
// 'kustomize.toolkit.fluxcd.io/substitute: disabled' the substitution is skipped.
// if dryRun is true, this means we should not attempt to talk to the cluster.
func SubstituteVariables(
	ctx context.Context,
	kubeClient client.Client,
	cueinstance *cuev1alpha1.CueInstance,
	expr *cue.Value,
    resData []byte,
	dryRun bool) ([]byte, error) {

    //for _, annotation := range []string{"metadata.labels", "metadata.annotations"} {
        //meta, err := expr.LookupPath(cue.ParsePath(annotation)).Struct()
        //if err == nil {
            //subAnnotation, err := meta.FieldByName(substituteAnnotationKey, false)
            //if err == nil {
                //subAnnotationValue, err := subAnnotation.Value.String()
                //if err == nil {
                    //if subAnnotationValue == DisabledValue {
                        //return nil, nil
                    //}
                //}
            //}
        //}
    //}

	// load vars from ConfigMaps and Secrets data keys
	// In dryRun mode this step is skipped. This might in different kind of errors.
	// But if the user is using dryRun, he/she should know what he/she is doing, and we should comply.
	var vars map[string]string
	if !dryRun {
        v, err := loadVars(ctx, kubeClient, cueinstance)
		if err != nil {
			return nil, err
		}
        vars = v
	}

	// run bash variable substitutions
	if len(vars) > 0 {
        d, err := cueEncodeYAML(*expr)
		res, err := varSubstitution(d, vars)
		//res, err := varSubstitution(resData, vars)
		if err != nil {
			return nil, fmt.Errorf("YAMLToJSON: %w", err)
		}
        return res, nil
	}

	return resData, nil
}

func loadVars(ctx context.Context, kubeClient client.Client, cueinstance *cuev1alpha1.CueInstance) (map[string]string, error) {
	vars := make(map[string]string)
	substituteFrom := cueinstance.Spec.PostBuild.SubstituteFrom

	for _, reference := range substituteFrom {
		namespacedName := types.NamespacedName{Namespace: cueinstance.GetNamespace(), Name: reference.Name}
		switch reference.Kind {
		case "ConfigMap":
			resource := &corev1.ConfigMap{}
			if err := kubeClient.Get(ctx, namespacedName, resource); err != nil {
				if reference.Optional && apierrors.IsNotFound(err) {
					continue
				}
				return nil, fmt.Errorf("substitute from 'ConfigMap/%s' error: %w", reference.Name, err)
			}
			for k, v := range resource.Data {
				vars[k] = strings.ReplaceAll(v, "\n", "")
			}
		case "Secret":
			resource := &corev1.Secret{}
			if err := kubeClient.Get(ctx, namespacedName, resource); err != nil {
				if reference.Optional && apierrors.IsNotFound(err) {
					continue
				}
				return nil, fmt.Errorf("substitute from 'Secret/%s' error: %w", reference.Name, err)
			}
			for k, v := range resource.Data {
				vars[k] = strings.ReplaceAll(string(v), "\n", "")
			}
		}
	}

	return vars, nil
}

func varSubstitution(data []byte, vars map[string]string) ([]byte, error) {
	r, _ := regexp.Compile(varsubRegex)
	for v := range vars {
		if !r.MatchString(v) {
			return nil, fmt.Errorf("'%s' var name is invalid, must match '%s'", v, varsubRegex)
		}
	}

	output, err := envsubst.Eval(string(data), func(s string) string {
		return vars[s]
	})
	if err != nil {
		return nil, fmt.Errorf("variable substitution failed: %w", err)
	}

	return []byte(output), nil
}
