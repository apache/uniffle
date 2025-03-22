/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package shuffleserver

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/client-go/kubernetes"
	"k8s.io/utils/pointer"

	unifflev1alpha1 "github.com/apache/incubator-uniffle/deploy/kubernetes/operator/api/uniffle/v1alpha1"
	"github.com/apache/incubator-uniffle/deploy/kubernetes/operator/pkg/constants"
	controllerconstants "github.com/apache/incubator-uniffle/deploy/kubernetes/operator/pkg/controller/constants"
	"github.com/apache/incubator-uniffle/deploy/kubernetes/operator/pkg/controller/sync/coordinator"
	"github.com/apache/incubator-uniffle/deploy/kubernetes/operator/pkg/controller/util"
	"github.com/apache/incubator-uniffle/deploy/kubernetes/operator/pkg/utils"
)

var defaultENVs sets.String

func init() {
	defaultENVs = sets.NewString()
	defaultENVs.Insert(controllerconstants.ShuffleServerRPCPortEnv,
		controllerconstants.ShuffleServerHTTPPortEnv,
		controllerconstants.RSSCoordinatorQuorumEnv,
		controllerconstants.XmxSizeEnv,
		controllerconstants.ServiceNameEnv,
		controllerconstants.NodeNameEnv,
		controllerconstants.RssIPEnv)
}

// GenerateShuffleServers generates objects related to shuffle servers.
func GenerateShuffleServers(kubeClient kubernetes.Interface, rss *unifflev1alpha1.RemoteShuffleService) (
	*corev1.ServiceAccount, *appsv1.StatefulSet) {
	sa := GenerateSA(rss)
	sts := GenerateSts(kubeClient, rss)
	return sa, sts
}

// GenerateSA generates service account of shuffle servers.
func GenerateSA(rss *unifflev1alpha1.RemoteShuffleService) *corev1.ServiceAccount {
	sa := &corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{
			Name:      GenerateName(rss),
			Namespace: rss.Namespace,
		},
	}
	util.AddOwnerReference(&sa.ObjectMeta, rss)
	return sa
}

// GenerateHPA generates hpa object of shuffle servers and return when it is enabled.
func GenerateHPA(rss *unifflev1alpha1.RemoteShuffleService) (*autoscalingv2.HorizontalPodAutoscaler, bool) {
	rssHPASpec := rss.Spec.ShuffleServer.Autoscaler.HPASpec
	name := GenerateName(rss)
	hpa := &autoscalingv2.HorizontalPodAutoscaler{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: rss.Namespace,
			Name:      name,
			Labels: map[string]string{
				constants.LabelShuffleServer: "true",
			},
		},
		Spec: autoscalingv2.HorizontalPodAutoscalerSpec{
			MinReplicas: rssHPASpec.MinReplicas,
			MaxReplicas: rssHPASpec.MaxReplicas,
			Metrics:     rssHPASpec.Metrics,
			Behavior:    rssHPASpec.Behavior,
		},
	}
	hpa.Spec.ScaleTargetRef = autoscalingv2.CrossVersionObjectReference{
		Kind:       "StatefulSet",
		Name:       name,
		APIVersion: appsv1.SchemeGroupVersion.String(),
	}
	util.AddOwnerReference(&hpa.ObjectMeta, rss)
	return hpa, rss.Spec.ShuffleServer.Autoscaler.Enable
}

// getReplicas returns replicas of shuffle servers.
func getReplicas(kubeClient kubernetes.Interface, rss *unifflev1alpha1.RemoteShuffleService) *int32 {
	// it is only during testing that kubeClient is empty.
	if kubeClient == nil {
		return rss.Spec.ShuffleServer.Replicas
	}
	if !rss.Spec.ShuffleServer.Autoscaler.Enable {
		return rss.Spec.ShuffleServer.Replicas
	}
	stsName := GenerateName(rss)
	existSts, err := kubeClient.AppsV1().StatefulSets(rss.Namespace).
		Get(context.Background(), stsName, metav1.GetOptions{})
	if err != nil {
		return rss.Spec.ShuffleServer.Autoscaler.HPASpec.MinReplicas
	}
	return existSts.Spec.Replicas
}

// GenerateSts generates statefulSet of shuffle servers.
func GenerateSts(kubeClient kubernetes.Interface, rss *unifflev1alpha1.RemoteShuffleService) *appsv1.StatefulSet {
	name := GenerateName(rss)
	replicas := getReplicas(kubeClient, rss)

	podSpec := corev1.PodSpec{
		SecurityContext:    rss.Spec.ShuffleServer.SecurityContext,
		HostNetwork:        *rss.Spec.ShuffleServer.HostNetwork,
		ServiceAccountName: GenerateName(rss),
		Tolerations:        rss.Spec.ShuffleServer.Tolerations,
		Volumes:            rss.Spec.ShuffleServer.Volumes,
		NodeSelector:       rss.Spec.ShuffleServer.NodeSelector,
		Affinity:           rss.Spec.ShuffleServer.Affinity,
		ImagePullSecrets:   rss.Spec.ImagePullSecrets,
	}

	configurationVolume := corev1.Volume{
		Name: controllerconstants.ConfigurationVolumeName,
		VolumeSource: corev1.VolumeSource{
			ConfigMap: &corev1.ConfigMapVolumeSource{
				LocalObjectReference: corev1.LocalObjectReference{
					Name: rss.Spec.ConfigMapName,
				},
				DefaultMode: pointer.Int32(0777),
			},
		},
	}
	podSpec.Volumes = append(podSpec.Volumes, configurationVolume)
	if podSpec.HostNetwork {
		podSpec.DNSPolicy = corev1.DNSClusterFirstWithHostNet
	}

	defaultLabels := utils.GenerateShuffleServerLabels(rss)
	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: rss.Namespace,
			Labels:    defaultLabels,
		},
		Spec: appsv1.StatefulSetSpec{
			Selector: &metav1.LabelSelector{
				MatchLabels: defaultLabels,
			},
			PodManagementPolicy: rss.Spec.ShuffleServer.PodManagementPolicy,
			UpdateStrategy: appsv1.StatefulSetUpdateStrategy{
				Type: appsv1.RollingUpdateStatefulSetStrategyType,
				RollingUpdate: &appsv1.RollingUpdateStatefulSetStrategy{
					Partition: replicas,
				},
			},
			ServiceName: generateHeadlessSVCName(rss),
			Replicas:    replicas,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: make(map[string]string),
				},
				Spec: podSpec,
			},
		},
	}
	for k, v := range rss.Spec.ShuffleServer.Labels {
		sts.Spec.Template.Labels[k] = v
	}
	for k, v := range defaultLabels {
		sts.Spec.Template.Labels[k] = v
	}

	// set runtimeClassName
	if rss.Spec.ShuffleServer.RuntimeClassName != nil {
		sts.Spec.Template.Spec.RuntimeClassName = rss.Spec.ShuffleServer.RuntimeClassName
	}

	// add VolumeClaimTemplates, support cloud storage
	sts.Spec.VolumeClaimTemplates = make([]corev1.PersistentVolumeClaim, 0, len(rss.Spec.ShuffleServer.VolumeClaimTemplates))
	for _, pvcTemplate := range rss.Spec.ShuffleServer.VolumeClaimTemplates {
		sts.Spec.VolumeClaimTemplates = append(sts.Spec.VolumeClaimTemplates, corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name: *pvcTemplate.VolumeNameTemplate,
			},
			Spec: pvcTemplate.Spec,
		})
	}

	// add custom annotation, and default annotation used by rss
	reservedAnnotations := map[string]string{
		constants.AnnotationRssName: rss.Name,
		constants.AnnotationRssUID:  string(rss.UID),
		constants.AnnotationMetricsServerPort: fmt.Sprintf("%v",
			*rss.Spec.ShuffleServer.HTTPPort),
		constants.AnnotationShuffleServerPort: fmt.Sprintf("%v",
			*rss.Spec.ShuffleServer.RPCPort),
	}

	annotations := map[string]string{}
	for key, value := range rss.Spec.ShuffleServer.Annotations {
		annotations[key] = value
	}
	// reserved annotations have higher preference.
	for key, value := range reservedAnnotations {
		annotations[key] = value
	}

	sts.Spec.Template.Annotations = annotations

	// add init containers, the main container and other containers.
	sts.Spec.Template.Spec.InitContainers = util.GenerateInitContainers(rss.Spec.ShuffleServer.RSSPodSpec)
	containers := []corev1.Container{*generateMainContainer(rss)}
	containers = append(containers, rss.Spec.ShuffleServer.SidecarContainers...)
	sts.Spec.Template.Spec.Containers = containers

	// add hostPath volumes for shuffle servers.
	hostPathMounts := rss.Spec.ShuffleServer.HostPathMounts
	logHostPath := rss.Spec.ShuffleServer.LogHostPath
	sts.Spec.Template.Spec.Volumes = append(sts.Spec.Template.Spec.Volumes,
		util.GenerateHostPathVolumes(hostPathMounts, logHostPath, name)...)

	util.AddOwnerReference(&sts.ObjectMeta, rss)
	return sts
}

// GenerateName returns workload or nodePort service name of shuffle server.
func GenerateName(rss *unifflev1alpha1.RemoteShuffleService) string {
	return utils.GenerateShuffleServerName(rss)
}

// GenerateProperties generates configuration properties of shuffle servers.
func GenerateProperties(rss *unifflev1alpha1.RemoteShuffleService) map[controllerconstants.PropertyKey]string {
	result := make(map[controllerconstants.PropertyKey]string)
	result[controllerconstants.RPCServerPort] = fmt.Sprintf("%v", *rss.Spec.ShuffleServer.RPCPort)
	result[controllerconstants.JettyHTTPPort] = fmt.Sprintf("%v", *rss.Spec.ShuffleServer.HTTPPort)
	result[controllerconstants.CoordinatorQuorum] = coordinator.GenerateAddresses(rss)
	result[controllerconstants.StorageBasePath] = generateStorageBasePath(rss)
	return result
}

// generateStorageBasePath generates storage base path in shuffle server's configuration.
func generateStorageBasePath(rss *unifflev1alpha1.RemoteShuffleService) string {
	var paths []string
	for k, v := range rss.Spec.ShuffleServer.HostPathMounts {
		if k == rss.Spec.ShuffleServer.LogHostPath {
			continue
		}
		paths = append(paths, strings.TrimSuffix(v, "/")+"/"+controllerconstants.RssDataDir)
	}

	for _, vm := range rss.Spec.ShuffleServer.VolumeMounts {
		paths = append(paths, strings.TrimSuffix(vm.MountPath, "/")+"/"+controllerconstants.RssDataDir)
	}

	sort.Strings(paths)
	return strings.Join(paths, ",")
}

// generateHeadlessSVCName returns name of shuffle servers' headless service.
func generateHeadlessSVCName(rss *unifflev1alpha1.RemoteShuffleService) string {
	return GenerateName(rss) + "-headless"
}

// generateMainContainer generates main container of shuffle servers.
func generateMainContainer(rss *unifflev1alpha1.RemoteShuffleService) *corev1.Container {
	return util.GenerateMainContainer(constants.RSSShuffleServer, rss.Spec.ShuffleServer.ConfigDir,
		rss.Spec.ShuffleServer.RSSPodSpec.DeepCopy(), generateMainContainerPorts(rss),
		generateMainContainerENV(rss), nil)
}

// generateMainContainerPorts generates ports of main container of shuffle servers.
func generateMainContainerPorts(rss *unifflev1alpha1.RemoteShuffleService) []corev1.ContainerPort {
	ports := []corev1.ContainerPort{
		{
			ContainerPort: *rss.Spec.ShuffleServer.RPCPort,
			Protocol:      corev1.ProtocolTCP,
		},
		{
			ContainerPort: *rss.Spec.ShuffleServer.HTTPPort,
			Protocol:      corev1.ProtocolTCP,
		},
	}
	ports = append(ports, rss.Spec.ShuffleServer.Ports...)
	return ports
}

// generateMainContainerENV generates environment variables of main container of shuffle servers.
func generateMainContainerENV(rss *unifflev1alpha1.RemoteShuffleService) []corev1.EnvVar {
	env := []corev1.EnvVar{
		{
			Name:  controllerconstants.ShuffleServerRPCPortEnv,
			Value: strconv.FormatInt(int64(*rss.Spec.ShuffleServer.RPCPort), 10),
		},
		{
			Name:  controllerconstants.ShuffleServerHTTPPortEnv,
			Value: strconv.FormatInt(int64(*rss.Spec.ShuffleServer.HTTPPort), 10),
		},
		{
			Name:  controllerconstants.RSSCoordinatorQuorumEnv,
			Value: coordinator.GenerateAddresses(rss),
		},
		{
			Name:  controllerconstants.XmxSizeEnv,
			Value: rss.Spec.ShuffleServer.XmxSize,
		},
		{
			Name:  controllerconstants.ServiceNameEnv,
			Value: controllerconstants.ShuffleServerServiceName,
		},
		{
			Name: controllerconstants.NodeNameEnv,
			ValueFrom: &corev1.EnvVarSource{
				FieldRef: &corev1.ObjectFieldSelector{
					APIVersion: "v1",
					FieldPath:  "spec.nodeName",
				},
			},
		},
		{
			Name: controllerconstants.RssIPEnv,
			ValueFrom: &corev1.EnvVarSource{
				FieldRef: &corev1.ObjectFieldSelector{
					APIVersion: "v1",
					FieldPath:  "status.podIP",
				},
			},
		},
	}
	for _, e := range rss.Spec.ShuffleServer.Env {
		if !defaultENVs.Has(e.Name) {
			env = append(env, e)
		}
	}
	return env
}
