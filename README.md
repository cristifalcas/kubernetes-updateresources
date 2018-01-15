# kubernetes-updateresources

It has minimal testing

It was tested only with kube 1.6

## How it works

We start 2 watchers for configmaps and secrets
When something is received by the watcher, we retrieve all resources that use
that configmap or secret

If the resource has an annotation named opsguru.signature/should_update with value True,
it is send to be updated

Updating is done based on other annotations:
* for each configmap/secret for the respective resource, we add an annotation with the coresponding version
from the cm/secret
* if that version is different, we force update the resource

Updating ReplicationControllers or StatefulSets is not implemented in kubernets for this version.

Solutions:
* delete _ALL_ pods from that resource
* or try to implement something like kubernetes does: delete a pod, wait to be up again, go to next pod

TBD:
* Annotations should be aplpied only for stable resources: all desired replicas == current replicas
