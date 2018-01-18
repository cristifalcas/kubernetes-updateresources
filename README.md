# kubernetes-updateresources

It was tested only with kube 1.6

## How it works

We start 2 watchers for configmaps and secrets.

When something is received by the watcher, we retrieve all resources that use
that configmap or secret

If the resource has an annotation named opsguru.signature/should_update with value True,
it is send to be updated

Updating is done based on other annotations:
* for each configmap/secret for the respective resource, we add an annotation with the corresponding version
from the cm/secret
* if that version is different, we force update the resource

## Issues

DaemonSets needs to have spec.updateStrategy.type=RollingUpdate. We are not managing this.

StatefulSets can't be patched. We ignore them.

Updating rollingupdates for ReplicationControllers and StatefulSets is not implemented in kubernets for this version.
We don't manage them yet.

If controllers are edited with our signature after we start and they use the existing cm/secrets, we don't notice.

On the very first run, all pods are restarted because we need to insert our annotations
