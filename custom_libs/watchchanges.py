import logging, threading, time
from kubernetes import watch, client
from custom_libs import annotations

use_threads = True

class threadWatchChanges (threading.Thread):
    def __init__(self, name, queue, obj):
        """
        On every change of cm/secret this class retrieves all resources using the respective cm/secret
        It will populate a queue with the necessary information

        Receives a queue and a function that lists secrets or configmaps objects
        """

        threading.Thread.__init__(self)
        self.log = logging.getLogger(__name__ + " " + name)
        self.log.info("Init threadWatchChanges")
        self.obj = obj
        self.v1 = client.CoreV1Api()
        self.v1b1 = client.AppsV1beta1Api()
        self.v1b1e = client.ExtensionsV1beta1Api()
        self.ann = annotations.Annotations()
        # statefulsets 1.7
        # v1b2 = client.AppsV1beta2Api()
        self.q = queue

    def run(self):
        """
        Spawns a new thread of "get_resources_using_obj" function on every change

        Since the watch will eventually timeout, we use an while loop to restart watchers
        This means that every hour we will iterate over all cm and secrets again
        """
        self.log.info("Starting thread")

        w = watch.Watch()
        while True:
            try:
                for event in w.stream(self.obj, _request_timeout=0):
                    self.log.info("Event: %s %s %s %s %s" % (event['type'], \
                                                             event['object'].kind, \
                                                             event['object'].metadata.name, \
                                                             event['object'].metadata.namespace, \
                                                             event['object'].metadata.resource_version))
                    if use_threads:
                        # we are I/O bound on the network. so threading is helping a little
                        # (4s vs 8s on my cluster)
                        t = threading.Thread(target=self.get_resources_using_obj, args = [event['object']])
                        t.daemon = True
                        t.start()
                    else:
                        self.get_resources_using_obj(event['object'])
            except BaseException as e:
                self.log.exception('{!r}. Restarting loop.'.format(e))
                time.sleep(1)
        self.log.info("Thread has been stopped")

    def get_resources_using_obj(self, obj):
        """
        We receive an object that has been modified
        For each controller type, get all volumes/env values andcheck if it's the received object
        StatefulSets can't be patched, so we ignore them

        Assumption:
         - configmaps are only used as volumes
         - secrets are used as volumes or ENV values

        We need to send "kind" for each request because of https://github.com/kubernetes-client/python/issues/429
        (we don't know what kind of resource we have on return)
        """
        self.log.debug("get_resources_using_obj")

        namespace = obj.metadata.namespace
        # here threading each request is taking longer then the request 
        ds = self.v1b1e.list_namespaced_daemon_set(namespace)
        dp = self.v1b1e.list_namespaced_deployment(namespace)
        rc = self.v1.list_namespaced_replication_controller(namespace)
#         ss = self.v1b1.list_namespaced_stateful_set(namespace)

        self.find_resources_using_volume(obj, ds, 'DaemonSet')
        self.find_resources_using_volume(obj, dp, 'Deployment')
        self.find_resources_using_volume(obj, rc, 'ReplicationController')
#         self.find_resources_using_volume(obj, ss, 'StatefulSet')
        if 'ConfigMap' == obj.kind:
            return
        elif 'Secret' == obj.kind:
            self.find_resources_using_env(obj, ds, 'DaemonSet')
            self.find_resources_using_env(obj, dp, 'Deployment')
            self.find_resources_using_env(obj, rc, 'ReplicationController')
#             self.find_resources_using_env(obj, ss, 'StatefulSet')
        else:
            self.log.critical("Unknown object type: %s", obj.kind)
            return

    def find_resources_using_volume(self, obj, resources, kind):
        """
        When we find a volume on the respective resources that it's the same as the desired one,
        send it for update (put resource+environment in a queue)
        
        We don't check that the volume is actually mounted inside any container
        """
        self.log.debug("find_resources_using_volume")

        vol_name = obj.metadata.name
        for res in resources.items:
            if res.spec.template.spec.volumes:
                for volume in res.spec.template.spec.volumes:
                    if obj.kind == 'ConfigMap' and volume.config_map and volume.config_map.name and volume.config_map.name == vol_name:
                        self.log.debug("****** Vol %s is used by %s (%s)" % (vol_name, res.metadata.name, obj.kind))
                        self.add_resource_for_update(obj, res, kind)
                        break
                    if obj.kind == 'Secret' and volume.secret and volume.secret.secret_name and volume.secret.secret_name == vol_name:
                        self.log.debug("****** Vol secret %s is used by %s (%s)" % (vol_name, res.metadata.name, obj.kind))
                        self.add_resource_for_update(obj, res, kind)
                        break

    def find_resources_using_env(self, obj, resources, kind):
        """
        When we find an environment value on the respective resources that it's the same as the desired one,
        send it for update (put resource+environment in a queue)
        """
        self.log.debug("find_resources_using_env")

        env_name = obj.metadata.name
        for res in resources.items:
            for container in res.spec.template.spec.containers:
                if container.env:
                    for env in container.env:
                        if env.value_from and env.value_from.secret_key_ref and env.value_from.secret_key_ref.name == env_name:
                            self.log.debug("****** ENV %s is used by %s (%s)" % (env_name, res.metadata.name, obj.kind))
                            self.add_resource_for_update(obj, res, kind)

    def add_resource_for_update(self, obj, res, kind):
        """
        Check if the controller has our annotation (signature).
        Only those elements are managed by us
        Check if we have the corresponding annotation at the correct version

        Put in the queue the necessary info for updating the controllers
        """
        self.log.debug("Check if resource is updatable: %s %s" %(kind, res.metadata.name))

        if not self.ann.has_signature(res):
            return
        annotation_ver = self.ann.get_version(res, obj.kind, obj.metadata.name)
        if str(annotation_ver) == str(obj.metadata.resource_version):
            return

        self.log.info("Version differ %s:%s" % (annotation_ver, obj.metadata.resource_version))
        self.log.info("Send for update %s %s/%s by %s/%s" % (kind, res.metadata.namespace, res.metadata.name, obj.kind, obj.metadata.name))
        item = {
                'res_name': res.metadata.name,
                'res_namespace': res.metadata.namespace,
                'res_kind': kind,
                'cfg_kind': obj.kind,
                'cfg_name': obj.metadata.name,
                'cfg_version': obj.metadata.resource_version,
                }
        self.q.put(item)
