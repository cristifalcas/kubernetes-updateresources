import logging, threading, time
from kubernetes import watch, client

use_threads = True
global opsguru_signature

class threadWatchChanges (threading.Thread):
    def __init__(self, name, q, obj):
        """
        On every change of cm/secret retrieves all resources using the respective cm/secret
        It will populate a queue with the necessary information

        Receives a queue and a list of secrets or configmaps objects
        """
        threading.Thread.__init__(self)
        self.log = logging.getLogger(__name__ + " " + name)
        self.log.info("Init threadWatchChanges")
        self.obj = obj
        self.v1 = client.CoreV1Api()
        self.v1b1 = client.AppsV1beta1Api()
        self.v1b1e = client.ExtensionsV1beta1Api()
        # statefulsets 1.7
        # v1b2 = client.AppsV1beta2Api()
        self.q = q

    def run(self):
        """
        Spawns a new thread for get_resources_using_obj on every change

        Since the watch will eventually timeout, we use an while loop to restart watchers
        """
        self.log.info("Starting thread")
        w = watch.Watch()
        while True:
            try:
                for event in w.stream(self.obj, _request_timeout=0):
                    kind = event['object'].kind
                    name = event['object'].metadata.name
                    namespace = event['object'].metadata.namespace
                    resource_version = event['object'].metadata.resource_version
                    self.log.info("Event: %s %s %s %s %s" % (event['type'], kind, name, namespace, resource_version))
                    if use_threads:
                        # we are I/O bound on the network. so threading is helping a little
                        # (4s vs 8s on my cluster)
                        t = threading.Thread(target=self.get_resources_using_obj, args = [event['object']])
                        t.daemon = True
                        t.start()
                    else:
                        self.get_resources_using_obj(event['object'])
            except BaseException as e:
                # watcher will timeout eventually
                # this means every hour we will iterate over all cm and secrets again
                self.log.exception('{!r}. Restarting loop.'.format(e))
                time.sleep(1)
        self.log.info("Thread has been stopped")

    def get_resources_using_obj(self, obj):
        """
        We receive an object that has been modified
        For each controller type, get all volumes/env values and 
        check if it's the received object

        Assumption:
         - configmaps are only used as volumes
         - secrets are used as volumes or ENV values

        We need to send kind for each request because of https://github.com/kubernetes-client/python/issues/429
        (we don't know what kind of resource we have on return)
        """
        self.log.debug("get_resources_using_obj")
        namespace = obj.metadata.namespace
        # here threading each request is taking longer then the request 
        ds = self.v1b1e.list_namespaced_daemon_set(namespace)
        dp = self.v1b1e.list_namespaced_deployment(namespace)
        rc = self.v1.list_namespaced_replication_controller(namespace)
        ss = self.v1b1.list_namespaced_stateful_set(namespace)

        self.find_resources_using_volume(obj, ds, 'DaemonSet')
        self.find_resources_using_volume(obj, dp, 'Deployment')
        self.find_resources_using_volume(obj, rc, 'ReplicationController')
        self.find_resources_using_volume(obj, ss, 'StatefulSet')
        if 'ConfigMap' == obj.kind:
            return
        elif 'Secret' == obj.kind:
            self.find_resources_using_env(obj, ds, 'DaemonSet')
            self.find_resources_using_env(obj, dp, 'Deployment')
            self.find_resources_using_env(obj, rc, 'ReplicationController')
            self.find_resources_using_env(obj, ss, 'StatefulSet')
        else:
            self.log.critical("Unknown object type: %s", obj.kind)
            return

    def find_resources_using_volume(self, obj, resources, kind):
        """
        When we found a volume on the respective resources that it's the same as the desired one,
        send the resource+environment to a queue
        """
        # we don't check that the volume is actually mounted inside any container
        self.log.debug("find_resources_using_volume")
        vol_name = obj.metadata.name
        for res in resources.items:
            done = 0
            if res.spec.template.spec.volumes:
                for volume in res.spec.template.spec.volumes:
                    if volume.config_map and volume.config_map.name and volume.config_map.name == vol_name:
                        self.log.info("****** Vol %s is used by %s" % (vol_name, res.metadata.name))
                        self.add_resource_for_update(obj, res, kind)
                        done = 1
                        break
                    if volume.secret and volume.secret.secret_name and volume.secret.secret_name == vol_name:
                        self.log.info("****** Vol secret %s is used by %s" % (vol_name, res.metadata.name))
                        self.add_resource_for_update(obj, res, kind)
                        done = 1
                        break
                if done:
                    break

    def find_resources_using_env(self, obj, resources, kind):
        """
        When we found an environment value on the respective resources that it's the same as the desired one,
        send the resource+environment to a queue
        """
        self.log.debug("find_resources_using_env")
        env_name = obj.metadata.name
        for res in resources.items:
            for container in res.spec.template.spec.containers:
                if container.env:
                    for env in container.env:
                        if env.value_from and env.value_from.secret_key_ref and env.value_from.secret_key_ref.name == env_name:
                            self.log.info("****** ENV %s is used by %s" % (env_name, res.metadata.name))
                            self.add_resource_for_update(obj, res, kind)
                            break

    def add_resource_for_update(self, obj, res, kind):
        """
        Check if the controller has our annotation (signature).
        Only those elements are managed by us

        Put in the queue the necessary info for updating the controllers
        """
        self.log.debug("update_resource")
        opsguru_signature = 'opsguru.signature/should_update'

        ann = res.metadata.annotations
        if ann and opsguru_signature in ann and ann[opsguru_signature] == 'True':
            self.log.info("Send for update %s from namespace %s" % (res.metadata.name, res.metadata.namespace))
            item = {
                    'res_name': res.metadata.name,
                    'res_namespace': res.metadata.namespace,
                    'res_kind': kind,
                    'cfg_kind': obj.kind,
                    'cfg_name': obj.metadata.name,
                    'cfg_version': obj.metadata.resource_version,
                    }
            self.q.put(item)
