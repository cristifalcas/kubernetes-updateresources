import logging, threading, time, json
from kubernetes import client
from kubernetes.client.rest import ApiException
from custom_libs import annotations

class threadApplyChanges (threading.Thread):
    def __init__(self, name, queue, timer_timeout=300):
        """
        For each controller that needs updated, write a custom annotation
        and maybe restart the necessary pods

        Receives a queue wiht what needs updating
        
        A thread is spawn for the update function
        The main class thread and the update thread both use for_update dict
        to write/delete, so a lock is used to ensure they don't step on each other toes
        """

        threading.Thread.__init__(self)
        self.log = logging.getLogger(__name__ + " " + name)
        self.log.info("Init threadApplyChanges")
        global opsguru_signature
        self.timer_timeout = timer_timeout
        self.q = queue
        self.v1 = client.CoreV1Api()
        self.v1b1 = client.AppsV1beta1Api()
        self.v1b2 = client.AppsV1beta2Api()
        self.v1b1e = client.ExtensionsV1beta1Api()
        self.ann = annotations.Annotations()
        self.for_update = {}
        self.for_update_lock = threading.Lock()

        t = threading.Thread(target=self.update)
        t.daemon = True
        t.start()

    def run(self):
        """
        Start getting elements from the queue. If the queue is empty, wait for more elements

        We consider that the elements needs to be restarted if the resource_version of the config
        is different then the one that we add as an annotation to the controller

        On the very first run, all pods are restarted because we need to insert our annotations

        Each element consists of an updated config/secret and a controller that needs to be restarted
        Since we can have cascading updates for different cms/secrets (user updates multiple cm at intervals of a few minutes),
        we put a timer for each controller. The timer is considered expired when crt_time - item_time > timer_timeout
        If we get an update for the same controller we reset item_time = crt_time

        For each controller set the necessary patch function and who should perform the update
        On kube 1.6:
         - only daemonsets and deployments have rolling updates
         - daemonset needs to have spec.updateStrategy.type=RollingUpdate. We are not managing this
         - replication_controllers and stateful_sets need to be updated manually
        """
        self.log.info("Starting thread")

        def f(x):
            return {
                    'DaemonSet': (self.update_rollingupdate, self.v1b1e.patch_namespaced_daemon_set),
                    'Deployment': (self.update_rollingupdate, self.v1b1e.patch_namespaced_deployment),
                    'ReplicationController': (self.update_manually, self.v1.patch_namespaced_replication_controller),
                    'StatefulSet': (self.update_manually, self.v1b1.patch_namespaced_stateful_set),
                    }.get(x)

        while True:
            try:
                item = self.q.get()
                kind = item['res_kind']

                if self.should_update(item):
                    key_res = item['res_namespace'] + "/" + item['res_name']
                    key_ann = self.ann.get_annotation(item['cfg_kind'], item['cfg_name'])
                    self.log.info("Needs update: %s %s by %s" % (kind, key_res, item['cfg_kind'] + '/' + item['cfg_name']))
                    with self.for_update_lock:
                        if key_res in self.for_update:
                            self.for_update[key_res]['changes'].update({key_ann: item['cfg_version']})
                        else:
                            (update_function, patch_func) = f(kind)
                            value = {}
                            value.update({'name':  item['res_name']})
                            value.update({'namespace': item['res_namespace']})
                            value.update({'changes': {key_ann: item['cfg_version']}})
                            value.update({'time': time.time()})
                            value.update({'patch_func': patch_func})
                            value.update({'update_function': update_function})
                            value.update({'kind': kind})
                            self.for_update.update({key_res: value})
                else:
                    # put the item back if not ready
                    self.log.info("We don't update %s yet. Not ready.", item['res_name'])
                    self.q.put(item)
 
                self.q.task_done()
                time.sleep(1)
            except BaseException as e:
                self.log.exception('{!r}. Restarting thread.'.format(e))
                time.sleep(1)
        self.log.info("Thread has been stopped")

    def should_update(self, item):
        """
        Check that all pods from the controller are running
        """
        self.log.info("Check if we should update %s %s", item['res_kind'], item['res_name'])

        try:
            pods = self.get_pods_for_controller(item['res_namespace'], item['res_name'], item['res_kind'])
            if not pods:
                self.log.info("No pods found for %s %s", item['res_kind'], item['res_name'])
                return False
            for pod in pods:
                if pod.status.phase != 'Running':
                    self.log.info("Pod name %s is %s", pod.metadata.name, pod.status.phase)
                    return False
            return True
        except ApiException as e:
            self.log.critical("Exception when calling read_function: %s\n" % e)

    def update(self):
        """
        Every 5 seconds block the update dict and start looking for elements that have the timer expired
        If the update is successful delete the key from dict
        """
        self.log.info("Update thread started")

        while True:
            time.sleep(5)
            with self.for_update_lock:
                for key in list(self.for_update):
                    if time.time() - self.for_update[key]['time'] > self.timer_timeout:
                        self.log.info("We will update %s", self.for_update[key]['name'])
                        name = self.for_update[key]['name']
                        namespace = self.for_update[key]['namespace']
                        body = self.ann.build_annotation(self.for_update[key]['changes'])
                        try:
                            patch_function = self.for_update[key]['patch_func']
                            patch_function(name=name, namespace=namespace, body=body)
                            update_function = self.for_update[key]['update_function']
                            update_function(name=name, namespace=namespace, self.for_update[key]['kind'])
                            del self.for_update[key]
                        except ApiException as e:
                            self.log.critical("Exception when calling patch_function: %s\n" % e)
                    else:
                        self.log.info("We will not update %s yet.", self.for_update[key]['name'])

    def update_rollingupdate(self, namespace, name, kind):
        """
        Nothing to do. Kubernetes will take care of everything
        """
        self.log.info("RollingUpdate resource %s:%s", kind, name)

    def update_manually(self, namespace, name, kind):
        """
        Not implemented
        """
        self.log.info("Manually update resource")

        return
        self.get_pods_for_controller(namespace, name, kind)

    def get_pods_for_controller(self, namespace, name, kind):
        """
        In order to ensure that we do safe updates, we first ensure that all pods
        from the controller are ready

        Deployments create a ReplicaSet that creates the pods
        """
        self.log.info("Get pods for controller %s %s" % (kind, name))

        if kind == 'Deployment':
            res = self.v1b1e.list_namespaced_replica_set(namespace=namespace)
            count = 0
            reference_name = ''
            for rs in res.items:
                for owner_reference in rs.metadata.owner_references:
                    if rs.status.replicas > 0 and owner_reference.kind == kind and owner_reference.name == name:
                        self.log.info("Found ReplicaSet %s for %s:%s", rs.metadata.name, kind, name)
                        reference_name = rs.metadata.name
                        reference_kind = 'ReplicaSet'
                        count += 1
            if count > 1:
                self.log.error("Too many (%s) ReplicaSets for %s:%s", count, kind, name)
                return False
        else:
            # kind: 'DaemonSet', 'ReplicationController', 'StatefulSet'
            reference_name = name
            reference_kind = kind

        pods_array = []
        pods = self.v1.list_namespaced_pod(namespace=namespace)
        for pod in pods.items:
            if pod.metadata.annotations and 'kubernetes.io/created-by' in pod.metadata.annotations:
                ann = json.loads(pod.metadata.annotations['kubernetes.io/created-by'])
                ref = ann['reference']
                if ref['name'] == reference_name and ref['kind'] == reference_kind:
                    pods_array.append(pod)
        return pods_array
