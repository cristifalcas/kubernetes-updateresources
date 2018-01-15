# on the very first run, all pods are restarted because we need to insert our annotation
import logging, threading, time, yaml
from pprint import pprint
from kubernetes import client
from kubernetes.client.rest import ApiException

class threadApplyChanges (threading.Thread):
    def __init__(self, name, q):
        """
        For each controller that needs updated, write a custom annotation

        Receives a queue from where to get what needs updating
        """
        threading.Thread.__init__(self)
        self.log = logging.getLogger(__name__ + " " + name)
        self.log.info("Init threadApplyChanges")
        global opsguru_signature
        self.timer_timeout = 3
        self.q = q
        self.v1 = client.CoreV1Api()
        self.v1b1 = client.AppsV1beta1Api()
        self.v1b1e = client.ExtensionsV1beta1Api()
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

        Each element consists of an updated config and a controller that needs to be updated
        Since we can have cascading updates for different cms/secrets, we put a timer for each
        controller. The timer is considered expired when crt_time - item_time > 300
        If we get an update for the same controller we reset item_time = crt_time

        For each controller set the necessary patch function and who should perform the update
        On kube 1.6:
         - only daemonsets and deployments have rolling updates
         - daemonset need to have spec.updateStrategy.type=RollingUpdate. We are not managing this
         - replication_controllers and stateful_sets need to be updated manually
        """
        self.log.info("Starting thread")
        while True:
            try:
                item = self.q.get()

                kind = item['res_kind']
                if kind == 'DaemonSet':
                    patch_func = self.v1b1e.patch_namespaced_daemon_set
                    update_function = self.update_rollingupdate
                    read_function = self.v1b1e.read_namespaced_daemon_set
                elif kind == 'Deployment':
                    patch_func = self.v1b1e.patch_namespaced_deployment
                    update_function = self.update_rollingupdate
                    read_function = self.v1b1e.read_namespaced_deployment
                elif kind == 'ReplicationController':
                    patch_func = self.v1.patch_namespaced_replication_controller
                    update_function = self.update_manually
                    read_function = self.v1.read_namespaced_replication_controller
                elif kind == 'StatefulSet':
                    patch_func = self.v1b1.patch_namespaced_stateful_set
                    update_function = self.update_manually
                    read_function = self.v1b1.read_namespaced_stateful_set
                else:
                    self.log.error('Unknown kind %s', kind)
                    return

                ret = self.should_update(item, read_function)
                if ret:
                    key_res = item['res_namespace'] + "/" + item['res_name']
                    self.log.info("Needs update: %s, by %s", key_res, item['cfg_kind'] + '/' + item['cfg_name'])
                    if key_res in self.for_update:
                        self.for_update[key_res]['changes'].update(ret)
                    else:
                        value = {}
                        value.update({'name':  item['res_name']})
                        value.update({'namespace': item['res_namespace']})
                        value.update({'changes': ret})
                        value.update({'time': time.time()})
                        value.update({'patch_func': patch_func})
                        value.update({'update_function': update_function})

                    with self.for_update_lock:
                        self.for_update.update({key_res: value})
 
                self.q.task_done()
            except BaseException as e:
                self.log.exception('{!r}. Restarting thread.'.format(e))
                time.sleep(1)
        self.log.info("Thread has been stopped")

    def should_update(self, item, read_function):
        """
        Check if we have the corresponding annotation at the correct version
        """
        self.log.debug("should_update")
        opsguru_annotation = 'opsguru.signature/' + item['cfg_kind'] + '.' + item['cfg_name']
        try:
            res = read_function(name=item['res_name'], namespace=item['res_namespace'])
            ann = res.spec.template.metadata.annotations
            if ann and opsguru_annotation in ann and ann[opsguru_annotation] == item['cfg_version']:
                return False
            return {opsguru_annotation: item['cfg_version']}
        except ApiException as e:
            self.log.critical("Exception when calling read_function: %s\n" % e)

    def update(self):
        """
        
        """
        self.log.debug("update")
        while True:
            time.sleep(5)
            with self.for_update_lock:
                for key in list(self.for_update):
                    if time.time() - self.for_update[key]['time'] > 5:
                        patch_function = self.for_update[key]['patch_func']
                        update_function = self.for_update[key]['update_function']
                        name = self.for_update[key]['name']
                        namespace = self.for_update[key]['namespace']
                        ann_list = []
                        for change in self.for_update[key]['changes']:
                            ann_list.append('"' + change + '": "' + self.for_update[key]['changes'][change] + '"')
                        annotation = '{"spec": {"template": {"metadata":{"annotations":{' + ",".join(ann_list) + '}}}}}'
                        body = yaml.load(annotation)
                        pprint(body)
                        try:
                            patch_function(name=name, namespace=namespace, body=body)
                            update_function()
                            del self.for_update[key]
                        except ApiException as e:
                            self.log.critical("Exception when calling read_function: %s\n" % e)

    def update_rollingupdate(self):
        """
        Nothing to do. Kubernetes will take care of everything
        """
        self.log.info("rollingupdate update resource")

    def update_manually(self):
        """
        Not implemented
        """
        self.log.info("manually update resource")

