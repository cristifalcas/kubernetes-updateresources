#!/bin/python
from __future__ import print_function
import logging, traceback, time, signal, sys, threading, os
from custom_libs import logger, watchchanges, applychanges
from kubernetes import client, config
from Queue import Queue 

def signal_handler(signal, frame):
        sys.exit(0)
signal.signal(signal.SIGINT, signal_handler)

update_resource_timeout = os.getenv('UPDATE_RESOURCE_TIMEOUT', 300)

if __name__ == '__main__':
    """
    Start watchers for configmap and secret changes.
    When something is updated, a queue is populated with relevant info for an update

    Start a worker that gets elements from the queue and updates/restarts resources accordingly
    """

    try:
        logger.Mylog()
        log = logging.getLogger(__name__)
        log.info("Loading kubernetes config")
        config.load_incluster_config()
        config.connection_pool_maxsize = 500
        # config.load_kube_config('/etc/kubernetes/admin.conf')
        v1 = client.CoreV1Api()
        q = Queue()
 
        log.info("Starting watchers")
        cm = watchchanges.threadWatchChanges("configmaps", q, v1.list_config_map_for_all_namespaces)
        cm.daemon = True
        cm.start()
        secret = watchchanges.threadWatchChanges("secrets", q, v1.list_secret_for_all_namespaces)
        secret.daemon = True
        secret.start()
 
        log.info("Starting worker")
        worker = applychanges.threadApplyChanges("worker", q, update_resource_timeout)
        worker.daemon = True
        worker.start()

        while True:
            # Main thread sleeping for ever
            log.debug("Current number of threads: " + str(threading.active_count()))
            time.sleep(1)
 
    except Exception, e:
        traceback.print_exc()
        sys.exit(1)

# yum -y install epel-release 
# yum -y install python2-pip
# # the api is different betwen v3 and v4
# pip install kubernetes==4.0.0
# pip install backports.ssl-match-hostname>=3.5.0 --upgrade
