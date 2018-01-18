import logging, yaml

class Annotations():
    def __init__(self):
        """
        """

        self.log = logging.getLogger(__name__)
        self.log.info("Init Annotations")
        self.opsguru_domain = 'opsguru.signature'
        self.opsguru_signature = self.opsguru_domain +'/should_update'
        self.opsguru_signature_value = 'True'

    def has_signature(self, res):
        """
        Check if the controller has our signature
        """
        self.log.debug("has_signature")

        ann_path = res.metadata.annotations
        if ann_path and self.opsguru_signature in ann_path and ann_path[self.opsguru_signature] == self.opsguru_signature_value:
            return True
        return False

    def get_version(self, res, kind, name):
        """
        Check if the controller has any annotation for the respective config
        Return False if we don't find it, otherwise return the value
        """
        self.log.debug("get_version")

        annotations =  res.spec.template.metadata.annotations
        if not annotations:
            return False
        key_ann = self.get_annotation(kind, name)
        if key_ann in annotations:
            return annotations[key_ann]
        else:
            return False

    def get_annotation(self, kind, name):
        """
        Check if the controller has any annotation for the respective config
        Return False if we don't find it, otherwise return the value
        """
        self.log.debug("get_annotation")

        return self.opsguru_domain + '/' + kind + '.' + name

    def build_annotation(self, changes):
        """
        Built the annotation for patching
        """
        self.log.debug("build_annotation")

        ann_list = []
        for change in changes:
            ann_list.append('"' + change + '": "' + changes[change] + '"')
        annotation = '{"spec": {"template": {"metadata":{"annotations":{' + ",".join(ann_list) + '}}}}}'
        return yaml.load(annotation)
