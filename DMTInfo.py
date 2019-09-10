"""
"""

class DMTInfo:
    def __init__(self, app_name = '', uuid = '', versions = []):
        self.app_name = app_name
        self.uuid = uuid
        self.versions = versions

    def get_app_name(self):
        return self.app_name

    def set_app_name(self, app_name):
        self.app_name = app_name

    def get_uuid(self):
        return self.uuid

    def set_uuid(self, uuid):
        self.uuid = uuid

    def get_versions(self):
        return self.versions

    def set_versions(self, versions):
        self.versions = versions