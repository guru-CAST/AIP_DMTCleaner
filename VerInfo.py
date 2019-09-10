"""
"""

import os
import tempfile as TF
import xml.etree.ElementTree as ET
import traceback

class VerInfo:
    def __init__(self, uuid = '', name = '', status = '', date = '', entity_file = '', has_prev_ver = False):
        self.status = status
        self.uuid = uuid
        self.name = name
        self.status = status
        self.date = date
        self.entity_file = entity_file
        self.has_prev_ver = has_prev_ver

    def get_uuid(self):
        return self.uuid

    def set_uuid(self, uuid):
        self.uuid = uuid

    def get_name(self):
        return self.name

    def set_name(self, name):
        self.name = name

    def get_status(self):
        return self.status

    def set_status(self, status):
        self.status = status

    def get_date(self):
        return self.date

    def set_date(self, date):
        self.date = date

    def get_entity_file(self):
        return self.entity_file

    def set_entity_file(self, entity_file):
        self.entity_file = entity_file

    def get_has_prev_ver(self):
        return self.has_prev_ver

    def set_has_prev_ver(self, has_prev_ver):
        self.has_prev_ver = has_prev_ver

    def clear_prev_version(self):
        # Update is only needed when there is a previous version setting in the entity file.

        if (self.get_has_prev_ver()):
            try:
                xml_tree = ET.parse(self.get_entity_file())
                root = xml_tree.getroot()

                for entry in root.iter('delivery.Version'):
                    #print('TAG:%s' % entry.tag)
                    #print('First child:%s' % entry.attrib.get('previousVersionEntry'))
                    entry.set('previousVersionEntry', "")
                    #print('First child:%s' % entry.attrib.get('previousVersionEntry'))
                    #print('ATTR:%s' % attr.get('previousVersionEntry').text)

                # TODO: Error handling
                entity_file = self.get_entity_file()
                old_entity_file = entity_file + "_OLD"

                os.rename(entity_file, old_entity_file)

                # NOTE: The file created below will not have the following line and hence will not be usable.
                # So, using a workaround here.

                # Creating a brand new entity file and and appending it the contents of the temp file.
#                with open(entity_file, 'w+') as f:
#                    f.write('<?xml version="1.0" encoding="UTF-8"?>')

                with TF.NamedTemporaryFile(delete=False) as temp_entity_file:
                    xml_tree.write(temp_entity_file)

                #print(temp_entity_file.name)

                with open(temp_entity_file.name, 'r') as fi:
                    buffer = '<?xml version="1.0" encoding="UTF-8"?>' + fi.read()
                    with open(entity_file, 'w') as f:
                        f.write(buffer)
                        f.close()
                    fi.close()
                    
                os.remove(temp_entity_file.name)
                os.remove(old_entity_file)

            except (TypeError, AttributeError) as dom_exc:
                traceback.print_exc()
                print('An exception occurred while reading delivery index file. Cannot continue..')
                raise
        else:
            return True