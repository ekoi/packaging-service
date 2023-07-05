import os
import shutil
from datetime import datetime

import polling2
import requests

from src import db
from src.commons import settings
import xml.etree.ElementTree as ET


class Sword_Tracking_Poller:
    def __init__(self, metadata_id, sword_username, sword_password, interval_time_in_seconds):
        self.metadata_id = metadata_id
        self.sword_username = sword_username
        self.sword_password = sword_password
        self.interval_time_in_seconds = interval_time_in_seconds

    def is_published(self, deposit_state):
        resp_link = db.find_progress_state_url_by_metadata_id(settings.DATA_DB_FILE, self.metadata_id)[0][0]
        print(resp_link)
        response = requests.get(resp_link, auth=(self.sword_username, self.sword_password))
        # rsp.status_code != 200:
        if response.status_code == 200:
            xml_content = response.text
            print(xml_content)
            # Parse the XML data
            root = ET.fromstring(xml_content)

            # Define the namespace
            namespace = {'atom': 'http://www.w3.org/2005/Atom'}

            # Find the element with the desired attribute value
            category_element = root.find('.//atom:category[@label="State"]', namespace)

            # Retrieve the attribute value
            deposit_state = category_element.attrib['term']
            db.update_form_metadata_progress_state(settings.DATA_DB_FILE, self.metadata_id, deposit_state)

            # Print the attribute value
            print(deposit_state)
            """Check that the deposit_state returned 'SUBMITTED'"""
            if 'SUBMITTED' == deposit_state or 'FINALIZING' == deposit_state:
                # Continue to poll
                return False
            elif deposit_state == 'PUBLISHED':
                # Find the link elements with the desired attribute values
                link_elements = root.findall('.//atom:link[@rel="self"][@href]', namespace)

                # Retrieve the values of the 'href' attributes
                attribute_values = [link_element.attrib['href'] for link_element in link_elements]

                # Print the attribute values
                for value in attribute_values:
                    if 'urn:nbn' in value:
                        urn_nbn_value = value
                    else:
                        pid_value = value
                db.update_form_metadata_pid_urn_nbn(settings.DATA_DB_FILE, self.metadata_id, pid_value, urn_nbn_value)
                shutil.rmtree(os.path.join(settings.data_tmp_base_dir_upload, self.metadata_id))
                os.remove(os.path.join(settings.data_tmp_base_dir_zips, f'{self.metadata_id}.zip'))
            else:
                # When Failed or rejected
                error_msg = category_element.text
                db.update_form_metadata_error(settings.DATA_DB_FILE, self.metadata_id, error_msg)

            return True

        print('Error occurred:', response.status_code)
        # TODO: Return exception!
        return True  # exit the polling

    def run(self):
        polling2.poll(target=lambda: self.is_published(self), step=self.interval_time_in_seconds,
                      poll_forever=True)
