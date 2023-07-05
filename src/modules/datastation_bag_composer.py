import json
import logging
import mimetypes
import os
import shutil
from datetime import datetime, timezone

import bagit
import jinja2
import requests

from src.commons import settings


#Create metadata folder
def create_bagit_file(source, destination):
    base = os.path.basename(destination)
    name = base.split('.')[0]
    format = base.split('.')[1]
    archive_from = os.path.dirname(source)
    archive_to = os.path.basename(source.strip(os.sep))
    logging.debug(source, destination, archive_from, archive_to)
    shutil.make_archive(name, format, archive_from, archive_to)
    shutil.move('%s.%s' % (name, format), destination)


class Datastation_Bag_Composer:
    def __init__(self, metadata_json, files_folder, xslt_name):
        self.bag_dir = files_folder
        # Initiate jinja template. TODO: refactoring - move to other class(?) or startup
        self.templateEnv = jinja2.Environment(loader=jinja2.FileSystemLoader(
            searchpath=settings.jinja_template_dir))
        #Create a bag, a data folder will be created and move all files to data folder
        bag = bagit.make_bag(self.bag_dir, checksums=["sha1"])
        self.bag_data_dir = os.path.join(self.bag_dir, 'data')
        #Create metadata folder for dataset.xml and files.xml
        metadata_dir = os.path.join(self.bag_dir, 'metadata')
        os.makedirs(metadata_dir)
        # Create files.xml
        with open(os.path.join(metadata_dir, 'files.xml'), mode="wt") as f:
            f.write(self.create_easy_files())
        # Create dataset.xml
        try:
            with open(os.path.join(metadata_dir, 'dataset.xml'), mode="wt") as f:
                f.write(self.create_easy_dataset(metadata_json, xslt_name))
        except ValueError as ve:
            raise ve

    def create_easy_dataset(self, metadata_json, xslt_name):
        headers = {
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {settings.dans_transformer_api_key}'
        }
        transformer_response = requests.post(f'{settings.transformer_url}/{xslt_name}', headers=headers, data=metadata_json)
        if transformer_response.status_code == 200:
            easy_dataset_xml = transformer_response.json()
            str_easy_dataset_xml = easy_dataset_xml['result']
            logging.debug(str_easy_dataset_xml)
            return str_easy_dataset_xml

        raise ValueError(f"Error - Transfomer response status code: {transformer_response.status_code}")

    def create_easy_files(self):

        filelist = []
        for filename in os.listdir(self.bag_data_dir):
            f = os.path.join(self.bag_data_dir, filename)
            # checking if it is a file
            if os.path.isfile(f):
                # TODO: Mimetypes - use type verification service (?)
                filelist.append({"filename": filename, "mimetype": mimetypes.guess_type(f)[0]})

        easy_files_template = self.templateEnv.get_template("files.xml")
        easy_files_xml = easy_files_template.render(files=filelist)
        return easy_files_xml

    def create_ds_bagit(self):
        bag = bagit.Bag(self.bag_dir)
        bag.info['Created'] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000+00:00")
        bag.save(manifests=True)
        logging.debug("BagIt file created successfully!")
        #validate
        try:
            bag.validate()
            logging.debug("BagIt file validated successfully!")
            zip_name = os.path.basename(self.bag_dir)
            zip_output_path = f'{os.path.join(settings.data_tmp_base_dir_zips, zip_name)}.zip'
            create_bagit_file(self.bag_dir, zip_output_path)
            return os.path.abspath(zip_output_path)
        except bagit.BagValidationError as e:
            for d in e.details:
                if isinstance(d, bagit.ChecksumMismatch):
                    logging.debug("expected %s to have %s checksum of %s but found %s" %
                          (d.path, d.algorithm, d.expected, d.found))


