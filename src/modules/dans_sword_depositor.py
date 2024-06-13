import json
import mimetypes
import os
import shutil
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from time import sleep

import bagit
import jmespath
import requests
from simple_file_checksum import get_checksum
from sword2 import Connection

from src.bridge import Bridge, BridgeOutputDataModel
from src.commons import (settings, transform, logger, db_manager, handle_deposit_exceptions, dmz_dataverse_headers)
from src.dbz import DataFile, DepositStatus, FilePermissions
from src.models.bridge_output_model import TargetResponse, ResponseContentType, IdentifierProtocol, IdentifierItem


class DansSwordDepositor(Bridge):

    @handle_deposit_exceptions
    def deposit(self) -> BridgeOutputDataModel:
        md_json = json.loads(self.metadata_rec.md)
        files_metadata = jmespath.search('"file-metadata"[*]', md_json)
        # Creating generated file
        generated_files = self.__create_generated_files()
        for gf in generated_files:
            files_metadata.append({"name": gf.name, "mimetype": gf.mime_type, "private": gf.permissions})
        if generated_files: db_manager.insert_datafiles(generated_files)
        # Update the file-metadata: added some attributes
        md_json.update({"file-metadata": files_metadata})
        # updating mimetype of user's uploaded files since no mimetype in the form-metadata submission
        for _ in db_manager.find_non_generated_files(dataset_id=self.dataset_id):
            f_json = jmespath.search(f'[?name == \'{_.name}\']', files_metadata)
            logger(f'{self.__class__.__name__} f_json: {f_json}', 'debug', self.app_name)
            f_json[0].update({"mimetype": _.mime_type})

        str_updated_metadata_json = json.dumps(md_json)
        logger(f'str_updated_metadata_json: {str_updated_metadata_json}', 'debug', self.app_name)
        bagit_path = self.__create_bag(str_updated_metadata_json)
        output_response = self.__ingest(bagit_path=bagit_path)
        logger(f'dans_sword_response_xml: {output_response}', 'debug', self.app_name)
        # if ingest is successfully, clean bagit
        if output_response.deposit_status == DepositStatus.ACCEPTED:
            logger(f'Successfully ingested {output_response}: {output_response}', 'debug', self.app_name)
            logger(f'Deleting bagit file: {bagit_path}', 'debug', self.app_name)
            os.remove(bagit_path)
            shutil.rmtree(self.dataset_dir)

        return output_response

    def __ingest(self, bagit_path: str) -> BridgeOutputDataModel:
        sword_conn = Connection(self.target.target_url, headers=dmz_dataverse_headers('API_KEY',
                                                                                      self.target.password))
        logger(f'SENDING SWORD for {bagit_path} filename: {bagit_path}', 'debug', self.app_name)
        error_occured = False
        with open(bagit_path, "rb") as pkg:
            # try:
            receipt = sword_conn.create(col_iri=self.target.target_url, payload=pkg,
                                        mimetype="application/zip", filename=os.path.basename(bagit_path),
                                        packaging='http://purl.org/net/sword/package/BagIt',
                                        in_progress=False)  # As the deposit isn't yet finished

            resp_link = receipt.links['http://purl.org/net/sword/terms/statement'][0]['href']
            logger(f'resp_link: {resp_link}', 'debug', self.app_name)

            while True:  # TODO: User Tenacity!
                logger(f'CHECKING every {settings.interval_check_sword} secs', 'debug', self.app_name)
                target_resp = self.__is_published(resp_link=resp_link)
                deposit_state = target_resp.status
                retries = True if deposit_state in [DepositStatus.SUBMITTED.value, DepositStatus.FINALIZING.value] \
                    else False

                if retries:
                    logger(
                        f"I'm going to sleep for {settings.interval_check_sword} seconds. "
                        f"Please wait for me! deposit_state: {deposit_state}", 'debug', self.app_name)
                    sleep(settings.interval_check_sword)
                else:
                    break
        bridge_output_model = BridgeOutputDataModel(message=target_resp.message, response=target_resp)
        bridge_output_model.deposit_time = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f")
        bridge_output_model.deposit_status = deposit_state
        return bridge_output_model

    def __create_bag(self, str_updated_metadata: str) -> str:
        bag = bagit.make_bag(self.dataset_dir, checksums=["sha1"])
        # Now create files in target-dir
        self.__generated_dans_sword_files(str_updated_metadata)
        bag = bagit.Bag(self.dataset_dir)
        bag.info['Created'] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000+00:00")
        bag.save(manifests=True)
        logger("BagIt file created successfully!", 'debug', self.app_name)

        bag.validate()
        logger("BagIt file validated successfully!", 'debug', self.app_name)
        bagit_path = f'{self.dataset_dir}.zip'
        self.__create_bagit_file(self.dataset_dir, bagit_path, self.app_name)
        logger(f'bagit_path: {bagit_path}', 'debug', self.app_name)
        return bagit_path

    def __generated_dans_sword_files(self, str_updated_metadata) -> type(None):
        for generated_file in self.target.metadata.transformed_metadata:
            gf_dir = generated_file.target_dir
            if not gf_dir:
                continue  # generated files see self.__create_generated_files()
            else:
                gf_dir = os.path.join(self.dataset_dir, gf_dir)
                if not os.path.exists(gf_dir): os.makedirs(gf_dir)
                gf_path = os.path.join(gf_dir, generated_file.name)
                if generated_file.transformer_url:
                    gf_str = transform(generated_file.transformer_url, str_updated_metadata)
                    with open(gf_path, mode="wt") as f:
                        f.write(gf_str)

    def __create_generated_files(self) -> [DataFile]:
        generated_files = []
        # Create generated file in target-dir. The base directory is the app_name/target-dir
        for gnr_file in self.target.metadata.transformed_metadata:
            gf_dir = gnr_file.target_dir
            if gf_dir:
                continue  # exclude target-dir="metadata"
            else:
                gf_filename = gnr_file.name
                gf_restricted = gnr_file.restricted
                gf_path = os.path.join(self.dataset_dir, gf_filename)
                if gnr_file.transformer_url:
                    # generate file content
                    gf_str = transform(gnr_file.transformer_url, self.metadata_rec.md)
                    # write file
                    with open(gf_path, mode="wt") as f:
                        f.write(gf_str)
                else:
                    # No transformer-url, so the file content is from the original input metadata
                    with open(gf_path, mode="wt") as f:
                        f.write(self.metadata_rec.md)

                gf_mimetype = mimetypes.guess_type(gf_path)[0]
                fp = FilePermissions.PRIVATE if gf_restricted else FilePermissions.PUBLIC
                generated_files.append(DataFile(ds_id=self.dataset_id, name=gf_filename, path=gf_path,
                                                size=os.path.getsize(gf_path), mime_type=gf_mimetype,
                                                checksum_value=get_checksum(gf_path, algorithm="MD5"),
                                                date_added=datetime.utcnow(), permissions=fp, generated=True))
        return generated_files

    def __create_bagit_file(self, source, destination, app_name: str) -> type(None):
        _base = os.path.basename(destination)
        name = _base.split('.')[0]
        _format = _base.split('.')[1]
        archive_from = os.path.dirname(source)
        archive_to = os.path.basename(source.strip(os.sep))
        logger(f'source:{source} dest:{destination}, from:{archive_from}, to:{archive_to}', 'debug', app_name)
        shutil.make_archive(name, _format, archive_from, archive_to)
        shutil.move('%s.%s' % (name, _format), destination)

    def __is_published(self, resp_link: str) -> TargetResponse:
        logger(f'is_publish: {resp_link}. ds_api_key: {self.target.password}', 'debug', self.app_name)
        retries = False
        identifier_items = []
        deposit_state = DepositStatus.SUCCESS
        message = ''
        sword_resp = requests.get(resp_link, headers=dmz_dataverse_headers('API_KEY', self.target.password), verify=False)
        logger(f'sword_resp.status_code: {sword_resp.status_code}', 'debug', self.app_name)
        if sword_resp.status_code == 200:
            logger(f'dans_sword_response_xml: {sword_resp.text}', 'debug', self.app_name)
            root = ET.fromstring(sword_resp.text)
            namespace = {'atom': 'http://www.w3.org/2005/Atom'}
            category_element = root.find('.//atom:category[@label="State"]', namespace)
            message = category_element.text
            deposit_state = category_element.attrib['term'].lower()
            logger(f'deposit_state: {deposit_state}', 'debug', self.app_name)
            if deposit_state == DepositStatus.ACCEPTED.value:
                # link_element = root.find('.//atom:link[@rel="self"][@href]', namespace)
                db_manager.set_dataset_published(self.dataset_id)
                url = root.find('.//atom:entry/atom:link[@rel="self"][@href]', namespace).attrib['href']
                doi = url.split("doi.org/")[1]
                ideni = IdentifierItem(value=doi, url=url, protocol=IdentifierProtocol('doi'))
                identifier_items.append(ideni)

        else:
            logger(
                f'Error. sword_resp.status_code:{sword_resp.status_code} sword_resp.tex: '
                f'{sword_resp.text}', 'error', self.app_name)
            raise ValueError(f'Error word_resp.status_code:{sword_resp.status_code} - {sword_resp.text}')
        logger(f'sword_resp.text: {sword_resp.text}', 'debug', self.app_name)
        target_repo = TargetResponse(url=resp_link, status=deposit_state, message=message,
                                     identifiers=identifier_items, content=sword_resp.text)
        target_repo.content_type = ResponseContentType.XML
        target_repo.status_code = sword_resp.status_code
        return target_repo
