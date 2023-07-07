import json
import logging
import os
import shutil
import threading
import xml.etree.ElementTree as ET
from datetime import datetime

import requests
# import codecs
from fastapi import APIRouter, Request, UploadFile, Form, File, HTTPException
from jsonpath_ng.ext import parse

from src import db
from src.commons import settings, data, reserved_filename, dmz_headers
from src.modules.datastation_bag_composer import Datastation_Bag_Composer
from src.modules.sword_ingester import SwordIngester
from src.modules.sword_tracking_poller import Sword_Tracking_Poller

# from src.main import settings

router = APIRouter()


@router.get("/settings")
async def get_settings():
    return settings


@router.post("/inbox/metadata")
async def process_metadata(request: Request, repo_target: str):
    files_name = []
    file_records = []
    filemetadata = {}
    logging.debug(repo_target)
    deposit_username = request.headers.get("target-username")
    deposit_password = request.headers.get("target-password")
    if (deposit_username or deposit_password) is None:
        raise HTTPException(status_code=401, detail="Please provide username and/or password.")
    repo_url = f'{settings.repo_selection_url}/{repo_target}'
    try:
        rsp = requests.get(repo_url)
        if rsp.status_code != 200:
            raise HTTPException(status_code=404, detail=f"{repo_url} not found")
    except Exception as ex:
        logging.debug(ex)
        raise HTTPException(status_code=404, detail=f"Error, caused by: {repo_url} could be down.")
    repo_json = rsp.json()
    try:
        form_metadata_json = await request.json()
        logging.debug(form_metadata_json)
        jsonpath_metadata_id = parse("$.id")

        for match in jsonpath_metadata_id.find(form_metadata_json):
            metadata_id = match.value
            break

        jsonpath_filemetadata_names = parse("$.file-metadata[*].name")
        for match in jsonpath_filemetadata_names.find(form_metadata_json):
            filename = match.value
            files_name.append(filename)
            file_records.append((filename, metadata_id))

        form_metadata_record = (metadata_id, datetime.now().strftime("%m/%d/%Y %H:%M:%S.%f"))

        # TODO : Return error when something wrong during database insert
        # Create temp folder
        tmp_dir = os.path.join(settings.DATA_TMP_BASE_DIR_UPLOAD, metadata_id)
        if os.path.exists(tmp_dir):
            shutil.rmtree(tmp_dir)
        os.makedirs(tmp_dir)
        # Write to original-metadata.json
        with open(os.path.join(tmp_dir, reserved_filename), mode="wt") as f:
            f.write(json.dumps(form_metadata_json))
        if len(file_records) == 0:
            if repo_json['deposit-protocol'] == 'SWORD2':
                # Ingest using sword

                receipt_links = await ingest_sword(form_metadata_json, tmp_dir, repo_json['transformer-name'],
                                                   repo_json['deposit-url'], deposit_username, deposit_password)
                db.insert_record(settings.DATA_DB_FILE, form_metadata_record, file_records)
                db.update_form_metadata_progress_state_url(settings.data_db_file, metadata_id, receipt_links)
                track_deposit(metadata_id, deposit_username, deposit_password)
                return {"status": "OK", "id": metadata_id, "receipt-links": receipt_links}
            else:
                raise HTTPException(status_code=501, detail=f"{repo_json['deposit-protocol']} is not implemented yet.")
        db.insert_record(settings.DATA_DB_FILE, form_metadata_record, file_records)
    except Exception as ex:
        error_msg = 'Unknown Error'
        for em in ex.args:
            logging.debug(em)
            if isinstance(em, str):
                error_msg = em

        if "UNIQUE constraint" in error_msg:
            error_msg = f"Metadata id" \
                        f" '{metadata_id}' already exist."
        else:
            db.delete_record_by_metadata_id(settings.DATA_DB_FILE, metadata_id)
        raise HTTPException(status_code=500, detail=error_msg)
    # save sword username/password when files need to upload
    # in this step, it means that the upload isn't finish yet
    if metadata_id in data:
        data.pop(metadata_id)
    data.update({metadata_id: {"transformer-name": repo_json['transformer-name'], "deposit-url": repo_json['deposit-url'],
                               "deposit-username": deposit_username, "deposit-password": deposit_password}})
    return {"status": "OK", "id": metadata_id, "files": files_name}


async def ingest_sword(form_metadata_json, tmp_dir, xslt_name, sword_url, sword_username, sword_password):
    print(f'sword_url: {sword_url}, sword_username: {sword_username}, sword_password: {sword_password}')

    si = SwordIngester(sword_url, sword_username, sword_password)

    dbc = Datastation_Bag_Composer(json.dumps(form_metadata_json), tmp_dir, xslt_name)
    bagzip = dbc.create_ds_bagit()
    logging.debug(bagzip)

    receipt_links = si.ingest(bagzip)

    ca_certs = None
    if settings.exists("ca_certs_file", fresh=False):
        ca_certs = settings.CA_CERTS_FILE
    sword_response = requests.get(receipt_links, headers=dmz_headers(sword_username, sword_password),
                                  verify=ca_certs)

    if sword_response.status_code == 200:
        xml_content = sword_response.text
        logging.debug(xml_content)
    else:
        logging.debug('Error occurred:', sword_response.status_code)
    # Parse the XML data
    root = ET.fromstring(xml_content)
    # Define the namespace
    namespace = {'atom': 'http://www.w3.org/2005/Atom'}
    # Find the element with the desired attribute value
    category_element = root.find('.//atom:category[@label="State"]', namespace)
    # Retrieve the attribute value
    attribute_value = category_element.attrib['term']
    # Print the attribute value
    logging.debug(f'PROTECTED: {attribute_value}')
    # Find the link element with the desired attribute value
    link_element = root.find('.//atom:link[@rel="self"][@href]', namespace)

    # Retrieve the value of the 'href' attribute
    attribute_value1 = link_element.attrib['href']
    return receipt_links


@router.post("/inbox/file")
async def process_files(metadataId: str = Form(), fileName: str = Form(), file: UploadFile = File()):
    files_name = []
    submitted_filename = fileName
    print(f"submitted_filename: {submitted_filename}")
    # Process the formId field
    if metadataId is not None:
        results = db.find_record_by_metadata_id(settings.data_db_file, metadataId)
        if not results:
            raise HTTPException(status_code=404, detail=f"metadataId '{metadataId}' not found.")

        tmp_dir = os.path.join(settings.DATA_TMP_BASE_DIR_UPLOAD, metadataId)

    else:
        raise HTTPException(status_code=404, detail=f"Not found")
    # Process the files
    files_uploaded_status = []
    if file is not None:
        metadata_id, file_name, uploaded = results[0] #results from database query
        for r in results:
            file_name, uploaded = r[1:] #start from column index 1 (file_name)
            # Save the uploaded file
            if submitted_filename == file_name:
                contents = await file.read()
                with open(os.path.join(tmp_dir, submitted_filename), "wb") as f:
                    f.write(contents)

                update_success = db.update_file_uploaded_status(settings.data_db_file, metadata_id, submitted_filename)
                if not update_success:
                    raise HTTPException(status_code=404, detail=f"metadata_id: {metadata_id}, submitted_filename: {submitted_filename} not found")
                files_uploaded_status.append({"name": file_name, "uploaded": True})
            else:
                if uploaded == 0:
                    files_uploaded_status.append({"name": file_name, "uploaded": False})
                else:
                    files_uploaded_status.append({"name": file_name, "uploaded": True})


    else:
        raise HTTPException(status_code=404, detail=f"Not found")

    all_files_uploaded = True
    for fus in files_uploaded_status:
        if fus['uploaded'] is False:
            all_files_uploaded = False
            break

    # Update record
    if all_files_uploaded:
        with open(os.path.join(tmp_dir, reserved_filename), 'r') as f:
            form_metadata_json = json.loads(f.read())

        try:
            user_data = data[metadata_id]
            if user_data is None:
                raise HTTPException(status_code=404, detail=f"metadata_id: {metadata_id} not found.")
        except KeyError as ke:
            raise HTTPException(status_code=404, detail=f"Error on metadata_id: {metadata_id}. Caused by: {ke.args[0]} not found.")

        receipt_links = await ingest_sword(form_metadata_json, tmp_dir, user_data['transformer-name'],
                                                   user_data['deposit-url'], user_data['deposit-username'], user_data['deposit-password'])
        db.update_form_metadata_progress_state_url(settings.DATA_DB_FILE, metadata_id, receipt_links)
        track_deposit(metadata_id, user_data['deposit-username'], user_data['deposit-password'])
        return {"status": "SUBMITTED", "id": metadata_id, "receipt-links": receipt_links}

    return {"status": "OK", "id": metadataId, "files": files_uploaded_status}


@router.delete("/inbox/{metadataId}")
def delete_inbox(metadataId: str):
    num_rows_deleted = db.delete_form_metadata_record(settings.DATA_DB_FILE, metadataId)
    return {"Deleted": "OK", "num-row-deleted": num_rows_deleted}


def check_sword_status(metadata_id, sword_username, sword_password):
    stp = Sword_Tracking_Poller(metadata_id, sword_username, sword_password, settings.interval_check_sword_status)
    stp.run()


def track_deposit(metadata_id, sword_username, sword_password):
    t1 = threading.Thread(target=check_sword_status, args=(metadata_id, sword_username, sword_password,))
    t1.start()
