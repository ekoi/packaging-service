from __future__ import annotations

import json
from datetime import datetime

import requests
from requests.auth import HTTPBasicAuth

from src.bridge import Bridge
from src.models.bridge_output_model import BridgeOutputDataModel, TargetResponse
from src.commons import settings, DepositStatus, transform, logger, db_manager
from time import sleep
import sword2.deposit_receipt as dr


class SwhSwordDepositor(Bridge):

    def deposit(self) -> BridgeOutputDataModel:

        bridge_output_model = BridgeOutputDataModel()
        # create_sword_payload(self):
        swh_form_md = json.loads(self.metadata_rec.md)
        dv_target = db_manager.find_target_repo(self.dataset_id, self.target.input.from_target_name)
        if dv_target:
            swh_form_md.update({"doi": json.loads(dv_target.target_output)['response']['identifiers'][0]['value']})
        logger(f"SwhSwordDepositor- swh_form_md - after update (doi): {json.dumps(swh_form_md)}", 'debug', self.app_name)
        str_sword_payload = transform(
            transformer_url=self.target.metadata.transformed_metadata[0].transformer_url,
            str_tobe_transformed=json.dumps(swh_form_md)
        )
        logger(f'deposit to "{self.target.target_url}"', "debug", self.app_name)
        headers = {
            'Content-Type': 'application/atom+xml;type=entry',
        }
        auth = HTTPBasicAuth(settings.swh_sword_username, settings.swh_sword_password)
        response = requests.post(self.target.target_url, headers=headers, auth=auth, data=str_sword_payload)
        logger(f'status_code: {response.status_code}. Response: {response.text}', "debug", self.app_name)
        if response.status_code == 200 or response.status_code == 201:  # TODO: remove 200, use only 201
            rt = response.text
            logger(f'sword response: {rt}', 'debug', self.app_name)
            deposit_response = dr.Deposit_Receipt(xml_deposit_receipt=rt)
            status_url = deposit_response.alternate
            logger(f'Status request send to {status_url}', 'debug', self.app_name)
            counter = 0
            while True and (counter < settings.swh_api_max_retries):
                counter += 1
                sleep(settings.swh_delay_polling_sword)
                rsp = requests.get(status_url, headers=headers, auth=auth)
                if rsp.status_code == 200:
                    rsp_text = rsp.text
                    logger(f'response from {status_url} is {rsp_text}', 'debug', self.app_name)
                    rsp_dep = dr.Deposit_Receipt(xml_deposit_receipt=rsp_text)
                    print(rsp_dep.metadata)
                    swh_metadata = rsp_dep.metadata
                    swh_deposit_status = swh_metadata.get('atom_deposit_status')
                    if swh_deposit_status and swh_deposit_status[0] == DepositStatus.DEPOSITED:
                        target_repo = TargetResponse(url=self.target.target_url, status=DepositStatus.FINISH,
                                                     message=rsp_text, content=rsp_text)
                        target_repo.url = status_url
                        target_repo.status_code = rsp.status_code
                        target_repo.content = rsp_text
                        bridge_output_model.deposit_status = DepositStatus.FINISH
                        bridge_output_model.response = target_repo
                        bridge_output_model.deposit_time = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f")
                        return bridge_output_model

                else:
                    raise ValueError(
                        f'Error request to {status_url} with rsp.status_code: {rsp.status_code} and rsp.text: {rsp.text}')
        else:
            bridge_output_model.deposit_status = DepositStatus.ERROR
            bridge_output_model.notes = response.text
        return bridge_output_model




