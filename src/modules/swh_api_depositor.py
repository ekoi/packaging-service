from __future__ import annotations

from time import sleep

import json

import jmespath
import requests

from src.bridge import Bridge
from src.models.bridge_output_model import BridgeOutputDataModel, TargetResponse, ResponseContentType, IdentifierItem, \
    IdentifierProtocol
from src.commons import logger, settings
from src.dbz import DepositStatus


class SwhApiDepositor(Bridge):

    def deposit(self) -> BridgeOutputDataModel:
        logger(f'DEPOSIT to {self.target.repo_name}', 'debug', self.app_name)
        target_response = TargetResponse()
        target_swh = jmespath.search("metadata[*].fields[?name=='repository_url'].value",
                                     json.loads(self.metadata_rec.md))
        bridge_output_model = BridgeOutputDataModel(response=target_response)
        headers = {'Content-Type': 'application/json', 'Authorization': f'Bearer {settings.SWH_ACCESS_TOKEN}'}
        logger(f'self.target.target_url: {self.target.target_url}', 'debug', self.app_name)
        url = f'{self.target.target_url}/{target_swh[0][0]}/'
        api_resp = requests.post(url, data="{}", headers=headers)
        logger(f'{api_resp.status_code} {api_resp.text}', 'debug', self.app_name)
        if api_resp.status_code == 200:
            api_resp_json = api_resp.json()
            logger(f'swh_api response json: {json.dumps(api_resp_json)}', 'debug', self.app_name)
            goto_sleep = False
            counter = 0 # TODO: Refactor using Tenancy!
            while True and (counter < settings.SWH_API_MAX_RETRIES):
                counter += 1
                swh_check_url = api_resp_json.get("request_url")
                check_resp = requests.get(swh_check_url, headers=headers)
                if check_resp.status_code == 200:
                    swh_resp_json = check_resp.json()
                    logger(f'{swh_check_url} response: {json.dumps(swh_resp_json)}', 'debug', self.app_name)
                    if swh_resp_json.get('save_task_status') == DepositStatus.FAILED:
                        bridge_output_model.deposit_status = DepositStatus.FAILED
                        logger(f"save_task_status is failed.", 'error', self.app_name)
                        break
                    elif swh_resp_json.get('snapshot_swhid'):
                        bridge_output_model.deposit_status = DepositStatus.FINISH
                        target_response.status_code = check_resp.status_code
                        target_response.content_type = ResponseContentType.JSON
                        target_response.content = json.dumps(swh_resp_json)
                        target_response.status = DepositStatus.SUCCESS
                        identifier_items = []
                        target_response.identifiers = identifier_items
                        ideni = IdentifierItem(value=swh_resp_json.get('snapshot_swhid'), url=url,
                                               protocol=IdentifierProtocol('swhid'))
                        identifier_items.append(ideni)
                        break
                    else:
                        goto_sleep = True
                if goto_sleep:
                    sleep(settings.SWH_DELAY_POLLING)

        else:
            logger(f'ERROR api_resp.status_code: {api_resp.status_code}', 'debug', self.app_name)
            target_response.status_code = api_resp.status_code
            bridge_output_model.deposit_status = DepositStatus.ERROR
            target_response.error = json.dumps(api_resp.json())
            target_response.content_type = ResponseContentType.JSON
            target_response.status = DepositStatus.ERROR
            target_response.content = json.dumps(api_resp.json())
            logger(f'bridge_output_model: {bridge_output_model.model_dump(by_alias=True)}', 'debug', self.app_name)

        return bridge_output_model
