from __future__ import annotations

import json
from time import sleep

import jmespath
import requests

from src.bridge import Bridge
from src.commons import logger, settings, LOG_LEVEL_DEBUG
from src.dbz import DepositStatus
from src.models.bridge_output_model import BridgeOutputDataModel, TargetResponse, ResponseContentType, IdentifierItem, \
    IdentifierProtocol


class SwhApiDepositor(Bridge):

    def deposit(self) -> BridgeOutputDataModel:
        logger(f'DEPOSIT to {self.target.repo_name}', LOG_LEVEL_DEBUG, self.app_name)
        target_response = TargetResponse()
        target_swh = jmespath.search("metadata[*].fields[?name=='repository_url'].value",
                                     json.loads(self.metadata_rec.md))
        bridge_output_model = BridgeOutputDataModel(response=target_response)
        headers = {'Content-Type': 'application/json', 'Authorization': f'Bearer {settings.SWH_ACCESS_TOKEN}'}
        logger(f'self.target.target_url: {self.target.target_url}', LOG_LEVEL_DEBUG, self.app_name)
        swh_url = f'{self.target.target_url}/{target_swh[0][0]}/'
        logger(f'swh_url: {swh_url}', LOG_LEVEL_DEBUG, self.app_name)
        api_resp = requests.post(swh_url, data="{}", headers=headers)
        logger(f'{api_resp.status_code} {api_resp.text}', LOG_LEVEL_DEBUG, self.app_name)
        if api_resp.status_code == 200:
            api_resp_json = api_resp.json()
            logger(f'swh_api response json: {json.dumps(api_resp_json)}', LOG_LEVEL_DEBUG, self.app_name)
            goto_sleep = False
            counter = 0  # TODO: Refactor using Tenancy!
            while True and (counter < settings.SWH_API_MAX_RETRIES):
                counter += 1
                swh_check_url = api_resp_json.get("request_url")
                check_resp = requests.get(swh_check_url, headers=headers)
                if check_resp.status_code == 200:
                    swh_resp_json = check_resp.json()
                    logger(f'{swh_check_url} response: {json.dumps(swh_resp_json)}', LOG_LEVEL_DEBUG, self.app_name)
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
                        ideni = IdentifierItem(value=swh_resp_json.get('snapshot_swhid'), url=swh_url,
                                               protocol=IdentifierProtocol('swhid'))
                        identifier_items.append(ideni)
                        break
                    else:
                        goto_sleep = True
                if goto_sleep:
                    sleep(settings.SWH_DELAY_POLLING)

        else:
            logger(f'ERROR api_resp.status_code: {api_resp.status_code}', LOG_LEVEL_DEBUG, self.app_name)
            target_response.status_code = api_resp.status_code
            bridge_output_model.deposit_status = DepositStatus.ERROR
            target_response.error = json.dumps(api_resp.json())
            target_response.content_type = ResponseContentType.JSON
            target_response.status = DepositStatus.ERROR
            target_response.url = swh_url
            target_response.content = json.dumps(api_resp.json())
            logger(f'bridge_output_model: {bridge_output_model.model_dump(by_alias=True)}', LOG_LEVEL_DEBUG,
                   self.app_name)

        return bridge_output_model
