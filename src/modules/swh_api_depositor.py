from __future__ import annotations

from time import sleep

import json

import requests

from src.bridge import Bridge
from src.models.bridge_output_model import BridgeOutputDataModel, TargetResponse, ResponseContentType
from src.commons import logger, settings
from src.dbz import DepositStatus


class SwhApiDepositor(Bridge):

    def deposit(self) -> BridgeOutputDataModel:
        target_resp = TargetResponse()
        bridge_output_model = BridgeOutputDataModel(response= target_resp)
        headers = {'Content-Type': 'application/json', 'Authorization': f'Bearer {settings.SWH_ACCESS_TOKEN}'}
        api_resp = requests.post(self.target.target_url, data="{}", headers=headers)
        if api_resp.status_code == 200:
            api_resp_json = api_resp.json()
            logger(f'swh_api response json: {json.dumps(api_resp_json)}', 'debug', self.app_name)
            goto_sleep = False
            counter = 0 # TODO: Refactor using Tenancy!
            while True or counter > settings.SWH_API_MAX_RETRIES:
                counter += 1
                swh_check_url = api_resp_json.get("request_url")
                check_resp = requests.get(swh_check_url, headers=headers)
                if check_resp.status_code == 200:
                    swh_resp_json = swh_check_url.json()
                    logger(f'{swh_check_url} response: {json.dumps(swh_resp_json)}', 'debug', self.app_name)
                    if swh_resp_json.get('save_task_status') == DepositStatus.FAILED:
                        bridge_output_model.deposit_status = DepositStatus.FAILED
                        logger(f"save_task_status is failed.", 'error', self.app_name)
                        break
                    elif swh_resp_json.get('snapshot_swhid'):
                        bridge_output_model.deposit_status = DepositStatus.FINISH
                        target_resp.status_code = check_resp.status_code
                        target_resp.content_type = ResponseContentType.JSON
                        target_resp.content = json.dumps(swh_resp_json)
                        target_resp.status = DepositStatus.SUCCESS
                        break
                    else:
                        goto_sleep = True
                if goto_sleep:
                    sleep(settings.SWH_DELAY_POOLING)

        else:
            target_resp.status_code = api_resp.status_code
            bridge_output_model.deposit_status = DepositStatus.ERROR
            target_resp.status_code = api_resp.status_code

        return bridge_output_model
