from __future__ import annotations

from typing import List, Optional

from pydantic import BaseModel, Field


class Credentials(BaseModel):
    password: Optional[str] = None
    username: Optional[str] = None


class TargetsCredential(BaseModel):
    target_repo_name: str = Field(..., alias='target-repo-name')
    credentials: Optional[Credentials] = None


class TargetsCredentialsModel(BaseModel):
    targets_credentials: List[TargetsCredential] = Field(
        ..., alias='targets-credentials'
    )

json_data={"targets-credentials":
[
    {
        "target-repo-name": "dans.sword.ssh.local",
        "credentials": {"password": "01bd92cf-de58-4389-942d-ebaec52fc073"}
    },
    {
        "target-repo-name": "dans.sword.ssh.local",
        "credentials": {
            "username": "eko",
            "password": "01bd92cf-de58-4389-942d-ebaec52fc073"
        }
    },
    {
        "target-repo-name": "dans.sword.ssh.local",
        "credentials": {
            "username": "eko"
        }
    },
    {
        "target-repo-name": "dans.sword.ssh.local"
    }
]
}
# y = TargetsCredentialsModel(**json_data)
# for x in y.targets_credentials:
#     print(x.target_repo_name)