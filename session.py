import requests
from abc import ABC
from typing import Any, Dict, Optional


API_URL = "https://api.tastyworks.com"
CERT_URL = "https://api.cert.tastyworks.com"


class TastytradeError(Exception):
    pass


def validate_response(response: requests.Response) -> None:
    if response.status_code // 100 != 2:
        content = response.json()["error"]
        error_message = f"{content['code']}: {content['message']}"
        errors = content.get("errors")
        if errors is not None:
            for error in errors:
                if "code" in error:
                    error_message += f"\n{error['code']}: {error['message']}"
                else:
                    error_message += f"\n{error['domain']}: {error['reason']}"

        raise TastytradeError(error_message)


class Session(ABC):
    base_url: str
    headers: Dict[str, str]
    user: Dict[str, str]
    session_token: str

    def validate(self) -> bool:
        response = requests.post(
            f"{self.base_url}/sessions/validate", headers=self.headers
        )
        return response.status_code // 100 == 2

    def destroy(self) -> bool:
        response = requests.delete(f"{self.base_url}/sessions", headers=self.headers)
        return response.status_code // 100 == 2

    def get_customer(self) -> Dict[str, Any]:
        response = requests.get(f"{self.base_url}/customers/me", headers=self.headers)
        validate_response(response)
        return response.json()["data"]


class ProductionSession(Session):
    def __init__(
        self,
        login: str,
        password: Optional[str] = None,
        remember_me: bool = False,
        remember_token: Optional[str] = None,
        two_factor_authentication: Optional[str] = None,
    ):
        body = {"login": login, "remember-me": remember_me}
        if password is not None:
            body["password"] = password
        elif remember_token is not None:
            body["remember-token"] = remember_token
        else:
            raise TastytradeError(
                "You must provide a password or remember " "token to log in."
            )
        #: The base url to use for API requests
        self.base_url: str = API_URL

        if two_factor_authentication is not None:
            headers = {"X-Tastyworks-OTP": two_factor_authentication}
            response = requests.post(
                f"{self.base_url}/sessions", json=body, headers=headers
            )
        else:
            response = requests.post(f"{self.base_url}/sessions", json=body)
        validate_response(response)  # throws exception if not 200

        json = response.json()
        #: The user dict returned by the API; contains basic user information
        self.user: Dict[str, str] = json["data"]["user"]
        #: The session token used to authenticate requests
        self.session_token: str = json["data"]["session-token"]
        #: A single-use token which can be used to login without a password
        self.remember_token: Optional[str] = (
            json["data"]["remember-token"] if remember_me else None
        )
        #: The headers to use for API requests
        self.headers: Dict[str, str] = {"Authorization": self.session_token}
        self.validate()

        #: Pull streamer tokens and urls
        response = requests.get(
            f"{self.base_url}/quote-streamer-tokens", headers=self.headers
        )
        validate_response(response)
        data = response.json()["data"]
        self.streamer_token = data["token"]
        url = data["websocket-url"] + "/cometd"
        self.dxfeed_url = url.replace("https", "wss")
        self.dxlink_url = data["dxlink-url"]
        self.rest_url = data["websocket-url"] + "/rest/events.json"
        self.streamer_headers = {"Authorization": f"Bearer {self.streamer_token}"}