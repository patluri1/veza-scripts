import requests
import urllib
import logging
from oaaclient.client import (
    OAAClient,
    OAAResponseError,
    OAAClientError,
    OAAConnectionError,
)

log = logging.getLogger(__name__)

class VezaHTTPExtender(OAAClient):

    def __init__(self, url = None, api_key = None, username = None, token = None):
        super().__init__(url, api_key, username, token)

    def _perform_request(
        self, method: str, api_path: str, data: dict = None, params: dict = None
    ) -> dict:
        """Perform HTTP request

        Performs an HTTP request of the specified method to the Veza tenant

        Args:
            method (str): HTTP method, GET, POST, DELETE
            api_path (str): API path relative to Veza host URL, e.g. `/api/v1/providers`
            data (dict, optional): For POST operation data to send. Defaults to None.

        Raises:
            OAAClientError: For errors connecting to or returned by the Veza tenant

        Returns:
            dict: Veza API JSON response as dictionary
        """

        response = None

        headers = {}
        
        if self.api_key.startswith("k") and self.api_key[1].isdigit():
            headers["authorization"] = f"Bearer {self.api_key}"
        else:
            headers["cookie"] = f"token={self.api_key}"
        headers["user-agent"] = self._user_agent
        api_timeout = 300
        api_path = api_path.lstrip("/")

        if params:
            params_str = urllib.parse.urlencode(params)
        else:
            params_str = None

        try:
            response = self._http_adapter.request(
                method,
                f"{self.url}/{api_path}",
                headers=headers,
                timeout=api_timeout,
                params=params_str,
                json=data,
                verify=self.verify_ssl,
            )
            response.raise_for_status()
            # load API response
            result = response.json()
        except requests.exceptions.HTTPError as e:
            # HTTP request completed but returned an error
            # decode expected error message parts
            details = []
            timestamp = None
            request_id = None
            try:
                result = response.json()
                message = result.get(
                    "message", f"Unknown error during {method.upper()}"
                )
                code = result.get("code", "UNKNOWN")
                timestamp = result.get("timestamp", None)
                request_id = result.get("request_id", None)
                details = result.get("details", [])
            except requests.exceptions.JSONDecodeError:
                # response is not a valid JSON, unexpected
                result = {}
                code = "ERROR"
                if response.reason:
                    message = f"Error reason: {response.reason}"
                else:
                    message = "Unknown error, response is not JSON"

            log.debug(
                f"Error returned by Veza API: {e.response.status_code} {message} {e.response.url} request_id: {request_id} timestamp {timestamp}"
            )
            for d in details:
                log.debug(d)

            raise OAAResponseError(
                code,
                message,
                status_code=e.response.status_code,
                details=details,
                timestamp=timestamp,
                request_id=request_id,
            )
        except requests.exceptions.JSONDecodeError as e:
            # HTTP response reports success but response does not decode to JSON
            if response:
                status_code = response.status_code
            else:
                status_code = None
            raise OAAClientError("ERROR", "Response not JSON", status_code=status_code)
        except requests.exceptions.RequestException as e:
            if not e.response:
                raise OAAConnectionError("ERROR", message=str(e))
            else:
                raise OAAConnectionError(
                    "ERROR", message=str(e), status_code=e.response.status_code
                )

        return result