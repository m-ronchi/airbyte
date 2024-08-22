import requests
from airbyte_cdk import AirbyteTracedException, AvailabilityStrategy, HttpStream, SyncMode
from airbyte_cdk.sources.streams.call_rate import APIBudget
from airbyte_protocol.models import FailureType
from requests.auth import AuthBase
from abc import ABC
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional
from urllib.parse import parse_qs, urlparse


class AppStoreConnectStream(HttpStream, ABC):

    def __init__(self, app_id: str, limit: int, authenticator: AuthBase | None = None, api_budget: APIBudget | None = None):
        super().__init__(authenticator, api_budget)

        self.app_id = app_id
        self.limit = limit

    @property
    def url_base(self): return "https://api.appstoreconnect.apple.com/v1/"

    @property
    def primary_key(self): return "id"

    def stream_slices(self, *, sync_mode: SyncMode, cursor_field: List[str] | None = None, stream_state: Mapping[str, Any] | None = None) -> Iterable[Mapping[str, Any] | None]:
        return super().stream_slices(sync_mode=sync_mode, cursor_field=cursor_field, stream_state=stream_state)

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        links = response.json().get("links", {})
        if "next" in links:
            return {"next_url": links["next"]}
        else:
            return None

    def request_params(
        self, stream_state, stream_slice=None, next_page_token=None
    ) -> MutableMapping[str, Any]:
        if next_page_token:
            return parse_qs(urlparse(next_page_token["next_url"]).query)
        else:
            return {"limit": self.limit, "sort": "createdDate"}

    @property
    def availability_strategy(self) -> Optional[AvailabilityStrategy]:
        return None

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        if response.status_code in (401, 403):
            internal_message = f"Unauthorized credentials. Response: {response.json()}"
            external_message = "Can not get metadata with unauthorized credentials. Try to re-authenticate in source settings."
            raise AirbyteTracedException(
                message=external_message, internal_message=internal_message, failure_type=FailureType.config_error
            )
        yield from response.json().get("data", [])


class CustomerReviews(AppStoreConnectStream):

    def path(
        self, *, stream_state=None, stream_slice=None, next_page_token=None
    ) -> str:
        return f"apps/{self.app_id}/customerReviews"
