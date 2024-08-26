import json
import requests
from airbyte_cdk import AirbyteMessage, AirbyteTracedException, AvailabilityStrategy, HttpStream, HttpSubStream, Record, StreamSlice, SyncMode
from airbyte_cdk.sources.streams.call_rate import APIBudget
from airbyte_protocol.models import FailureType, Type as MessageType
from requests.auth import AuthBase
from abc import ABC
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional
from urllib.parse import parse_qs, urlparse


class AppStoreConnectStream(HttpStream, ABC):

    extra_query_args: Mapping[str, str] = {}

    def __init__(self, limit: int, authenticator: Optional[AuthBase] = None, api_budget: Optional[APIBudget] = None):
        super().__init__(authenticator, api_budget)

        self.limit = limit

    @property
    def url_base(self): return "https://api.appstoreconnect.apple.com/v1/"

    @property
    def primary_key(self): return "id"

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
            return {"limit": self.limit, **self.extra_query_args}

    @property
    def availability_strategy(self) -> Optional[AvailabilityStrategy]:
        return None

    def parse_response(self, response: requests.Response, *, stream_slice=None, **kwargs) -> Iterable[Mapping]:
        if response.status_code in (401, 403):
            internal_message = f"Unauthorized credentials. Response: {response.json()}"
            external_message = "Can not get metadata with unauthorized credentials. Try to re-authenticate in source settings."
            raise AirbyteTracedException(
                message=external_message, internal_message=internal_message, failure_type=FailureType.config_error
            )
        for record in response.json().get("data", []):
            parent = {
                "app_id": stream_slice.get("id")
            } if stream_slice else {}
            yield {"id": record["id"], **record["attributes"], **parent}


class Apps(AppStoreConnectStream):
    def path(self, *, stream_state=None, stream_slice=None, next_page_token=None) -> str:
        return "apps"

    @property
    def use_cache(self) -> bool:
        return True


class CustomerReviews(AppStoreConnectStream):

    extra_query_args = {"sort": "createdDate"}

    def __init__(self, limit: int, parent: Apps, authenticator: Optional[AuthBase] = None, api_budget: Optional[APIBudget] = None):
        super().__init__(limit, authenticator, api_budget)

        self.parent = parent

    def path(
        self, *, stream_state=None, stream_slice=None, next_page_token=None
    ) -> str:
        assert stream_slice
        return f"apps/{stream_slice['id']}/customerReviews"

    def stream_slices(self, *, sync_mode, cursor_field=None, stream_state=None) -> Iterable[Mapping[str, Any]]:
        # read_stateless() assumes the parent is not concurrent. This is currently okay since the concurrent CDK does
        # not support either substreams or RFR, but something that needs to be considered once we do
        for parent_record in self.parent.read_only_records(stream_state):
            # Skip non-records (eg AirbyteLogMessage)
            if isinstance(parent_record, AirbyteMessage):
                if parent_record.type == MessageType.RECORD:
                    assert parent_record.record
                    parent_record = parent_record.record.data
                else:
                    continue
            elif isinstance(parent_record, Record):
                parent_record = parent_record.data

            yield StreamSlice(partition=parent_record, cursor_slice={})
