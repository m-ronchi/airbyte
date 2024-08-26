
from dataclasses import dataclass
import datetime
import time
from typing import Any, List, Mapping, Optional, Tuple

import jwt
from airbyte_cdk import AbstractSource, Stream, AbstractHeaderAuthenticator
from airbyte_protocol.models import SyncMode, AirbyteMessage
from .streams import Apps, CustomerReviews


@dataclass
class JwtAuthenticator(AbstractHeaderAuthenticator):

    key_id: str
    issuer_id: str
    private_key: str

    @property
    def auth_header(self):
        return "Authorization"

    @property
    def token(self):
        return jwt.encode(
            headers={
                "typ": "JWT",
                "alg": "ES256",
                "kid": self.key_id,
            },
            payload={
                "iat": int(time.time()),
                "exp": int(time.time()) + (2 * 60),
                "iss": self.issuer_id,
                "aud": "appstoreconnect-v1",
            },
            key=self.private_key
        )


class SourceAppStoreConnect(AbstractSource):
    def check_connection(self, logger, config) -> Tuple[bool, Any]:
        first_stream = next(iter(self.streams(config)))

        stream_slice = next(
            iter(first_stream.stream_slices(sync_mode=SyncMode.full_refresh)))

        try:
            read_stream = first_stream.read_records(
                sync_mode=SyncMode.full_refresh, stream_slice=stream_slice)
            first_record = None
            while not first_record:
                first_record = next(iter(read_stream))
                if isinstance(first_record, AirbyteMessage):
                    if first_record.type == "RECORD":
                        first_record = first_record.record
                        return True, None
                    else:
                        first_record = None
            return True, None
        except Exception as e:
            return False, f"Unable to connect to the API with the provided credentials - {str(e)}"

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        auth = JwtAuthenticator(
            key_id=config["auth_key_id"],
            issuer_id=config["auth_issuer_id"],
            private_key=str(config["auth_private_key"]).replace("\\n", "\n"))
        apps = Apps(limit=config["limit"], authenticator=auth)
        customer_reviews = CustomerReviews(
            parent=apps,
            limit=config["limit"],
            authenticator=auth
        )
        return [apps, customer_reviews]
