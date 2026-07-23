"""
Minimal Unipile client for LinkedIn relations (connections).

Only what the connector needs: cursor-paginate the connected account's
relations. See https://developer.unipile.com/ .
"""
import logging
import urllib.parse
import urllib.request
import urllib.error
import json
from datetime import datetime, timezone
from typing import Iterator


class UnipileClient:
    def __init__(self, dsn: str, api_key: str, page_size: int = 100):
        # The DSN may carry a non-standard port (e.g. api43.unipile.com:17362).
        # Many managed networks (like the Koalake worker) only allow outbound on
        # 443 and silently drop other ports. Unipile supports staying on 443 and
        # passing the real port as a ?port= query param, so split it off here.
        host, _, port = dsn.partition(":")
        self.host = host
        self.port = port or None
        self.api_key = api_key
        self.page_size = page_size

    def _get(self, path: str, params: dict) -> dict:
        params = dict(params)
        if self.port:
            params.setdefault("port", self.port)
        url = f"https://{self.host}{path}?{urllib.parse.urlencode(params)}"
        req = urllib.request.Request(
            url, headers={"X-API-KEY": self.api_key, "accept": "application/json"}
        )
        try:
            with urllib.request.urlopen(req, timeout=60) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            body = exc.read().decode("utf-8", "replace")
            raise RuntimeError(f"Unipile API error {exc.code} on {path}: {body}") from exc

    def iter_relations(self, account_id: str) -> Iterator[dict]:
        """Yield every relation (first-degree connection) for the account."""
        cursor = None
        fetched = 0
        while True:
            params = {"account_id": account_id, "limit": self.page_size}
            if cursor:
                params["cursor"] = cursor
            payload = self._get("/api/v1/users/relations", params)
            items = payload.get("items", [])
            for item in items:
                yield item
            fetched += len(items)
            cursor = payload.get("cursor")
            if not cursor or not items:
                break
        logging.info(f"Unipile: fetched {fetched} relation(s) for account {account_id}")


def relation_to_row(r: dict, ingested_at: str) -> dict:
    """Map a Unipile relation to our connections-table row."""
    ms = r.get("created_at")
    connected_at = (
        datetime.fromtimestamp(ms / 1000, tz=timezone.utc).replace(tzinfo=None)
        if isinstance(ms, (int, float))
        else None
    )
    return {
        "member_id": r.get("member_id"),
        "first_name": r.get("first_name"),
        "last_name": r.get("last_name"),
        "headline": r.get("headline"),
        "public_identifier": r.get("public_identifier"),
        "public_profile_url": r.get("public_profile_url"),
        "profile_picture_url": r.get("profile_picture_url"),
        "connection_urn": r.get("connection_urn"),
        "connected_at": connected_at,
        "ingested_at": ingested_at,
    }