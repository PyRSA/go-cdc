"""Minimal Elasticsearch 8 HTTP client for the integration harness (stdlib only).

The harness must not depend on the Go sink to *verify* the sink: every check in
this module talks to the cluster over plain HTTP, so a bug in the production
client cannot hide itself.

Deep pagination uses the scroll API (`size` pages of 1000). `from`/`size` is
never used past the 10k window, and `_count` is never treated as proof of
content — callers that care compare full documents.
"""
from __future__ import annotations

import base64
import json
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Iterable, Iterator

DEFAULT_PAGE_SIZE = 1000


class ESError(RuntimeError):
    """Non-2xx response or transport failure, with the request that caused it."""

    def __init__(self, method: str, path: str, status: int, body: str, url: str = ""):
        self.method = method
        self.path = path
        self.status = status
        self.body = body
        self.url = url
        suffix = f" ({url})" if url else ""
        super().__init__(f"{method} {path}{suffix} -> {status}: {_clip(body)}")


def _clip(text: str, limit: int = 600) -> str:
    text = text.replace("\n", " ")
    return text if len(text) <= limit else text[:limit] + "..."


@dataclass
class Doc:
    id: str
    index: str
    source: dict


class ESClient:
    def __init__(self, base_url: str, *, timeout: float = 30.0, username: str = "", password: str = ""):
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.username = username
        self.password = password

    # ---------------------------------------------------------------- transport
    def _auth_header(self) -> dict:
        if not self.username:
            return {}
        raw = f"{self.username}:{self.password}".encode("utf-8")
        return {"Authorization": "Basic " + base64.b64encode(raw).decode("ascii")}

    def request(
        self,
        method: str,
        path: str,
        body: object | None = None,
        *,
        ndjson: bool = False,
        ok: tuple[int, ...] = (200, 201),
        allow: tuple[int, ...] = (),
        timeout: float | None = None,
    ) -> tuple[int, object]:
        """Send one request. Returns (status, decoded body) for ok/allow statuses."""
        url = f"{self.base_url}/{path.lstrip('/')}"
        data = None
        headers = {"Accept": "application/json", **self._auth_header()}
        if body is not None:
            if ndjson:
                payload = body if isinstance(body, str) else "\n".join(
                    json.dumps(item, separators=(",", ":")) for item in body  # type: ignore[union-attr]
                )
                data = (payload + "\n").encode("utf-8")
                headers["Content-Type"] = "application/x-ndjson"
            else:
                data = json.dumps(body).encode("utf-8")
                headers["Content-Type"] = "application/json"
        req = urllib.request.Request(url, data=data, method=method, headers=headers)
        try:
            with urllib.request.urlopen(req, timeout=timeout or self.timeout) as resp:
                status = resp.status
                raw = resp.read()
        except urllib.error.HTTPError as exc:
            status = exc.code
            raw = exc.read() or b""
            if status in allow:
                return status, _decode(raw)
            raise ESError(method, path, status, raw.decode("utf-8", "replace"), url) from None
        except urllib.error.URLError as exc:
            raise ESError(method, path, 0, str(exc), url) from None
        if status in allow or status in ok:
            return status, _decode(raw)
        raise ESError(method, path, status, raw.decode("utf-8", "replace"), url)

    # ------------------------------------------------------------------ cluster
    def ping(self) -> bool:
        try:
            self.request("GET", "/")
            return True
        except ESError:
            return False

    def cluster_health(self, wait_for_status: str = "", timeout: str = "10s") -> dict:
        path = "_cluster/health"
        if wait_for_status:
            path += f"?wait_for_status={wait_for_status}&timeout={timeout}"
        _, body = self.request("GET", path, allow=(408,))
        return body if isinstance(body, dict) else {}

    def version(self) -> str:
        _, body = self.request("GET", "/")
        return str((body or {}).get("version", {}).get("number", "")) if isinstance(body, dict) else ""

    # ------------------------------------------------------------------ indices
    def index_exists(self, index: str) -> bool:
        status, _ = self.request("HEAD", index, ok=(), allow=(200, 404))
        return status == 200

    def create_index(self, index: str, body: dict | None = None) -> None:
        self.request("PUT", index, body or {}, ok=(200,))

    def delete_index(self, index: str, *, ignore_missing: bool = True) -> bool:
        allow = (404,) if ignore_missing else ()
        status, _ = self.request("DELETE", index, ok=(200,), allow=allow)
        return status == 200

    def list_indices(self, pattern: str = "*") -> list[str]:
        status, body = self.request(
            "GET",
            f"_cat/indices/{urllib.parse.quote(pattern, safe='*,')}?h=index&format=json",
            ok=(200,),
            allow=(404,),
        )
        if status == 404 or not isinstance(body, list):
            return []
        return sorted({str(row.get("index", "")) for row in body if row.get("index")})

    def delete_indices(self, pattern: str) -> list[str]:
        """Delete every index matching pattern one by one (destructive_requires_name)."""
        removed = []
        for name in self.list_indices(pattern):
            if name.startswith("."):
                continue  # never touch system indices
            if self.delete_index(name):
                removed.append(name)
        return removed

    def get_mapping(self, index: str) -> dict:
        """Return {"properties": {...}, "dynamic": ...} for one index."""
        status, body = self.request("GET", f"{index}/_mapping", ok=(200,), allow=(404,))
        if status == 404 or not isinstance(body, dict):
            return {}
        entry = body.get(index) or next(iter(body.values()), {})
        return ((entry or {}).get("mappings") or {}) if isinstance(entry, dict) else {}

    def refresh(self, index: str = "") -> None:
        path = f"{index}/_refresh" if index else "_refresh"
        self.request("POST", path, ok=(200,), allow=(404,))

    # ------------------------------------------------------------------- counts
    def count(self, index: str, query: dict | None = None) -> int:
        body = {"query": query or {"match_all": {}}}
        status, resp = self.request("GET", f"{index}/_count", body, ok=(200,), allow=(404,))
        if status == 404 or not isinstance(resp, dict):
            return 0
        return int(resp.get("count", 0))

    # ------------------------------------------------------------------ reading
    def get_doc(self, index: str, doc_id: str) -> dict | None:
        path = f"{index}/_doc/{urllib.parse.quote(doc_id, safe='')}"
        status, body = self.request("GET", path, ok=(200,), allow=(404,))
        if status == 404 or not isinstance(body, dict) or not body.get("found"):
            return None
        return body.get("_source") or {}

    def search(self, index: str, body: dict) -> dict:
        _, resp = self.request("POST", f"{index}/_search", body, ok=(200,))
        return resp if isinstance(resp, dict) else {}

    def ids_matching(self, index: str, query: dict, *, page_size: int = DEFAULT_PAGE_SIZE) -> list[str]:
        return [doc.id for doc in self.scan(index, query=query, page_size=page_size)]

    def scan(self, index: str, *, query: dict | None = None, page_size: int = DEFAULT_PAGE_SIZE) -> Iterator[Doc]:
        """Yield every matching doc via scroll (no 10k window)."""
        scroll = "2m"
        body = {
            "size": page_size,
            "sort": ["_doc"],
            "query": query or {"match_all": {}},
        }
        _, resp = self.request("POST", f"{index}/_search?scroll={scroll}", body, ok=(200,))
        scroll_id = ""
        try:
            while True:
                resp = resp if isinstance(resp, dict) else {}
                hits = ((resp.get("hits") or {}).get("hits") or [])
                scroll_id = str(resp.get("_scroll_id") or scroll_id)
                if not hits:
                    return
                for hit in hits:
                    yield Doc(id=str(hit.get("_id")), index=str(hit.get("_index")), source=hit.get("_source") or {})
                if not scroll_id:
                    return
                _, resp = self.request(
                    "POST", "_search/scroll", {"scroll": scroll, "scroll_id": scroll_id}, ok=(200,), allow=(404,)
                )
        finally:
            if scroll_id:
                try:
                    self.request("DELETE", "_search/scroll", {"scroll_id": [scroll_id]}, ok=(200,), allow=(404, 400))
                except ESError:
                    pass

    def scan_all(self, index: str, *, page_size: int = DEFAULT_PAGE_SIZE) -> dict[str, dict]:
        """Return {_id: _source} for the whole index (memory-bound: used up to ~300k docs)."""
        out: dict[str, dict] = {}
        for doc in self.scan(index, page_size=page_size):
            out[doc.id] = doc.source
        return out

    def get_docs(self, index: str, ids: Iterable[str], *, chunk: int = 500) -> dict[str, dict]:
        """Multi-get in chunks; ids use the deterministic sink _id (sha256 hex)."""
        out: dict[str, dict] = {}
        pending = [str(i) for i in ids]
        for start in range(0, len(pending), chunk):
            part = pending[start : start + chunk]
            body = {"ids": part}
            _, resp = self.request("GET", f"{index}/_mget", body, ok=(200,), allow=(404,))
            docs = (resp or {}).get("docs") or [] if isinstance(resp, dict) else []
            for doc in docs:
                if doc.get("found"):
                    out[str(doc.get("_id"))] = doc.get("_source") or {}
        return out


def _decode(raw: bytes):
    if not raw:
        return None
    try:
        return json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return raw.decode("utf-8", "replace")
