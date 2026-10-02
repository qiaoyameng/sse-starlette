from __future__ import annotations

from typing import Any, Awaitable, Callable, Mapping, Optional, Union

from starlette.datastructures import MutableHeaders
from starlette.requests import HTTPConnection
from starlette.responses import PlainTextResponse, Response

from sse_starlette.event_store import (
    BoundedEventStore,
    CursorStatus,
    StaleCursorError,
    UnknownCursorError,
)
from sse_starlette.sse import EventSourceResponse


class ResumableEventSourceResponse(EventSourceResponse):
    """EventSource response which validates and resumes from Last-Event-ID."""

    STALE_STATUS_CODE = 409
    UNKNOWN_STATUS_CODE = 400

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.resume_status: Optional[CursorStatus] = None
        self.resume_cursor: Optional[str] = None

    @classmethod
    def from_request(
        cls,
        store: BoundedEventStore,
        request: HTTPConnection,
        *,
        before_live: Optional[Callable[[], Awaitable[None]]] = None,
        **kwargs: Any,
    ) -> Response:
        last_event_id = request.headers.get("last-event-id")
        return cls.from_cursor(
            store,
            last_event_id=last_event_id,
            before_live=before_live,
            **kwargs,
        )

    @classmethod
    def from_cursor(
        cls,
        store: BoundedEventStore,
        *,
        last_event_id: Optional[object] = None,
        before_live: Optional[Callable[[], Awaitable[None]]] = None,
        stale_headers: Optional[Mapping[str, str]] = None,
        unknown_headers: Optional[Mapping[str, str]] = None,
        **kwargs: Any,
    ) -> Union["ResumableEventSourceResponse", PlainTextResponse]:
        error_headers = MutableHeaders()
        error_headers["Cache-Control"] = "no-store"

        try:
            subscription = store.subscribe(last_event_id, before_live=before_live)
        except StaleCursorError:
            if stale_headers:
                error_headers.update(stale_headers)
            return PlainTextResponse(
                "Last-Event-ID is older than the retained event window",
                status_code=cls.STALE_STATUS_CODE,
                headers=error_headers,
            )
        except UnknownCursorError:
            if unknown_headers:
                error_headers.update(unknown_headers)
            return PlainTextResponse(
                "Last-Event-ID is unknown",
                status_code=cls.UNKNOWN_STATUS_CODE,
                headers=error_headers,
            )

        response = cls(subscription, **kwargs)
        response.resume_status = subscription.status
        response.resume_cursor = subscription.cursor
        response.cursor_status = subscription.status
        response.last_event_id = subscription.cursor
        return response

    @classmethod
    def from_store(
        cls,
        store: BoundedEventStore,
        request: HTTPConnection,
        *,
        before_live: Optional[Callable[[], Awaitable[None]]] = None,
        **kwargs: Any,
    ) -> Response:
        return cls.from_request(store, request, before_live=before_live, **kwargs)
