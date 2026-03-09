from __future__ import annotations

import hashlib
import hmac
import json
import logging
import os
import queue
import re
import threading
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from typing import Literal
from urllib import parse, request

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from .db import SQLiteStore
from .live import rank_group
from .main import load_settings
from .models import AthleteRow
from .streaming import StreamRunConfig, event_to_json, run_stream


logger = logging.getLogger("online_results.api")
if not logger.handlers:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

GOOGLE_ID_PATTERN = re.compile(r"/d/([a-zA-Z0-9_-]{20,})")
PLAIN_ID_PATTERN = re.compile(r"^[a-zA-Z0-9_-]{20,}$")


class StartStreamRequest(BaseModel):
    protocol_link: str = Field(..., description="Ссылка Google Drive/Sheets или ID файла")
    poll_interval_sec: float = Field(default=1.0, ge=0.2, le=60.0)
    refresh_titles_every: int = Field(default=120, ge=1, le=3600)
    finalize_timeout_sec: int = Field(default=300, ge=0, le=86400)
    finalize_max_missing: int = Field(default=2, ge=0, le=100)
    callback_url: str = Field(default="", description="Webhook URL внешней системы")
    callback_secret: str = Field(default="", description="Секрет для HMAC подписи webhook")
    worker_print: bool = Field(default=False, description="Печатать консольный вывод внутри воркера")


class StreamStateResponse(BaseModel):
    stream_id: str
    status: Literal["running", "completed", "failed", "stopped"]
    spreadsheet_id: str
    started_at: str
    finished_at: str = ""
    error: str = ""
    event_count: int
    last_event_at: str = ""
    callback_url: str = ""


class StartStreamResponse(BaseModel):
    stream_id: str
    status: str


class ResetStreamStateResponse(BaseModel):
    stream_id: str
    status: str


class EventItemResponse(BaseModel):
    event_date: str
    event_name: str
    participants: int
    groups: int
    updated_at: str


class GroupAthleteResultResponse(BaseModel):
    place: int
    athlete_key: str
    start_number: int
    full_name: str
    club: str
    run1: str
    run2: str
    total: str
    status: str
    judge_note: str


class GroupFinalResponse(BaseModel):
    event_date: str
    event_name: str
    sheet_name: str
    group_name: str
    athletes: list[GroupAthleteResultResponse]


class AthletePlacesRowResponse(BaseModel):
    athlete_name: str
    by_date: dict[str, str]


@dataclass
class StreamRuntime:
    stream_id: str
    spreadsheet_id: str
    status: Literal["running", "completed", "failed", "stopped"]
    started_at: str
    callback_url: str = ""
    finished_at: str = ""
    error: str = ""
    event_count: int = 0
    last_event_at: str = ""
    thread: threading.Thread | None = None
    stop_event: threading.Event = field(default_factory=threading.Event)


class StreamManager:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._streams: dict[str, StreamRuntime] = {}
        self._subscribers: dict[str, dict[str, queue.Queue[dict[str, object]]]] = {}

    def start(self, req: StartStreamRequest) -> StreamRuntime:
        settings = load_settings(require_source=False)
        if not settings.service_account_file:
            raise RuntimeError("Не задан GOOGLE_SERVICE_ACCOUNT_FILE")
        spreadsheet_id = _extract_google_file_id(req.protocol_link)
        with self._lock:
            for runtime in self._streams.values():
                if runtime.spreadsheet_id == spreadsheet_id and runtime.status == "running":
                    return runtime
        stream_id = uuid.uuid4().hex
        runtime = StreamRuntime(
            stream_id=stream_id,
            spreadsheet_id=spreadsheet_id,
            status="running",
            started_at=datetime.now().isoformat(),
            callback_url=req.callback_url.strip(),
        )
        config = StreamRunConfig(
            spreadsheet_id=spreadsheet_id,
            service_account_file=settings.service_account_file,
            sqlite_db=settings.sqlite_db,
            poll_interval_sec=req.poll_interval_sec,
            refresh_titles_every=req.refresh_titles_every,
            finalize_timeout_sec=req.finalize_timeout_sec,
            finalize_max_missing=req.finalize_max_missing,
            stop_on_completion=True,
            console_output=req.worker_print,
        )

        def sink(event_type: str, payload: dict[str, object]) -> None:
            self._on_event(runtime.stream_id, event_type, payload, req.callback_url.strip(), req.callback_secret.strip())

        def target() -> None:
            try:
                run_stream(config=config, sink=sink, stop_event=runtime.stop_event)
                with self._lock:
                    current = self._streams.get(runtime.stream_id)
                    if current and current.status == "running":
                        current.status = "completed"
                        current.finished_at = datetime.now().isoformat()
            except Exception as exc:
                with self._lock:
                    current = self._streams.get(runtime.stream_id)
                    if current:
                        current.status = "failed"
                        current.error = str(exc)
                        current.finished_at = datetime.now().isoformat()
                logger.exception("stream_failed stream_id=%s", runtime.stream_id)

        thread = threading.Thread(target=target, name=f"stream-{stream_id}", daemon=True)
        runtime.thread = thread
        with self._lock:
            self._streams[stream_id] = runtime
        thread.start()
        return runtime

    def stop(self, stream_id: str) -> StreamRuntime:
        with self._lock:
            runtime = self._streams.get(stream_id)
            if runtime is None:
                raise KeyError(stream_id)
            runtime.stop_event.set()
            if runtime.status == "running":
                runtime.status = "stopped"
                runtime.finished_at = datetime.now().isoformat()
            return runtime

    def reset_state(self, stream_id: str) -> StreamRuntime:
        with self._lock:
            runtime = self._streams.get(stream_id)
            if runtime is None:
                raise KeyError(stream_id)
            runtime.stop_event.set()
            if runtime.status == "running":
                runtime.status = "stopped"
                runtime.finished_at = datetime.now().isoformat()
            thread = runtime.thread

        if thread is not None and thread.is_alive():
            thread.join(timeout=10.0)
            if thread.is_alive():
                raise RuntimeError("stream thread did not stop in time")

        settings = load_settings(require_source=False)
        _clear_sqlite_state(settings.sqlite_db)
        with self._lock:
            self._subscribers.pop(stream_id, None)
        return runtime

    def get(self, stream_id: str) -> StreamRuntime:
        with self._lock:
            runtime = self._streams.get(stream_id)
            if runtime is None:
                raise KeyError(stream_id)
            return runtime

    def list(self) -> list[StreamRuntime]:
        with self._lock:
            return list(self._streams.values())

    def subscribe(self, stream_id: str) -> tuple[str, queue.Queue[dict[str, object]]]:
        with self._lock:
            if stream_id not in self._streams:
                raise KeyError(stream_id)
            sub_id = uuid.uuid4().hex
            q: queue.Queue[dict[str, object]] = queue.Queue(maxsize=1000)
            self._subscribers.setdefault(stream_id, {})[sub_id] = q
            return sub_id, q

    def unsubscribe(self, stream_id: str, sub_id: str) -> None:
        with self._lock:
            by_stream = self._subscribers.get(stream_id)
            if not by_stream:
                return
            by_stream.pop(sub_id, None)
            if not by_stream:
                self._subscribers.pop(stream_id, None)

    def _on_event(
        self,
        stream_id: str,
        event_type: str,
        payload: dict[str, object],
        callback_url: str,
        callback_secret: str,
    ) -> None:
        event_json = event_to_json(stream_id=stream_id, event_type=event_type, payload=payload)
        logger.info(event_json)
        envelope: dict[str, object] = json.loads(event_json)
        with self._lock:
            runtime = self._streams.get(stream_id)
            if runtime:
                runtime.event_count += 1
                runtime.last_event_at = datetime.now().isoformat()
                if event_type == "stream_completed" and runtime.status == "running":
                    runtime.status = "completed"
                    runtime.finished_at = datetime.now().isoformat()
                if event_type == "stream_stopped":
                    runtime.status = "stopped"
                    runtime.finished_at = datetime.now().isoformat()
                if event_type == "stream_error":
                    runtime.status = "failed"
                    runtime.error = str(payload.get("error", "unknown error"))
                    runtime.finished_at = datetime.now().isoformat()
            subscribers = list(self._subscribers.get(stream_id, {}).values())
        for q in subscribers:
            try:
                q.put_nowait(envelope)
            except queue.Full:
                # Drop oldest and keep stream alive for slow consumers.
                try:
                    q.get_nowait()
                    q.put_nowait(envelope)
                except queue.Empty:
                    pass
        if callback_url:
            _post_webhook(callback_url=callback_url, callback_secret=callback_secret, body=event_json)


def _post_webhook(callback_url: str, callback_secret: str, body: str) -> None:
    data = body.encode("utf-8")
    headers = {"Content-Type": "application/json; charset=utf-8"}
    if callback_secret:
        digest = hmac.new(callback_secret.encode("utf-8"), data, hashlib.sha256).hexdigest()
        headers["X-Online-Results-Signature"] = f"sha256={digest}"
    req = request.Request(callback_url, data=data, headers=headers, method="POST")
    try:
        with request.urlopen(req, timeout=5) as resp:
            if resp.status >= 300:
                logger.warning("webhook_non_success status=%s url=%s", resp.status, callback_url)
    except Exception:
        logger.exception("webhook_failed url=%s", callback_url)


def _extract_google_file_id(link_or_id: str) -> str:
    raw = link_or_id.strip()
    if not raw:
        raise RuntimeError("Пустая ссылка/ID протокола")
    if PLAIN_ID_PATTERN.match(raw):
        return raw
    match = GOOGLE_ID_PATTERN.search(raw)
    if match:
        return match.group(1)
    parsed_url = parse.urlparse(raw)
    query = parse.parse_qs(parsed_url.query)
    query_id = (query.get("id") or [""])[0]
    if query_id and PLAIN_ID_PATTERN.match(query_id):
        return query_id
    raise RuntimeError("Не удалось извлечь ID Google файла из ссылки")


load_dotenv()
app = FastAPI(title="Online Results API", version="1.0.0")
manager = StreamManager()


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/v1/streams", response_model=StartStreamResponse)
def start_stream(req: StartStreamRequest) -> StartStreamResponse:
    try:
        runtime = manager.start(req)
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return StartStreamResponse(stream_id=runtime.stream_id, status=runtime.status)


@app.get("/v1/streams", response_model=list[StreamStateResponse])
def list_streams() -> list[StreamStateResponse]:
    return [_to_response(runtime) for runtime in manager.list()]


@app.get("/v1/streams/{stream_id}", response_model=StreamStateResponse)
def get_stream(stream_id: str) -> StreamStateResponse:
    try:
        runtime = manager.get(stream_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="stream not found") from exc
    return _to_response(runtime)


@app.post("/v1/streams/{stream_id}/stop", response_model=StreamStateResponse)
def stop_stream(stream_id: str) -> StreamStateResponse:
    try:
        runtime = manager.stop(stream_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="stream not found") from exc
    return _to_response(runtime)


@app.post("/v1/streams/{stream_id}/reset-state", response_model=ResetStreamStateResponse)
def reset_stream_state(stream_id: str) -> ResetStreamStateResponse:
    try:
        runtime = manager.reset_state(stream_id)
    except KeyError:
        settings = load_settings(require_source=False)
        _clear_sqlite_state(settings.sqlite_db)
        return ResetStreamStateResponse(stream_id=stream_id, status="reset")
    except RuntimeError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    return ResetStreamStateResponse(stream_id=runtime.stream_id, status="reset")


@app.get("/v1/results/events", response_model=list[EventItemResponse])
def list_events() -> list[EventItemResponse]:
    settings = load_settings(require_source=False)
    store = SQLiteStore(settings.sqlite_db)
    try:
        store.init_schema()
        events = store.list_events()
    finally:
        store.close()
    return [EventItemResponse(**item) for item in events]


@app.get("/v1/results/final", response_model=list[GroupFinalResponse])
def get_final_results(event_date: str = "", event_name: str = "") -> list[GroupFinalResponse]:
    settings = load_settings(require_source=False)
    store = SQLiteStore(settings.sqlite_db)
    try:
        store.init_schema()
        groups = store.fetch_current_groups_filtered(event_date=event_date, event_name=event_name)
    finally:
        store.close()

    response: list[GroupFinalResponse] = []
    for group in groups:
        sample = group.athletes[0] if group.athletes else None
        athletes: list[GroupAthleteResultResponse] = []
        for place, athlete, _ in rank_group(group.athletes):
            athletes.append(_to_group_athlete_result(place=place, athlete=athlete))
        response.append(
            GroupFinalResponse(
                event_date=(sample.event_date if sample else ""),
                event_name=(sample.event_name if sample else ""),
                sheet_name=group.sheet_name,
                group_name=group.group_name,
                athletes=athletes,
            )
        )
    return response


@app.get("/v1/results/athlete-places", response_model=list[AthletePlacesRowResponse])
def get_athlete_places(event_name: str = "") -> list[AthletePlacesRowResponse]:
    settings = load_settings(require_source=False)
    store = SQLiteStore(settings.sqlite_db)
    try:
        store.init_schema()
        groups = store.fetch_current_groups_filtered(event_name=event_name)
    finally:
        store.close()

    by_name: dict[str, dict[str, str]] = {}
    for group in groups:
        sample = group.athletes[0] if group.athletes else None
        date_key = (sample.event_date if sample and sample.event_date else "без_даты")
        for place, athlete, _ in rank_group(group.athletes):
            name = _name_without_patronymic(athlete.full_name)
            by_name.setdefault(name, {})
            current = by_name[name].get(date_key)
            incoming = _athlete_date_cell(athlete, place)
            by_name[name][date_key] = _merge_result_cell(current, incoming)

    result: list[AthletePlacesRowResponse] = []
    for name in sorted(by_name.keys(), key=lambda value: value.lower()):
        result.append(AthletePlacesRowResponse(athlete_name=name, by_date=by_name[name]))
    return result


@app.get("/v1/streams/{stream_id}/events")
def stream_events_sse(stream_id: str) -> StreamingResponse:
    try:
        sub_id, q = manager.subscribe(stream_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="stream not found") from exc

    def event_generator():
        yield "event: connected\ndata: {\"status\":\"ok\"}\n\n"
        try:
            while True:
                try:
                    event = q.get(timeout=15.0)
                    event_name = str(event.get("event_type", "message"))
                    data = json.dumps(event, ensure_ascii=False)
                    yield f"event: {event_name}\ndata: {data}\n\n"
                except queue.Empty:
                    yield "event: ping\ndata: {}\n\n"
        finally:
            manager.unsubscribe(stream_id, sub_id)

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@app.websocket("/ws/streams/{stream_id}")
async def stream_events_ws(websocket: WebSocket, stream_id: str) -> None:
    try:
        sub_id, q = manager.subscribe(stream_id)
    except KeyError:
        await websocket.close(code=1008, reason="stream not found")
        return

    await websocket.accept()
    await websocket.send_json({"event_type": "connected", "stream_id": stream_id})
    try:
        while True:
            try:
                event = q.get(timeout=15.0)
                await websocket.send_json(event)
            except queue.Empty:
                await websocket.send_json({"event_type": "ping", "stream_id": stream_id})
    except WebSocketDisconnect:
        pass
    finally:
        manager.unsubscribe(stream_id, sub_id)


def _to_response(runtime: StreamRuntime) -> StreamStateResponse:
    return StreamStateResponse(
        stream_id=runtime.stream_id,
        status=runtime.status,
        spreadsheet_id=runtime.spreadsheet_id,
        started_at=runtime.started_at,
        finished_at=runtime.finished_at,
        error=runtime.error,
        event_count=runtime.event_count,
        last_event_at=runtime.last_event_at,
        callback_url=runtime.callback_url,
    )


def _to_group_athlete_result(place: int, athlete: AthleteRow) -> GroupAthleteResultResponse:
    final_value = athlete.effective_total()
    status = final_value.status if final_value.is_status and final_value.status else ""
    return GroupAthleteResultResponse(
        place=place,
        athlete_key=athlete.athlete_key,
        start_number=athlete.start_number,
        full_name=athlete.full_name,
        club=athlete.club,
        run1=athlete.run1.to_display() or "-",
        run2=athlete.run2.to_display() or "-",
        total=final_value.to_display() or "-",
        status=status,
        judge_note=athlete.judge_note,
    )


def _name_without_patronymic(full_name: str) -> str:
    parts = [part for part in full_name.strip().split() if part]
    if len(parts) < 2:
        return full_name.strip()
    return f"{parts[0]} {parts[1]}"


def _athlete_date_cell(athlete: AthleteRow, place: int) -> str:
    final_value = athlete.effective_total()
    if final_value.is_status and final_value.status:
        return final_value.status
    if athlete.judge_note and athlete.judge_note.upper().strip() not in {"DNS", "DNF", "DSQ"}:
        return "DSQ"
    return str(place)


def _merge_result_cell(existing: str | None, incoming: str) -> str:
    if existing is None:
        return incoming
    existing_num = existing.isdigit()
    incoming_num = incoming.isdigit()
    if existing_num and incoming_num:
        return str(min(int(existing), int(incoming)))
    priority = {"DNS": 1, "DNF": 2, "DSQ": 3}
    if existing_num and not incoming_num:
        return incoming
    if incoming_num and not existing_num:
        return existing
    return incoming if priority.get(incoming, 0) >= priority.get(existing, 0) else existing


def _clear_sqlite_state(sqlite_db: str) -> None:
    targets = [sqlite_db, f"{sqlite_db}-wal", f"{sqlite_db}-shm"]
    for path in targets:
        try:
            if path and os.path.exists(path):
                os.remove(path)
        except OSError as exc:
            logger.warning("sqlite_state_clear_failed path=%s error=%s", path, exc)
