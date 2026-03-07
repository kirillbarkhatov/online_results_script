from __future__ import annotations

import json
import hashlib
import re
import threading
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Callable

from .db import SQLiteStore, diff_athletes
from .live import (
    LiveGroupTracker,
    build_group_analytics,
    build_sheet_progress,
    group_phase,
    has_result_update,
    rank_group,
    render_change_lines,
    render_group_club_stats,
    render_group_table,
    render_kanaev_sheet_summary,
    render_overall_club_stats,
    render_tick_header,
)
from .models import AthleteRow, GroupBlock
from .parser import parse_protocol_sheets
from .sheets_client import GoogleSheetsClient


EventSink = Callable[[str, dict[str, object]], None]
ANSI_ESCAPE_PATTERN = re.compile(r"\x1b\[[0-9;]*m")


@dataclass(frozen=True)
class StreamRunConfig:
    spreadsheet_id: str
    service_account_file: str
    sqlite_db: str
    poll_interval_sec: float
    refresh_titles_every: int
    finalize_timeout_sec: int
    finalize_max_missing: int
    stop_on_completion: bool = False
    console_output: bool = True
    retry_base_sec: float = 25.0
    retry_cap_sec: float = 45.0
    retry_max_attempts: int = 12


def run_stream(
    config: StreamRunConfig,
    sink: EventSink | None = None,
    stop_event: threading.Event | None = None,
) -> None:
    client = GoogleSheetsClient(
        spreadsheet_id=config.spreadsheet_id,
        service_account_file=config.service_account_file,
    )
    store = SQLiteStore(config.sqlite_db)
    store.init_schema()
    tracker = LiveGroupTracker(
        finalize_timeout_sec=config.finalize_timeout_sec,
        finalize_max_missing=config.finalize_max_missing,
    )

    previous_snapshot: dict[str, AthleteRow] = {}
    ticks = 0
    consecutive_fetch_errors = 0
    snapshot_emitted = False
    last_forecast_hash = ""
    _out(config, "Старт онлайн-трансляции протокола. Интервал опроса:", config.poll_interval_sec, "сек")
    _emit(
        sink,
        "stream_started",
        {
            "schema_version": 3,
            "started_at": datetime.now().isoformat(),
            "spreadsheet_id": config.spreadsheet_id,
            "poll_interval_sec": config.poll_interval_sec,
            "data": {
                "started_at": datetime.now().isoformat(),
                "spreadsheet_id": config.spreadsheet_id,
                "poll_interval_sec": config.poll_interval_sec,
            },
        },
    )

    try:
        while True:
            if stop_event and stop_event.is_set():
                _emit(
                    sink,
                    "stream_stopped",
                    {
                        "schema_version": 2,
                        "stopped_at": datetime.now().isoformat(),
                        "reason": "external_stop",
                    },
                )
                return

            ticks += 1
            try:
                if ticks == 1 or ticks % config.refresh_titles_every == 0:
                    client.load_sheet_titles()
                sheet_values = client.fetch_all_sheets()
                consecutive_fetch_errors = 0
            except Exception as exc:
                if not _is_retryable_fetch_error(exc):
                    raise
                consecutive_fetch_errors += 1
                retry_in = _next_retry_delay(
                    attempt=consecutive_fetch_errors,
                    base_sec=config.retry_base_sec,
                    cap_sec=config.retry_cap_sec,
                )
                _emit(
                    sink,
                    "stream_warning",
                    {
                        "schema_version": 3,
                        "warning_type": "source_retry",
                        "warning": str(exc),
                        "error_type": type(exc).__name__,
                        "attempt": consecutive_fetch_errors,
                        "next_retry_sec": retry_in,
                        "at": datetime.now().isoformat(),
                    },
                )
                if consecutive_fetch_errors >= config.retry_max_attempts:
                    raise RuntimeError(
                        f"Превышено число попыток чтения источника ({config.retry_max_attempts}): {exc}"
                    ) from exc
                if _sleep_with_stop(stop_event=stop_event, seconds=retry_in):
                    _emit(
                        sink,
                        "stream_stopped",
                        {
                            "schema_version": 3,
                            "stopped_at": datetime.now().isoformat(),
                            "reason": "external_stop",
                        },
                    )
                    return
                continue

            parsed = parse_protocol_sheets(sheet_values)
            now_ts = datetime.now()
            current_snapshot = {athlete.athlete_key: athlete for athlete in parsed.athletes}
            has_any_progress = any(athlete.has_any_progress() for athlete in parsed.athletes)
            competition_phase = "running" if has_any_progress else "upcoming"
            status_text = "Соревнование идет" if has_any_progress else "Соревнование скоро начнется"

            changes = diff_athletes(previous_snapshot, current_snapshot)
            store.persist_changes(changes)
            updated_results = [
                change.after
                for change in changes
                if has_result_update(change.before, change.after)
            ]
            tracker.register_result_updates(updated_results, now_ts)

            effective_groups = tracker.apply_auto_finalize(parsed.groups, now_ts)
            sheet_progress = build_sheet_progress(effective_groups)
            if not snapshot_emitted:
                _emit(
                    sink,
                    "stream_snapshot",
                    {
                        "schema_version": 3,
                        "ts": now_ts.isoformat(),
                        "competition_phase": competition_phase,
                        "status_text": status_text,
                        "competition_title": _extract_competition_title(parsed.athletes),
                        "teams": _extract_teams(parsed.athletes),
                        "athletes": _serialize_athletes(list(parsed.athletes)),
                        "groups": _serialize_groups_snapshot(effective_groups),
                    },
                )
                snapshot_emitted = True

            if changes:
                _out(config, render_tick_header(len(changes)))
                _emit(
                    sink,
                    "tick",
                    {
                        "schema_version": 3,
                        "ts": now_ts.isoformat(),
                        "competition_phase": competition_phase,
                        "status_text": status_text,
                        "changed_count": len(changes),
                        "updated_results": _serialize_athletes(updated_results),
                        "data": {
                            "tick_ts": now_ts.isoformat(),
                            "changed_count": len(changes),
                            "updated_results": _serialize_athletes(updated_results),
                        },
                    },
                )
                change_lines = render_change_lines(updated_results)
                if change_lines:
                    _out(config, "Обновленные результаты:")
                    for line in change_lines:
                        _out(config, " -", line)
                    _emit(
                        sink,
                        "result_updated",
                        {
                            "schema_version": 3,
                            "lines": change_lines,
                            "lines_plain": _to_plain_lines(change_lines),
                            "count": len(change_lines),
                            "data": {
                                "count": len(change_lines),
                                "updated_results": _serialize_athletes(updated_results),
                            },
                        },
                    )

                updated_group_keys = {
                    f"{athlete.sheet_name}|{athlete.group_name}"
                    for athlete in updated_results
                }
                printed_sheet_summaries: set[str] = set()
                for group in effective_groups:
                    if group.group_key not in updated_group_keys:
                        continue
                    progress = sheet_progress.get(group.sheet_name)
                    if progress and group.sheet_name not in printed_sheet_summaries:
                        summary_lines = render_kanaev_sheet_summary(
                            sheet_name=group.sheet_name,
                            groups=effective_groups,
                            sheet_progress=progress,
                            tracker=tracker,
                            now=now_ts,
                        )
                        for line in summary_lines:
                            _out(config, line)
                        _emit(
                            sink,
                            "kanaev_summary_updated",
                            {
                                "schema_version": 3,
                                "sheet_name": group.sheet_name,
                                "lines": summary_lines,
                                "lines_plain": _to_plain_lines(summary_lines),
                                "data": {
                                    "sheet_name": group.sheet_name,
                                    "lines_plain": _to_plain_lines(summary_lines),
                                },
                            },
                        )
                        printed_sheet_summaries.add(group.sheet_name)

                    analytics = build_group_analytics(
                        group=group,
                        sheet_phase=group_phase(group),
                    )
                    lines = render_group_table(group, header="Обновленная группа", analytics=analytics)
                    for line in lines:
                        _out(config, line)
                    _emit(
                        sink,
                        "group_table_updated",
                        {
                            "schema_version": 3,
                            "group_key": group.group_key,
                            "sheet_name": group.sheet_name,
                            "group_name": group.group_name,
                            "lines": lines,
                            "lines_plain": _to_plain_lines(lines),
                            "data": _serialize_group_table(
                                group=group,
                                analytics=analytics,
                                rendered_lines=lines,
                                header="Обновленная группа",
                            ),
                        },
                    )

            completed_groups = tracker.find_newly_completed_groups(effective_groups)
            if completed_groups and not changes:
                _out(config, render_tick_header(0))
            for group in completed_groups:
                analytics = build_group_analytics(
                    group=group,
                    sheet_phase=group_phase(group),
                )
                group_lines = render_group_table(group, header="Группа завершила старт", analytics=analytics)
                for line in group_lines:
                    _out(config, line)
                club_lines = render_group_club_stats(group)
                for line in club_lines:
                    _out(config, line)
                _emit(
                    sink,
                    "group_completed",
                    {
                        "schema_version": 3,
                        "group_key": group.group_key,
                        "sheet_name": group.sheet_name,
                        "group_name": group.group_name,
                        "table_lines": group_lines,
                        "table_lines_plain": _to_plain_lines(group_lines),
                        "club_stats_lines": club_lines,
                        "club_stats_lines_plain": _to_plain_lines(club_lines),
                        "data": {
                            "group_table": _serialize_group_table(
                                group=group,
                                analytics=analytics,
                                rendered_lines=group_lines,
                                header="Группа завершила старт",
                            ),
                            "club_stats": {
                                "lines_plain": _to_plain_lines(club_lines),
                            },
                        },
                    },
                )

            if tracker.should_render_global_club_stats(effective_groups):
                if not changes and not completed_groups:
                    _out(config, render_tick_header(0))
                overall_lines = render_overall_club_stats(effective_groups)
                for line in overall_lines:
                    _out(config, line)
                tracker.mark_global_club_stats_rendered()
                _emit(
                    sink,
                    "overall_completed",
                    {
                        "schema_version": 3,
                        "lines": overall_lines,
                        "lines_plain": _to_plain_lines(overall_lines),
                        "completed_at": datetime.now().isoformat(),
                        "data": {
                            "lines_plain": _to_plain_lines(overall_lines),
                            "completed_at": datetime.now().isoformat(),
                        },
                    },
                )
                if config.stop_on_completion:
                    _emit(
                        sink,
                        "stream_completed",
                        {
                            "schema_version": 3,
                            "completed_at": datetime.now().isoformat(),
                        },
                    )
                    return

            forecast_payload = _build_start_forecast_payload(
                groups=effective_groups,
                sheet_progress=sheet_progress,
                tracker=tracker,
                now=now_ts,
            )
            forecast_hash = hashlib.sha256(
                json.dumps(forecast_payload, ensure_ascii=False, sort_keys=True).encode("utf-8")
            ).hexdigest()
            if forecast_hash != last_forecast_hash:
                _emit(
                    sink,
                    "start_forecast_updated",
                    {
                        "schema_version": 3,
                        "ts": now_ts.isoformat(),
                        "competition_phase": competition_phase,
                        "status_text": status_text,
                        **forecast_payload,
                    },
                )
                last_forecast_hash = forecast_hash

            previous_snapshot = current_snapshot
            if _sleep_with_stop(stop_event=stop_event, seconds=config.poll_interval_sec):
                _emit(
                    sink,
                    "stream_stopped",
                    {
                        "schema_version": 3,
                        "stopped_at": datetime.now().isoformat(),
                        "reason": "external_stop",
                    },
                )
                return
    except Exception as exc:
        _emit(
            sink,
            "stream_error",
            {
                "schema_version": 3,
                "error": str(exc),
                "error_type": type(exc).__name__,
                "failed_at": datetime.now().isoformat(),
            },
        )
        raise
    finally:
        store.close()


def _is_retryable_fetch_error(exc: Exception) -> bool:
    text = str(exc).lower()
    markers = ("429", "rate_limit_exceeded", "quota exceeded", "timed out", "temporarily unavailable", "503", "502")
    return any(marker in text for marker in markers)


def _next_retry_delay(attempt: int, base_sec: float, cap_sec: float) -> float:
    if attempt <= 1:
        return max(base_sec, 1.0)
    scaled = base_sec + ((attempt - 1) * 5.0)
    return min(max(scaled, 1.0), max(cap_sec, 1.0))


def _sleep_with_stop(stop_event: threading.Event | None, seconds: float) -> bool:
    if seconds <= 0:
        return bool(stop_event and stop_event.is_set())
    if stop_event is None:
        time.sleep(seconds)
        return False
    return stop_event.wait(timeout=seconds)


def _extract_teams(athletes: tuple[AthleteRow, ...]) -> list[str]:
    teams = sorted({athlete.club.strip() for athlete in athletes if athlete.club.strip()}, key=str.lower)
    return teams


def _extract_competition_title(athletes: tuple[AthleteRow, ...]) -> str:
    for athlete in athletes:
        title = athlete.event_name.strip()
        if title:
            return title
    return ""


def _serialize_groups_snapshot(groups: tuple[GroupBlock, ...]) -> list[dict[str, object]]:
    payload: list[dict[str, object]] = []
    for group in groups:
        analytics = build_group_analytics(group=group, sheet_phase=group_phase(group))
        table_lines = render_group_table(group, header="Снимок группы", analytics=analytics)
        completion_stage = _infer_completed_stage(group)
        payload.append(
            {
                "group_key": group.group_key,
                "sheet_name": group.sheet_name,
                "group_name": group.group_name,
                "run_stage": completion_stage,
                "is_finalized": completion_stage is not None,
                "data": _serialize_group_table(
                    group=group,
                    analytics=analytics,
                    rendered_lines=table_lines,
                    header="Снимок группы",
                ),
            }
        )
    return payload


def _infer_completed_stage(group: GroupBlock) -> int | None:
    run1_complete = all(not athlete.run1.is_empty for athlete in group.athletes) if group.athletes else False
    run2_complete = all(not athlete.run2.is_empty for athlete in group.athletes) if group.athletes else False
    if run2_complete:
        return 2
    if run1_complete:
        return 1
    return None


def _build_start_forecast_payload(
    *,
    groups: tuple[GroupBlock, ...],
    sheet_progress: dict[str, object],
    tracker: LiveGroupTracker,
    now: datetime,
) -> dict[str, object]:
    rows: list[dict[str, object]] = []
    for group in groups:
        progress = sheet_progress.get(group.sheet_name)
        current_run = int(getattr(progress, "current_run", 0) or 0)
        if current_run not in {1, 2}:
            continue
        for athlete in group.athletes:
            if current_run == 1:
                has_result = not athlete.run1.is_empty
            else:
                has_result = not athlete.run2.is_empty
            eta = tracker.estimate_result_time(
                sheet_athletes=tuple(getattr(progress, "athletes", tuple(group.athletes))),
                sheet_name=group.sheet_name,
                run_number=current_run,
                athlete_key=athlete.athlete_key,
                now=now,
            )
            rows.append(
                {
                    "athlete_key": athlete.athlete_key,
                    "sheet_name": group.sheet_name,
                    "group_name": group.group_name,
                    "run_number": current_run,
                    "start_number": athlete.start_number,
                    "full_name": athlete.full_name,
                    "club": athlete.club,
                    "has_result": has_result,
                    "eta": (eta.isoformat() if eta else ""),
                }
            )
    rows.sort(
        key=lambda item: (
            str(item.get("sheet_name") or "").lower(),
            int(item.get("run_number") or 0),
            str(item.get("group_name") or "").lower(),
            int(item.get("start_number") or 0),
        )
    )
    return {"rows": rows}


def _serialize_athletes(athletes: list[AthleteRow]) -> list[dict[str, object]]:
    payload: list[dict[str, object]] = []
    for athlete in athletes:
        payload.append(
            {
                "athlete_key": athlete.athlete_key,
                "sheet_name": athlete.sheet_name,
                "group_name": athlete.group_name,
                "start_number": athlete.start_number,
                "full_name": athlete.full_name,
                "club": athlete.club,
                "run1": athlete.run1.to_display() or "-",
                "run2": athlete.run2.to_display() or "-",
                "total": athlete.effective_total().to_display() or "-",
                "judge_note": athlete.judge_note,
            }
        )
    return payload


def _emit(sink: EventSink | None, event_type: str, payload: dict[str, object]) -> None:
    if sink is None:
        return
    sink(event_type, payload)


def _out(config: StreamRunConfig, *parts: object) -> None:
    if not config.console_output:
        return
    print(*parts)


def event_to_json(stream_id: str, event_type: str, payload: dict[str, object]) -> str:
    return json.dumps(
        {
            "stream_id": stream_id,
            "event_type": event_type,
            "event_time": datetime.now().isoformat(),
            "payload": payload,
        },
        ensure_ascii=False,
    )


def _to_plain_lines(lines: list[str]) -> list[str]:
    plain: list[str] = []
    for line in lines:
        plain.append(ANSI_ESCAPE_PATTERN.sub("", line))
    return plain


def _serialize_group_table(
    group: GroupBlock,
    analytics,
    rendered_lines: list[str],
    header: str,
) -> dict[str, object]:
    ranking = rank_group(group.athletes)
    extra_headers = analytics.headers if analytics else tuple()
    headers = ("место", "ст.№", "ФИО", "клуб", "1 заезд", "2 заезд", "итог", "интервал") + extra_headers
    rows: list[dict[str, object]] = []
    for place, athlete, interval in ranking:
        row: dict[str, object] = {
            "place": place,
            "athlete_key": athlete.athlete_key,
            "start_number": athlete.start_number,
            "full_name": athlete.full_name,
            "club": athlete.club,
            "run1": athlete.run1.to_display() or "-",
            "run2": athlete.run2.to_display() or "-",
            "total": athlete.effective_total().to_display() or "-",
            "interval": (f"+{interval:.2f}" if interval is not None else "-"),
            "judge_note": athlete.judge_note,
        }
        if analytics:
            analytics_values = analytics.values_by_athlete.get(
                athlete.athlete_key,
                tuple("-" for _ in extra_headers),
            )
            row["analytics"] = {name: value for name, value in zip(extra_headers, analytics_values, strict=False)}
        rows.append(row)
    return {
        "header": header,
        "group_key": group.group_key,
        "sheet_name": group.sheet_name,
        "group_name": group.group_name,
        "headers": list(headers),
        "rows": rows,
        "lines_plain": _to_plain_lines(rendered_lines),
    }
