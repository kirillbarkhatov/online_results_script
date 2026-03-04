from __future__ import annotations

import json
import sqlite3
from collections import OrderedDict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from .models import AthleteRow, GroupBlock
from .parser import parse_value


@dataclass(frozen=True)
class AthleteChange:
    before: AthleteRow | None
    after: AthleteRow
    changed_fields: tuple[str, ...]


class SQLiteStore:
    def __init__(self, db_path: str | Path) -> None:
        self.db_path = str(db_path)
        self._connection = sqlite3.connect(self.db_path)
        self._connection.row_factory = sqlite3.Row
        self._connection.execute("PRAGMA journal_mode=WAL;")
        self._connection.execute("PRAGMA synchronous=NORMAL;")

    def close(self) -> None:
        self._connection.close()

    def init_schema(self) -> None:
        self._connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS athletes (
                athlete_key TEXT PRIMARY KEY,
                sheet_name TEXT NOT NULL,
                group_name TEXT NOT NULL,
                sheet_row INTEGER NOT NULL,
                start_number INTEGER NOT NULL,
                full_name TEXT NOT NULL,
                club TEXT NOT NULL,
                event_name TEXT NOT NULL DEFAULT '',
                event_date TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS current_results (
                athlete_key TEXT PRIMARY KEY,
                run1_raw TEXT NOT NULL,
                run2_raw TEXT NOT NULL,
                total_raw TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                FOREIGN KEY (athlete_key) REFERENCES athletes(athlete_key)
            );

            CREATE TABLE IF NOT EXISTS snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at TEXT NOT NULL,
                changed_count INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS result_updates (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                snapshot_id INTEGER NOT NULL,
                athlete_key TEXT NOT NULL,
                changed_fields TEXT NOT NULL,
                before_payload TEXT,
                after_payload TEXT NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY (snapshot_id) REFERENCES snapshots(id),
                FOREIGN KEY (athlete_key) REFERENCES athletes(athlete_key)
            );
            """
        )
        self._ensure_column("athletes", "event_name", "TEXT NOT NULL DEFAULT ''")
        self._ensure_column("athletes", "event_date", "TEXT NOT NULL DEFAULT ''")
        self._connection.commit()

    def _ensure_column(self, table_name: str, column_name: str, definition: str) -> None:
        cursor = self._connection.cursor()
        rows = cursor.execute(f"PRAGMA table_info({table_name})").fetchall()
        existing = {str(row["name"]) for row in rows}
        if column_name in existing:
            return
        cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {definition}")

    def persist_changes(self, changes: list[AthleteChange]) -> int | None:
        if not changes:
            return None

        now = _utc_now()
        cursor = self._connection.cursor()
        cursor.execute(
            "INSERT INTO snapshots(created_at, changed_count) VALUES(?, ?)",
            (now, len(changes)),
        )
        snapshot_id = int(cursor.lastrowid)

        for change in changes:
            athlete = change.after
            cursor.execute(
                """
                INSERT INTO athletes(
                    athlete_key, sheet_name, group_name, sheet_row, start_number, full_name, club, event_name, event_date, created_at, updated_at
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(athlete_key) DO UPDATE SET
                    sheet_name=excluded.sheet_name,
                    group_name=excluded.group_name,
                    sheet_row=excluded.sheet_row,
                    start_number=excluded.start_number,
                    full_name=excluded.full_name,
                    club=excluded.club,
                    event_name=excluded.event_name,
                    event_date=excluded.event_date,
                    updated_at=excluded.updated_at
                """,
                (
                    athlete.athlete_key,
                    athlete.sheet_name,
                    athlete.group_name,
                    athlete.sheet_row,
                    athlete.start_number,
                    athlete.full_name,
                    athlete.club,
                    athlete.event_name,
                    athlete.event_date,
                    now,
                    now,
                ),
            )
            cursor.execute(
                """
                INSERT INTO current_results(athlete_key, run1_raw, run2_raw, total_raw, updated_at)
                VALUES(?, ?, ?, ?, ?)
                ON CONFLICT(athlete_key) DO UPDATE SET
                    run1_raw=excluded.run1_raw,
                    run2_raw=excluded.run2_raw,
                    total_raw=excluded.total_raw,
                    updated_at=excluded.updated_at
                """,
                (
                    athlete.athlete_key,
                    athlete.run1.raw,
                    athlete.run2.raw,
                    athlete.effective_total().raw,
                    now,
                ),
            )
            cursor.execute(
                """
                INSERT INTO result_updates(
                    snapshot_id, athlete_key, changed_fields, before_payload, after_payload, created_at
                ) VALUES(?, ?, ?, ?, ?, ?)
                """,
                (
                    snapshot_id,
                    athlete.athlete_key,
                    json.dumps(change.changed_fields, ensure_ascii=False),
                    json.dumps(_athlete_payload(change.before), ensure_ascii=False) if change.before else None,
                    json.dumps(_athlete_payload(change.after), ensure_ascii=False),
                    now,
                ),
            )

        self._connection.commit()
        return snapshot_id

    def fetch_current_groups(self) -> tuple[GroupBlock, ...]:
        cursor = self._connection.cursor()
        cursor.execute(
            """
            SELECT
                a.athlete_key,
                a.sheet_name,
                a.group_name,
                a.sheet_row,
                a.start_number,
                a.full_name,
                a.club,
                COALESCE(a.event_name, '') AS event_name,
                COALESCE(a.event_date, '') AS event_date,
                COALESCE(c.run1_raw, '') AS run1_raw,
                COALESCE(c.run2_raw, '') AS run2_raw,
                COALESCE(c.total_raw, '') AS total_raw
            FROM athletes a
            LEFT JOIN current_results c ON c.athlete_key = a.athlete_key
            ORDER BY a.sheet_name, a.group_name, a.start_number, a.sheet_row
            """
        )
        grouped: "OrderedDict[str, list[AthleteRow]]" = OrderedDict()
        for row in cursor.fetchall():
            athlete = AthleteRow(
                athlete_key=str(row["athlete_key"]),
                sheet_name=str(row["sheet_name"]),
                group_name=str(row["group_name"]),
                sheet_row=int(row["sheet_row"]),
                start_number=int(row["start_number"]),
                full_name=str(row["full_name"]),
                club=str(row["club"]),
                run1=parse_value(str(row["run1_raw"])),
                run2=parse_value(str(row["run2_raw"])),
                total=parse_value(str(row["total_raw"])),
                event_name=str(row["event_name"]),
                event_date=str(row["event_date"]),
            )
            key = f"{athlete.event_date}|{athlete.event_name}|{athlete.sheet_name}|{athlete.group_name}"
            grouped.setdefault(key, []).append(athlete)

        return tuple(
            GroupBlock(
                group_key=group_key,
                sheet_name=athletes[0].sheet_name,
                group_name=athletes[0].group_name,
                athletes=tuple(athletes),
            )
            for group_key, athletes in grouped.items()
            if athletes
        )

    def fetch_group_updated_dates(self) -> dict[str, str]:
        cursor = self._connection.cursor()
        cursor.execute(
            """
            SELECT
                COALESCE(event_date, '') AS event_date,
                COALESCE(event_name, '') AS event_name,
                sheet_name,
                group_name,
                substr(MAX(updated_at), 1, 10) AS updated_date
            FROM athletes
            GROUP BY event_date, event_name, sheet_name, group_name
            """
        )
        result: dict[str, str] = {}
        for row in cursor.fetchall():
            group_key = f"{row['event_date']}|{row['event_name']}|{row['sheet_name']}|{row['group_name']}"
            result[group_key] = str(row["updated_date"])
        return result

    def fetch_group_event_meta(self) -> dict[str, tuple[str, str]]:
        cursor = self._connection.cursor()
        cursor.execute(
            """
            SELECT
                sheet_name,
                group_name,
                COALESCE(NULLIF(MAX(event_date), ''), '') AS event_date,
                COALESCE(NULLIF(MAX(event_name), ''), '') AS event_name
            FROM athletes
            GROUP BY sheet_name, group_name
            """
        )
        result: dict[str, tuple[str, str]] = {}
        for row in cursor.fetchall():
            group_key = f"{row['sheet_name']}|{row['group_name']}"
            result[group_key] = (str(row["event_date"]), str(row["event_name"]))
        return result


def diff_athletes(previous: dict[str, AthleteRow], current: dict[str, AthleteRow]) -> list[AthleteChange]:
    changes: list[AthleteChange] = []
    for athlete_key, athlete in current.items():
        before = previous.get(athlete_key)
        changed_fields: list[str] = []
        if before is None:
            changed_fields.append("new")
        else:
            if before.full_name != athlete.full_name:
                changed_fields.append("full_name")
            if before.club != athlete.club:
                changed_fields.append("club")
            if before.event_name != athlete.event_name:
                changed_fields.append("event_name")
            if before.event_date != athlete.event_date:
                changed_fields.append("event_date")
            if before.judge_note != athlete.judge_note:
                changed_fields.append("judge_note")
            if before.run1.raw != athlete.run1.raw:
                changed_fields.append("run1")
            if before.run2.raw != athlete.run2.raw:
                changed_fields.append("run2")
            if before.effective_total().raw != athlete.effective_total().raw:
                changed_fields.append("total")
        if changed_fields:
            changes.append(AthleteChange(before=before, after=athlete, changed_fields=tuple(changed_fields)))
    return changes


def _athlete_payload(athlete: AthleteRow | None) -> dict[str, str] | None:
    if athlete is None:
        return None
    return {
        "athlete_key": athlete.athlete_key,
        "sheet_name": athlete.sheet_name,
        "group_name": athlete.group_name,
        "start_number": str(athlete.start_number),
        "full_name": athlete.full_name,
        "club": athlete.club,
        "event_name": athlete.event_name,
        "event_date": athlete.event_date,
        "judge_note": athlete.judge_note,
        "run1": athlete.run1.raw,
        "run2": athlete.run2.raw,
        "total": athlete.effective_total().raw,
    }


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()
