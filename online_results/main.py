from __future__ import annotations

import argparse
import os
import time
from dataclasses import dataclass
from datetime import datetime

from dotenv import load_dotenv

from .db import SQLiteStore, diff_athletes
from .live import (
    LiveGroupTracker,
    build_group_analytics,
    group_phase,
    build_sheet_progress,
    has_result_update,
    render_change_lines,
    render_group_club_stats,
    render_group_table,
    render_kanaev_sheet_summary,
    render_overall_club_stats,
    render_tick_header,
)
from .models import AthleteRow
from .parser import parse_protocol_sheets
from .sheets_client import GoogleSheetsClient


@dataclass(frozen=True)
class Settings:
    spreadsheet_id: str
    service_account_file: str
    sqlite_db: str
    poll_interval_sec: float
    refresh_titles_every: int


def load_settings() -> Settings:
    load_dotenv()
    spreadsheet_id = os.getenv("GOOGLE_SPREADSHEET_ID", "").strip()
    service_account_file = os.getenv("GOOGLE_SERVICE_ACCOUNT_FILE", "").strip()
    sqlite_db = os.getenv("SQLITE_DB_PATH", "online_results.db").strip()
    poll_interval = float(os.getenv("POLL_INTERVAL_SEC", "1").strip())
    refresh_titles_every = int(os.getenv("REFRESH_SHEET_TITLES_EVERY", "120").strip())

    if not spreadsheet_id:
        raise RuntimeError("Не задан GOOGLE_SPREADSHEET_ID")
    if not service_account_file:
        raise RuntimeError("Не задан GOOGLE_SERVICE_ACCOUNT_FILE")

    return Settings(
        spreadsheet_id=spreadsheet_id,
        service_account_file=service_account_file,
        sqlite_db=sqlite_db,
        poll_interval_sec=poll_interval,
        refresh_titles_every=refresh_titles_every,
    )


def run() -> None:
    settings = load_settings()
    client = GoogleSheetsClient(
        spreadsheet_id=settings.spreadsheet_id,
        service_account_file=settings.service_account_file,
    )
    store = SQLiteStore(settings.sqlite_db)
    store.init_schema()
    tracker = LiveGroupTracker()

    previous_snapshot: dict[str, AthleteRow] = {}
    ticks = 0
    print("Старт онлайн-трансляции протокола. Интервал опроса:", settings.poll_interval_sec, "сек")

    try:
        while True:
            ticks += 1
            if ticks == 1 or ticks % settings.refresh_titles_every == 0:
                client.load_sheet_titles()

            sheet_values = client.fetch_all_sheets()
            parsed = parse_protocol_sheets(sheet_values)
            now_ts = datetime.now()
            sheet_progress = build_sheet_progress(parsed.groups)
            current_snapshot = {athlete.athlete_key: athlete for athlete in parsed.athletes}

            changes = diff_athletes(previous_snapshot, current_snapshot)
            store.persist_changes(changes)

            if changes:
                print(render_tick_header(len(changes)))
                updated_results = [
                    change.after
                    for change in changes
                    if has_result_update(change.before, change.after)
                ]
                tracker.register_result_updates(updated_results, now_ts)
                change_lines = render_change_lines(updated_results)
                if change_lines:
                    print("Обновленные результаты:")
                    for line in change_lines:
                        print(" -", line)

                updated_group_keys = {
                    f"{athlete.sheet_name}|{athlete.group_name}"
                    for athlete in updated_results
                }
                printed_sheet_summaries: set[str] = set()
                for group in parsed.groups:
                    if group.group_key not in updated_group_keys:
                        continue
                    progress = sheet_progress.get(group.sheet_name)
                    if progress and group.sheet_name not in printed_sheet_summaries:
                        summary_lines = render_kanaev_sheet_summary(
                            sheet_name=group.sheet_name,
                            groups=parsed.groups,
                            sheet_progress=progress,
                            tracker=tracker,
                            now=now_ts,
                        )
                        for line in summary_lines:
                            print(line)
                        printed_sheet_summaries.add(group.sheet_name)

                    analytics = build_group_analytics(
                        group=group,
                        sheet_phase=group_phase(group),
                    )
                    for line in render_group_table(group, header="Обновленная группа", analytics=analytics):
                        print(line)

                completed_groups = tracker.find_newly_completed_groups(parsed.groups)
                for group in completed_groups:
                    progress = sheet_progress.get(group.sheet_name)
                    analytics = build_group_analytics(
                        group=group,
                        sheet_phase=group_phase(group),
                    )
                    for line in render_group_table(group, header="Группа завершила старт", analytics=analytics):
                        print(line)
                    for line in render_group_club_stats(group):
                        print(line)

                if tracker.should_render_global_club_stats(parsed.groups):
                    for line in render_overall_club_stats(parsed.groups):
                        print(line)
                    tracker.mark_global_club_stats_rendered()

            previous_snapshot = current_snapshot
            time.sleep(settings.poll_interval_sec)
    finally:
        store.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Онлайн обновление результатов из Google Sheets в SQLite.")
    parser.add_argument(
        "--once",
        action="store_true",
        help="Считать снапшот один раз (для проверки конфигурации) и завершить.",
    )
    args = parser.parse_args()

    if args.once:
        settings = load_settings()
        client = GoogleSheetsClient(
            spreadsheet_id=settings.spreadsheet_id,
            service_account_file=settings.service_account_file,
        )
        data = client.fetch_all_sheets()
        parsed = parse_protocol_sheets(data)
        print(f"Листов: {len(data)}; спортсменов: {len(parsed.athletes)}; групп: {len(parsed.groups)}")
        return

    run()


if __name__ == "__main__":
    main()
