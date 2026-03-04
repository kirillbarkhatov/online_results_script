from __future__ import annotations

import argparse
import csv
import os
import re
import time
from collections import defaultdict
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
    rank_group,
)
from .models import AthleteRow
from .parser import parse_protocol_sheets
from .sheets_client import GoogleSheetsClient

ANSI_RESET = "\033[0m"
ANSI_STATUS_WARNING = "\033[48;5;226m\033[30m"


@dataclass(frozen=True)
class Settings:
    spreadsheet_id: str
    service_account_file: str
    sqlite_db: str
    poll_interval_sec: float
    refresh_titles_every: int
    finalize_timeout_sec: int
    finalize_max_missing: int


def load_settings(require_source: bool = True) -> Settings:
    load_dotenv()
    spreadsheet_id = os.getenv("GOOGLE_SPREADSHEET_ID", "").strip()
    service_account_file = os.getenv("GOOGLE_SERVICE_ACCOUNT_FILE", "").strip()
    sqlite_db = os.getenv("SQLITE_DB_PATH", "online_results.db").strip()
    poll_interval = float(os.getenv("POLL_INTERVAL_SEC", "1").strip())
    refresh_titles_every = int(os.getenv("REFRESH_SHEET_TITLES_EVERY", "120").strip())
    finalize_timeout_sec = int(
        os.getenv("FINALIZE_TIMEOUT_SEC", os.getenv("FINALIZE_TIMEOUT_MIN", "300")).strip()
    )
    if "FINALIZE_TIMEOUT_SEC" not in os.environ and "FINALIZE_TIMEOUT_MIN" in os.environ:
        finalize_timeout_sec *= 60
    finalize_max_missing = int(os.getenv("FINALIZE_MAX_MISSING", "2").strip())

    if require_source:
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
        finalize_timeout_sec=finalize_timeout_sec,
        finalize_max_missing=finalize_max_missing,
    )


def export_final_results(sqlite_db: str, csv_path: str | None = None) -> None:
    store = SQLiteStore(sqlite_db)
    try:
        store.init_schema()
        groups = store.fetch_current_groups()
    finally:
        store.close()

    if not groups:
        print("В базе нет актуальных результатов для экспорта.")
        return

    rows_for_csv: list[list[str]] = []
    for group in groups:
        for place, athlete, _ in rank_group(group.athletes):
            rows_for_csv.append(
                [
                    group.sheet_name,
                    group.group_name,
                    str(place),
                    str(athlete.start_number),
                    athlete.full_name,
                    athlete.club,
                    athlete.run1.to_display() or "-",
                    athlete.run2.to_display() or "-",
                    athlete.effective_total().to_display() or "-",
                ]
            )

    headers = ("лист", "группа", "место", "ст.№", "ФИО", "клуб", "1 заезд", "2 заезд", "итог")
    widths = [len(header) for header in headers]
    for row in rows_for_csv:
        for idx, cell in enumerate(row):
            widths[idx] = max(widths[idx], len(cell))

    def _fmt(cells: tuple[str, ...] | list[str]) -> str:
        return " | ".join(
            str(cell).ljust(widths[idx]) if idx in {0, 1, 4, 5} else str(cell).rjust(widths[idx])
            for idx, cell in enumerate(cells)
        )

    print("Итоговые результаты из SQLite")
    print(_fmt(headers))
    print("-+-".join("-" * width for width in widths))
    for row in rows_for_csv:
        print(_fmt(row))

    if csv_path:
        with open(csv_path, "w", newline="", encoding="utf-8") as out:
            writer = csv.writer(out)
            writer.writerow(headers)
            writer.writerows(rows_for_csv)
        print(f"\nCSV сохранен: {csv_path}")


def export_athlete_places_by_dates(sqlite_db: str, csv_path: str | None = None) -> None:
    store = SQLiteStore(sqlite_db)
    try:
        store.init_schema()
        groups = store.fetch_current_groups()
        group_updated_dates = store.fetch_group_updated_dates()
    finally:
        store.close()

    if not groups:
        print("В базе нет данных для экспорта истории спортсменов.")
        return

    places_by_name_date: dict[str, dict[str, str]] = defaultdict(dict)
    highlight_by_name_date: set[tuple[str, str]] = set()
    label_by_name: dict[str, str] = {}
    all_dates: set[str] = set()

    for group in groups:
        sample = group.athletes[0] if group.athletes else None
        event_date = sample.event_date if sample else ""
        fallback_date = _normalize_iso_date(group_updated_dates.get(group.group_key, ""))
        event_date = event_date.strip() or _extract_event_date(group.sheet_name, group.group_name) or fallback_date or "без_даты"
        all_dates.add(event_date)

        for place, athlete, _ in rank_group(group.athletes):
            name_key, name_label = _normalize_person_name(athlete.full_name)
            if not name_key:
                continue
            label_by_name.setdefault(name_key, name_label)
            cell_value, highlight = _athlete_date_value(athlete, place)
            existing = places_by_name_date[name_key].get(event_date)
            merged = _merge_place_cell(existing, cell_value)
            places_by_name_date[name_key][event_date] = merged
            if highlight and merged == "DSQ":
                highlight_by_name_date.add((name_key, event_date))

    sorted_dates = sorted(all_dates, key=_event_sort_key)
    headers = ["Фамилия Имя", *sorted_dates]
    rows: list[list[str]] = []
    for name_key in sorted(label_by_name.keys(), key=lambda k: label_by_name[k].lower()):
        row = [label_by_name[name_key]]
        for date_col in sorted_dates:
            row.append(places_by_name_date.get(name_key, {}).get(date_col, "-"))
        rows.append(row)

    widths = [len(h) for h in headers]
    for row in rows:
        for idx, cell in enumerate(row):
            widths[idx] = max(widths[idx], len(cell))

    def _fmt(cells: list[str]) -> str:
        parts: list[str] = []
        for idx, cell in enumerate(cells):
            if idx == 0:
                parts.append(cell.ljust(widths[idx]))
            else:
                parts.append(cell.rjust(widths[idx]))
        return " | ".join(parts)

    print("История мест по спортсменам (Фамилия + Имя)")
    print(_fmt(headers))
    print("-+-".join("-" * w for w in widths))
    for name_key in sorted(label_by_name.keys(), key=lambda k: label_by_name[k].lower()):
        raw_row = [label_by_name[name_key]]
        for date_col in sorted_dates:
            raw_row.append(places_by_name_date.get(name_key, {}).get(date_col, "-"))

        display_row = list(raw_row)
        for index, date_col in enumerate(sorted_dates, start=1):
            value = raw_row[index]
            if value == "DSQ" and (name_key, date_col) in highlight_by_name_date:
                display_row[index] = f"{ANSI_STATUS_WARNING}{value}{ANSI_RESET}"
        print(_fmt(display_row))

    if csv_path:
        with open(csv_path, "w", newline="", encoding="utf-8") as out:
            writer = csv.writer(out)
            writer.writerow(headers)
            writer.writerows(rows)
        print(f"\nCSV сохранен: {csv_path}")


def run() -> None:
    settings = load_settings()
    client = GoogleSheetsClient(
        spreadsheet_id=settings.spreadsheet_id,
        service_account_file=settings.service_account_file,
    )
    store = SQLiteStore(settings.sqlite_db)
    store.init_schema()
    tracker = LiveGroupTracker(
        finalize_timeout_sec=settings.finalize_timeout_sec,
        finalize_max_missing=settings.finalize_max_missing,
    )

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
            current_snapshot = {athlete.athlete_key: athlete for athlete in parsed.athletes}

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

            if changes:
                print(render_tick_header(len(changes)))
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
                            print(line)
                        printed_sheet_summaries.add(group.sheet_name)

                    analytics = build_group_analytics(
                        group=group,
                        sheet_phase=group_phase(group),
                    )
                    for line in render_group_table(group, header="Обновленная группа", analytics=analytics):
                        print(line)

            completed_groups = tracker.find_newly_completed_groups(effective_groups)
            if completed_groups and not changes:
                print(render_tick_header(0))
            for group in completed_groups:
                analytics = build_group_analytics(
                    group=group,
                    sheet_phase=group_phase(group),
                )
                for line in render_group_table(group, header="Группа завершила старт", analytics=analytics):
                    print(line)
                for line in render_group_club_stats(group):
                    print(line)

            if tracker.should_render_global_club_stats(effective_groups):
                if not changes and not completed_groups:
                    print(render_tick_header(0))
                for line in render_overall_club_stats(effective_groups):
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
    parser.add_argument(
        "--export-final",
        action="store_true",
        help="Экспортировать итоговые результаты из SQLite с расчетом мест по группам.",
    )
    parser.add_argument(
        "--export-final-csv",
        type=str,
        default="",
        help="Путь для сохранения CSV при экспорте итогов (используется с --export-final).",
    )
    parser.add_argument(
        "--export-athlete-places",
        action="store_true",
        help="Экспортировать историю мест: строка = Фамилия Имя, столбцы = даты соревнований.",
    )
    parser.add_argument(
        "--export-athlete-places-csv",
        type=str,
        default="",
        help="Путь CSV для --export-athlete-places.",
    )
    args = parser.parse_args()

    try:
        if args.export_athlete_places:
            settings = load_settings(require_source=False)
            csv_path = args.export_athlete_places_csv.strip() or None
            export_athlete_places_by_dates(sqlite_db=settings.sqlite_db, csv_path=csv_path)
            return

        if args.export_final:
            settings = load_settings(require_source=False)
            csv_path = args.export_final_csv.strip() or None
            export_final_results(sqlite_db=settings.sqlite_db, csv_path=csv_path)
            return

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
    except RuntimeError as exc:
        print(f"Ошибка запуска: {exc}")
        raise SystemExit(1) from exc


DATE_DDMMYYYY_PATTERN = re.compile(r"\b(\d{2})[.\-/](\d{2})[.\-/](\d{4})\b")
DATE_DDMMYY_PATTERN = re.compile(r"\b(\d{2})[.\-/](\d{2})[.\-/](\d{2})\b")


def _extract_event_date(sheet_name: str, group_name: str) -> str:
    text = f"{sheet_name} {group_name}"
    match = DATE_DDMMYYYY_PATTERN.search(text)
    if match:
        return f"{match.group(1)}.{match.group(2)}.{match.group(3)}"
    match = DATE_DDMMYY_PATTERN.search(text)
    if match:
        return f"{match.group(1)}.{match.group(2)}.20{match.group(3)}"
    return ""


def _normalize_person_name(full_name: str) -> tuple[str, str]:
    parts = [part for part in full_name.strip().split() if part]
    if len(parts) < 2:
        key = full_name.strip().upper()
        return key, full_name.strip()
    surname = parts[0]
    name = parts[1]
    label = f"{surname} {name}"
    return label.upper(), label


def _normalize_iso_date(value: str) -> str:
    text = value.strip()
    if not text:
        return ""
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", text):
        yyyy, mm, dd = text.split("-")
        return f"{dd}.{mm}.{yyyy}"
    return text


def _event_sort_key(date_value: str) -> tuple[int, int, int]:
    match = re.fullmatch(r"(\d{2})\.(\d{2})\.(\d{4})", date_value.strip())
    if not match:
        return (9999, 99, 99)
    dd, mm, yyyy = match.groups()
    return (int(yyyy), int(mm), int(dd))


def _athlete_date_value(athlete: AthleteRow, place: int) -> tuple[str, bool]:
    final_value = athlete.effective_total()
    if final_value.is_status and final_value.status in {"DNS", "DNF", "DSQ"}:
        return final_value.status, False
    if _has_text_judge_note(athlete.judge_note):
        return "DSQ", True
    return str(place), False


def _has_text_judge_note(note: str) -> bool:
    value = note.strip().upper()
    if not value:
        return False
    if value in {"DNS", "DNF", "DSQ"}:
        return False
    return True


def _merge_place_cell(existing: str | None, incoming: str) -> str:
    if existing is None:
        return incoming
    existing_is_num = existing.isdigit()
    incoming_is_num = incoming.isdigit()
    if existing_is_num and incoming_is_num:
        return str(min(int(existing), int(incoming)))
    priority = {"DNS": 1, "DNF": 2, "DSQ": 3}
    # Status must dominate numeric place if any status exists for this date.
    if existing_is_num and not incoming_is_num:
        return incoming
    if incoming_is_num and not existing_is_num:
        return existing
    return incoming if priority.get(incoming, 0) >= priority.get(existing, 0) else existing


if __name__ == "__main__":
    main()
