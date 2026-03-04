from __future__ import annotations

import argparse
import csv
import os
import re
from collections import defaultdict
from dataclasses import dataclass

from dotenv import load_dotenv

from .db import SQLiteStore
from .live import rank_group
from .models import AthleteRow
from .parser import parse_protocol_sheets
from .sheets_client import GoogleSheetsClient
from .streaming import StreamRunConfig, run_stream

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
    config = StreamRunConfig(
        spreadsheet_id=settings.spreadsheet_id,
        service_account_file=settings.service_account_file,
        sqlite_db=settings.sqlite_db,
        poll_interval_sec=settings.poll_interval_sec,
        refresh_titles_every=settings.refresh_titles_every,
        finalize_timeout_sec=settings.finalize_timeout_sec,
        finalize_max_missing=settings.finalize_max_missing,
        stop_on_completion=False,
        console_output=True,
    )
    run_stream(config=config)


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
