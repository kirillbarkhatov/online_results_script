from __future__ import annotations

import re
from collections import OrderedDict
from dataclasses import dataclass
from typing import Callable, Literal

from .models import AthleteRow, GroupBlock, ParsedValue, STATUS_CODE_BY_NUMBER


NUMBER_PATTERN = re.compile(r"^\d+(?:[.,]\d+)?$")
TIME_PATTERN = re.compile(r"^\d{1,2}:\d{1,2}(?:[.,]\d{1,3})?$")
ATHLETE_ROW_PATTERN = re.compile(r"^\d+(?:[.,]0+)?$")

SheetFormat = Literal["A", "A_LITE", "B", "UNKNOWN"]
DATE_DDMMYYYY_PATTERN = re.compile(r"\b(\d{2})[.\-/](\d{2})[.\-/](\d{4})\b")
DATE_DDMMYY_PATTERN = re.compile(r"\b(\d{2})[.\-/](\d{2})[.\-/](\d{2})\b")
DATE_TEXTUAL_PATTERN = re.compile(
    r"\b(\d{1,2})\s+(январ[яь]|феврал[яь]|март[а]?|апрел[яь]|ма[йя]|июн[яь]|июл[яь]|август[а]?|сентябр[яь]|октябр[яь]|ноябр[яь]|декабр[яь])\s+(\d{4})\b",
    flags=re.IGNORECASE,
)
RU_MONTH_NUM = {
    "январ": "01",
    "феврал": "02",
    "март": "03",
    "апрел": "04",
    "май": "05",
    "мая": "05",
    "июн": "06",
    "июл": "07",
    "август": "08",
    "сентябр": "09",
    "октябр": "10",
    "ноябр": "11",
    "декабр": "12",
}


@dataclass(frozen=True)
class ParsedProtocol:
    athletes: tuple[AthleteRow, ...]
    groups: tuple[GroupBlock, ...]


@dataclass(frozen=True)
class ColumnMap:
    start_number: int
    full_name: int | None
    surname: int | None
    name_part: int | None
    club: int
    run1: int
    run2: int | None
    total: int | None


@dataclass(frozen=True)
class SheetMeta:
    event_name: str
    event_date: str


def parse_protocol_sheets(sheet_values: dict[str, list[list[str]]]) -> ParsedProtocol:
    athletes: list[AthleteRow] = []
    groups: "OrderedDict[str, list[AthleteRow]]" = OrderedDict()

    for sheet_name, rows in sheet_values.items():
        parsed_sheet = _parse_sheet_auto(sheet_name, rows)
        if not parsed_sheet:
            parsed_sheet = _parse_sheet_legacy(sheet_name, rows)

        for athlete in parsed_sheet:
            athletes.append(athlete)
            group_key = _group_key(athlete.sheet_name, athlete.group_name)
            groups.setdefault(group_key, []).append(athlete)

    group_blocks = tuple(
        GroupBlock(
            group_key=group_key,
            sheet_name=group_athletes[0].sheet_name,
            group_name=group_athletes[0].group_name,
            athletes=tuple(group_athletes),
        )
        for group_key, group_athletes in groups.items()
        if group_athletes
    )

    return ParsedProtocol(athletes=tuple(athletes), groups=group_blocks)


def _parse_sheet_auto(sheet_name: str, rows: list[list[str]]) -> list[AthleteRow]:
    fmt = _detect_sheet_format(rows)
    if fmt in {"A", "A_LITE"}:
        return _parse_sheet_format_a(sheet_name, rows)
    if fmt == "B":
        return _parse_sheet_format_b(sheet_name, rows)
    return []


def _detect_sheet_format(rows: list[list[str]]) -> SheetFormat:
    scan = rows[:40]
    for row in scan:
        cells = [_clean(cell).lower() for cell in row]
        if any("старт номер" in cell for cell in cells) and any("фамил" in cell for cell in cells):
            return "B"

    header = _find_header_row_format_a(rows)
    if header is not None:
        _, cmap = header
        if cmap.run2 is None:
            return "A_LITE"
        return "A"

    return "UNKNOWN"


def _parse_sheet_format_a(sheet_name: str, rows: list[list[str]]) -> list[AthleteRow]:
    header = _find_header_row_format_a(rows)
    if header is None:
        return []
    header_row, cmap = header
    sheet_meta = _extract_sheet_meta(rows, header_row)

    parsed: list[AthleteRow] = []
    current_group = "Без группы"

    for row_index in range(header_row + 1, len(rows) + 1):
        row = rows[row_index - 1]

        if _is_header_like_a(row):
            continue

        group_title = _extract_group_title(row)
        if group_title is not None:
            current_group = group_title
            continue

        start_cell = _clean(_cell(row, cmap.start_number))
        if not start_cell:
            continue

        if not _is_athlete_row(start_cell):
            continue

        start_number = _parse_start_number(start_cell)
        if start_number is None:
            continue

        full_name = _clean(_cell(row, cmap.full_name)) if cmap.full_name is not None else ""
        club = _clean(_cell(row, cmap.club))
        run1 = parse_value(_clean(_cell(row, cmap.run1)))
        runs_count = 2
        if cmap.run2 is not None:
            run2 = parse_value(_clean(_cell(row, cmap.run2)))
        else:
            run2 = ParsedValue(raw="", value_type="empty")
            runs_count = 1
        total = parse_value(_clean(_cell(row, cmap.total))) if cmap.total is not None else ParsedValue(raw="", value_type="empty")
        judge_note = _extract_judge_note(row, (cmap.total if cmap.total is not None else cmap.run1) + 1)

        athlete_key = _athlete_key(sheet_name, sheet_meta, current_group, start_number, full_name)
        parsed.append(
            AthleteRow(
                athlete_key=athlete_key,
                sheet_name=sheet_name,
                group_name=current_group,
                sheet_row=row_index,
                start_number=start_number,
                full_name=full_name,
                club=club,
                run1=run1,
                run2=run2,
                total=total,
                runs_count=runs_count,
                event_name=sheet_meta.event_name,
                event_date=sheet_meta.event_date,
                judge_note=judge_note,
            )
        )

    return parsed


def _parse_sheet_format_b(sheet_name: str, rows: list[list[str]]) -> list[AthleteRow]:
    parsed: list[AthleteRow] = []
    current_group = "Без группы"
    cmap: ColumnMap | None = None
    sheet_meta = _extract_sheet_meta(rows, None)

    for row_index, row in enumerate(rows, start=1):
        if _is_header_like_b(row):
            cmap = _build_column_map_b(rows, row_index)
            continue

        group_title = _extract_group_title(row)
        if group_title is not None:
            current_group = group_title
            continue

        if cmap is None:
            continue

        start_cell = _clean(_cell(row, cmap.start_number))
        if not start_cell or not _is_athlete_row(start_cell):
            continue

        start_number = _parse_start_number(start_cell)
        if start_number is None:
            continue

        full_name = _compose_name_from_map(row, cmap)
        if not full_name:
            continue

        club = _clean(_cell(row, cmap.club))
        run1 = parse_value(_clean(_cell(row, cmap.run1)))
        run2 = parse_value(_clean(_cell(row, cmap.run2))) if cmap.run2 is not None else ParsedValue(raw="", value_type="empty")
        total = parse_value(_clean(_cell(row, cmap.total))) if cmap.total is not None else ParsedValue(raw="", value_type="empty")
        judge_note = _extract_judge_note(row, (cmap.total if cmap.total is not None else cmap.run1) + 1)

        athlete_key = _athlete_key(sheet_name, sheet_meta, current_group, start_number, full_name)
        parsed.append(
            AthleteRow(
                athlete_key=athlete_key,
                sheet_name=sheet_name,
                group_name=current_group,
                sheet_row=row_index,
                start_number=start_number,
                full_name=full_name,
                club=club,
                run1=run1,
                run2=run2,
                total=total,
                runs_count=2,
                event_name=sheet_meta.event_name,
                event_date=sheet_meta.event_date,
                judge_note=judge_note,
            )
        )

    return parsed


def _find_header_row_format_a(rows: list[list[str]]) -> tuple[int, ColumnMap] | None:
    for row_index, row in enumerate(rows[:40], start=1):
        cells = [_clean(cell).lower() for cell in row]
        if not any("фамилия" in cell and "имя" in cell for cell in cells):
            continue
        if not any("заезд" in cell or "трас" in cell for cell in cells):
            continue

        start_col = _find_column(cells, _is_start_header_cell)
        full_name_col = _find_column(cells, lambda value: "фамилия" in value and "имя" in value)
        club_col = _find_column(cells, lambda value: ("фсо" in value) or ("клуб" in value) or ("команда" in value))
        run1_col = _find_column(cells, lambda value: ("1" in value) and (("заезд" in value) or ("трас" in value)))
        run2_col = _find_column(cells, lambda value: ("2" in value) and (("заезд" in value) or ("трас" in value)))
        total_col = _find_column(cells, lambda value: ("результ" in value) or ("итог" in value) or ("сумма" in value))

        if start_col is None or full_name_col is None or club_col is None or run1_col is None:
            continue

        # One-run protocol variant: no run2 column, result after run1.
        if total_col is None:
            candidate = run1_col + 1
            if candidate < len(cells):
                total_col = candidate

        cmap = ColumnMap(
            start_number=start_col,
            full_name=full_name_col,
            surname=None,
            name_part=None,
            club=club_col,
            run1=run1_col,
            run2=run2_col,
            total=total_col,
        )
        return row_index, cmap

    return None


def _build_column_map_b(rows: list[list[str]], header_row_index: int) -> ColumnMap | None:
    header_row = rows[header_row_index - 1]
    cells = [_clean(cell).lower() for cell in header_row]

    start_col = _find_column(cells, _is_start_header_cell)
    surname_col = _find_column(cells, lambda value: "фамил" in value)
    name_col = _find_column(cells, lambda value: ("имя" in value) and ("отч" in value or value == "имя" or "имя " in value))
    club_col = _find_column(cells, lambda value: ("команда" in value) or ("фсо" in value))
    total_col = _find_column(cells, lambda value: ("сумма" in value) or ("результ" in value) or ("итог" in value))
    run1_col = _find_column(cells, lambda value: ("1" in value) and (("трас" in value) or ("заезд" in value)))
    run2_col = _find_column(cells, lambda value: ("2" in value) and (("трас" in value) or ("заезд" in value)))

    # In format B run1/run2 are often in the next row ("1 трасса", "2 трасса").
    if run1_col is None or run2_col is None:
        for offset in (1, 2):
            idx = header_row_index - 1 + offset
            if idx >= len(rows):
                continue
            sub_cells = [_clean(cell).lower() for cell in rows[idx]]
            if run1_col is None:
                run1_col = _find_column(sub_cells, lambda value: ("1" in value) and (("трас" in value) or ("заезд" in value)))
            if run2_col is None:
                run2_col = _find_column(sub_cells, lambda value: ("2" in value) and (("трас" in value) or ("заезд" in value)))
            if run1_col is not None and run2_col is not None:
                break

    if run1_col is None:
        attempts_col = _find_column(cells, lambda value: "попыт" in value)
        if attempts_col is not None:
            run1_col = attempts_col
            run2_col = attempts_col + 1 if attempts_col + 1 < len(cells) else None

    if total_col is None and run2_col is not None:
        candidate = run2_col + 1
        if candidate < len(cells):
            total_col = candidate

    if start_col is None or surname_col is None or name_col is None or club_col is None or run1_col is None:
        return None

    return ColumnMap(
        start_number=start_col,
        full_name=None,
        surname=surname_col,
        name_part=name_col,
        club=club_col,
        run1=run1_col,
        run2=run2_col,
        total=total_col,
    )


def _parse_sheet_legacy(sheet_name: str, rows: list[list[str]]) -> list[AthleteRow]:
    parsed: list[AthleteRow] = []
    current_group = "Без группы"
    sheet_meta = _extract_sheet_meta(rows, None)

    for row_index, row in enumerate(rows, start=1):
        if row_index <= 6:
            continue

        start_cell = _clean(_cell(row, 0))
        if not start_cell:
            continue

        if _is_group_separator(start_cell, row):
            current_group = start_cell
            continue

        if not _is_athlete_row(start_cell):
            continue

        start_number = _parse_start_number(start_cell)
        if start_number is None:
            continue

        full_name = _clean(_cell(row, 1))
        club = _clean(_cell(row, 2))
        run1 = parse_value(_clean(_cell(row, 3)))
        run2 = parse_value(_clean(_cell(row, 4)))
        total = parse_value(_clean(_cell(row, 5)))
        judge_note = _extract_judge_note(row, 6)

        athlete_key = _athlete_key(sheet_name, sheet_meta, current_group, start_number, full_name)
        parsed.append(
            AthleteRow(
                athlete_key=athlete_key,
                sheet_name=sheet_name,
                group_name=current_group,
                sheet_row=row_index,
                start_number=start_number,
                full_name=full_name,
                club=club,
                run1=run1,
                run2=run2,
                total=total,
                runs_count=2,
                event_name=sheet_meta.event_name,
                event_date=sheet_meta.event_date,
                judge_note=judge_note,
            )
        )

    return parsed


def parse_value(raw: str) -> ParsedValue:
    value = _clean(raw).upper()
    if not value:
        return ParsedValue(raw="", value_type="empty")

    if value in {"DNS", "DNF", "DSQ"}:
        return ParsedValue(raw=value, value_type="status", status=value)

    compact = re.sub(r"[^A-ZА-Я0-9]", "", value)
    if compact in {"MV", "МВ", "МВ1", "МВ2"}:
        return ParsedValue(raw=value, value_type="status", status="МВ")

    numeric_value = _parse_numeric(value)
    if numeric_value is not None:
        if int(numeric_value) in STATUS_CODE_BY_NUMBER and abs(numeric_value - int(numeric_value)) < 1e-9:
            status = STATUS_CODE_BY_NUMBER[int(numeric_value)]
            return ParsedValue(raw=value, value_type="status", status=status)
        if numeric_value <= 0:
            return ParsedValue(raw=value, value_type="empty")
        return ParsedValue(raw=value, value_type="time", seconds=numeric_value)

    if TIME_PATTERN.match(value):
        minutes, seconds = value.replace(",", ".").split(":")
        parsed = int(minutes) * 60 + float(seconds)
        return ParsedValue(raw=value, value_type="time", seconds=parsed)

    return ParsedValue(raw=value, value_type="status", status=value)


def _athlete_key(sheet_name: str, meta: SheetMeta, group_name: str, start_number: int, full_name: str) -> str:
    event_part = f"{meta.event_date}|{meta.event_name}".strip("|") or "event"
    return f"{event_part}|{sheet_name}|{group_name}|{start_number}|{full_name}"


def _extract_sheet_meta(rows: list[list[str]], header_row: int | None) -> SheetMeta:
    scan_limit = header_row if header_row is not None else min(len(rows), 12)
    lines: list[str] = []
    for row in rows[:scan_limit]:
        first = _clean(_cell(row, 0))
        if not first:
            continue
        low = first.lower()
        if "усл. обознач" in low:
            continue
        if _is_header_like_a(row) or _is_header_like_b(row):
            continue
        if "предварительные результаты" in low:
            continue
        lines.append(first)

    event_date = ""
    joined = " ".join(lines)
    match = DATE_DDMMYYYY_PATTERN.search(joined)
    if match:
        event_date = f"{match.group(1)}.{match.group(2)}.{match.group(3)}"
    if not event_date:
        match = DATE_DDMMYY_PATTERN.search(joined)
        if match:
            event_date = f"{match.group(1)}.{match.group(2)}.20{match.group(3)}"
    if not event_date:
        match = DATE_TEXTUAL_PATTERN.search(joined)
        if match:
            day = int(match.group(1))
            month_token = match.group(2).lower()
            year = match.group(3)
            month = next((value for key, value in RU_MONTH_NUM.items() if month_token.startswith(key)), "")
            if month:
                event_date = f"{day:02d}.{month}.{year}"

    event_name = _extract_event_name(lines)
    if not event_name:
        event_name = _clean(_cell(rows[0], 0)) if rows else ""
    event_name = event_name.strip(' "')
    return SheetMeta(event_name=event_name, event_date=event_date)


def _extract_event_name(lines: list[str]) -> str:
    if not lines:
        return ""
    best = ""
    best_score = -999
    for line in lines[:8]:
        value = line.strip()
        if not value:
            continue
        low = value.lower()
        score = 0
        if any(token in low for token in ("кубок", "первенств", "чемпионат", "старт", "этап", "тур", "мемориал")):
            score += 6
        if "соревнования по горнолыжному спорту" in low:
            score -= 5
        if any(token in low for token in ("область", "гк ", "ск ", "д. ", "ленинградская")):
            score -= 3
        if any(token in low for token in ("слалом", "девоч", "мальчик", "юнош", "девуш", "год рождения")):
            score -= 2
        if "протокол" in low and "кубок" not in low:
            score -= 2
        if DATE_DDMMYYYY_PATTERN.search(low) or DATE_DDMMYY_PATTERN.search(low) or DATE_TEXTUAL_PATTERN.search(low):
            score -= 1
        if value.isupper():
            score += 1
        if score > best_score:
            best_score = score
            best = value
    return best


def _is_athlete_row(value: str) -> bool:
    return bool(ATHLETE_ROW_PATTERN.match(value))


def _is_group_separator(start_cell: str, row: list[str]) -> bool:
    if _is_athlete_row(start_cell):
        return False
    if _clean(_cell(row, 1)) or _clean(_cell(row, 2)):
        return False
    return True


def _is_header_like_a(row: list[str]) -> bool:
    low = " | ".join(_clean(cell).lower() for cell in row if _clean(cell))
    return ("фамилия" in low and "имя" in low and ("заезд" in low or "трас" in low))


def _is_header_like_b(row: list[str]) -> bool:
    low = " | ".join(_clean(cell).lower() for cell in row if _clean(cell))
    return ("старт номер" in low and "фамил" in low) or ("попыт" in low and "команда" in low)


def _extract_group_title(row: list[str]) -> str | None:
    for cell in row[:3]:
        value = _clean(cell)
        if not value:
            continue
        low = value.lower()
        if any(token in low for token in ("девоч", "мальч", "девуш", "юнош", "юниор", "группа", "год рождения")):
            return value

    first = _clean(_cell(row, 0))
    if first and not _is_athlete_row(first):
        if not any(_clean(cell) for cell in row[1:4]):
            return first
    return None


def _compose_name_from_map(row: list[str], cmap: ColumnMap) -> str:
    if cmap.full_name is not None:
        return _clean(_cell(row, cmap.full_name))

    parts: list[str] = []
    if cmap.surname is not None:
        surname = _clean(_cell(row, cmap.surname))
        if surname:
            parts.append(surname)
    if cmap.name_part is not None:
        name_part = _clean(_cell(row, cmap.name_part))
        if name_part:
            parts.append(name_part)
    return " ".join(parts)


def _find_column(cells: list[str], predicate: Callable[[str], bool]) -> int | None:
    for index, cell in enumerate(cells):
        if predicate(cell):
            return index
    return None


def _is_start_header_cell(value: str) -> bool:
    if not value:
        return False
    return ("старт номер" in value) or (("ст" in value) and ("№" in value or "номер" in value))


def _parse_start_number(value: str) -> int | None:
    normalized = value.replace(",", ".")
    if not ATHLETE_ROW_PATTERN.match(normalized):
        return None
    try:
        return int(float(normalized))
    except ValueError:
        return None


def _group_key(sheet_name: str, group_name: str) -> str:
    return f"{sheet_name}|{group_name}"


def _cell(row: list[str], index: int | None) -> str:
    if index is None or index < 0 or index >= len(row):
        return ""
    value = row[index]
    return "" if value is None else str(value)


def _clean(value: str) -> str:
    return value.strip()


def _parse_numeric(value: str) -> float | None:
    normalized = value.replace(",", ".")
    if not NUMBER_PATTERN.match(normalized):
        return None
    return float(normalized)


def _extract_judge_note(row: list[str], start_index: int) -> str:
    if start_index < 0:
        start_index = 0
    for index in range(start_index, len(row)):
        value = _clean(_cell(row, index))
        if not value:
            continue
        upper = value.upper()
        if upper in {"DNS", "DNF", "DSQ"}:
            return upper

        numeric = _parse_numeric(value)
        if numeric is not None and abs(numeric - int(numeric)) < 1e-9:
            code = int(numeric)
            if code in STATUS_CODE_BY_NUMBER:
                return STATUS_CODE_BY_NUMBER[code]

        # Ignore pure place-like markers or arbitrary numbers in right-side columns.
        if _is_athlete_row(value) or numeric is not None:
            continue
        return value
    return ""
