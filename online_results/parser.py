from __future__ import annotations

import re
from collections import OrderedDict
from dataclasses import dataclass

from .models import AthleteRow, GroupBlock, ParsedValue, STATUS_CODE_BY_NUMBER


NUMBER_PATTERN = re.compile(r"^\d+(?:[.,]\d+)?$")
TIME_PATTERN = re.compile(r"^\d{1,2}:\d{1,2}(?:[.,]\d{1,3})?$")
ATHLETE_ROW_PATTERN = re.compile(r"^\d+(?:[.,]0+)?$")


@dataclass(frozen=True)
class ParsedProtocol:
    athletes: tuple[AthleteRow, ...]
    groups: tuple[GroupBlock, ...]


def parse_protocol_sheets(sheet_values: dict[str, list[list[str]]]) -> ParsedProtocol:
    athletes: list[AthleteRow] = []
    groups: "OrderedDict[str, list[AthleteRow]]" = OrderedDict()

    for sheet_name, rows in sheet_values.items():
        current_group = "Без группы"

        for row_index, row in enumerate(rows, start=1):
            if row_index <= 6:
                continue

            start_cell = _clean(_cell(row, 0))
            if not start_cell:
                continue

            if _is_group_separator(start_cell, row):
                current_group = start_cell
                group_key = _group_key(sheet_name, current_group)
                groups.setdefault(group_key, [])
                continue

            if not _is_athlete_row(start_cell):
                continue

            start_number = int(float(start_cell.replace(",", ".")))
            full_name = _clean(_cell(row, 1))
            club = _clean(_cell(row, 2))
            run1 = parse_value(_clean(_cell(row, 3)))
            run2 = parse_value(_clean(_cell(row, 4)))
            total = parse_value(_clean(_cell(row, 5)))

            athlete_key = f"{sheet_name}|{current_group}|{start_number}|{full_name}"
            athlete = AthleteRow(
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
            )
            athletes.append(athlete)

            group_key = _group_key(sheet_name, current_group)
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


def parse_value(raw: str) -> ParsedValue:
    value = _clean(raw).upper()
    if not value:
        return ParsedValue(raw="", value_type="empty")

    if value in {"DNS", "DNF", "DSQ"}:
        return ParsedValue(raw=value, value_type="status", status=value)

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


def _is_athlete_row(value: str) -> bool:
    return bool(ATHLETE_ROW_PATTERN.match(value))


def _is_group_separator(start_cell: str, row: list[str]) -> bool:
    if _is_athlete_row(start_cell):
        return False
    if _clean(_cell(row, 1)) or _clean(_cell(row, 2)):
        return False
    return True


def _group_key(sheet_name: str, group_name: str) -> str:
    return f"{sheet_name}|{group_name}"


def _cell(row: list[str], index: int) -> str:
    if index >= len(row):
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

