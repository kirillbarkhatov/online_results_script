from __future__ import annotations

from dataclasses import dataclass
from typing import Literal


STATUS_CODE_BY_NUMBER = {100: "DNS", 150: "DNF", 200: "DSQ"}
STATUS_NUMBER_BY_CODE = {v: k for k, v in STATUS_CODE_BY_NUMBER.items()}
STATUS_RANK_WEIGHT = {"DNS": 10_000_000.0, "DNF": 20_000_000.0, "DSQ": 30_000_000.0}


@dataclass(frozen=True)
class ParsedValue:
    raw: str
    value_type: Literal["empty", "time", "status"]
    seconds: float | None = None
    status: str | None = None

    @property
    def is_empty(self) -> bool:
        return self.value_type == "empty"

    @property
    def is_time(self) -> bool:
        return self.value_type == "time"

    @property
    def is_status(self) -> bool:
        return self.value_type == "status"

    def to_display(self) -> str:
        if self.value_type == "empty":
            return ""
        if self.value_type == "status":
            return self.status or ""
        return format_seconds(self.seconds)

    def sort_key(self) -> float:
        if self.value_type == "time":
            return self.seconds if self.seconds is not None else float("inf")
        if self.value_type == "status":
            return STATUS_RANK_WEIGHT.get(self.status or "", 99_000_000.0)
        return 90_000_000.0


EMPTY_VALUE = ParsedValue(raw="", value_type="empty")


@dataclass(frozen=True)
class AthleteRow:
    athlete_key: str
    sheet_name: str
    group_name: str
    sheet_row: int
    start_number: int
    full_name: str
    club: str
    run1: ParsedValue
    run2: ParsedValue
    total: ParsedValue

    def has_any_progress(self) -> bool:
        return any(not value.is_empty for value in (self.run1, self.run2, self.total))

    def has_second_run_result(self) -> bool:
        return not self.run2.is_empty

    def effective_total(self) -> ParsedValue:
        # Any status in runs must dominate the total result.
        if self.run2.is_status:
            return self.run2
        if self.run1.is_status:
            return self.run1
        if self.total.is_status:
            return self.total
        if self.total.is_time:
            return self.total
        if self.run1.is_time and self.run2.is_time and self.run1.seconds is not None and self.run2.seconds is not None:
            summed = self.run1.seconds + self.run2.seconds
            return ParsedValue(raw=format_seconds(summed), value_type="time", seconds=summed)
        return EMPTY_VALUE

    def is_finished(self) -> bool:
        # Group completion is allowed by run2 value OR final status.
        return self.has_second_run_result() or self.effective_total().is_status

    def last_passed_track(self) -> ParsedValue:
        if not self.run2.is_empty:
            return self.run2
        return self.run1

    def ranking_value(self) -> ParsedValue:
        if self.has_second_run_result():
            return self.effective_total()
        return self.run1


@dataclass(frozen=True)
class GroupBlock:
    group_key: str
    sheet_name: str
    group_name: str
    athletes: tuple[AthleteRow, ...]

    def started(self) -> bool:
        return any(athlete.has_any_progress() for athlete in self.athletes)

    def completed(self) -> bool:
        if not self.started():
            return False
        return all(athlete.is_finished() for athlete in self.athletes)


def format_seconds(value: float | None) -> str:
    if value is None:
        return ""
    minutes = int(value // 60)
    seconds = value - (minutes * 60)
    if minutes > 0:
        return f"{minutes}:{seconds:05.2f}"
    return f"{seconds:.2f}"
