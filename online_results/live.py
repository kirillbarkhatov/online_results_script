from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from .models import AthleteRow, GroupBlock, format_seconds


ANSI_RESET = "\033[0m"
ANSI_KANAEV_ROW = "\033[48;5;25m\033[97m"


@dataclass
class LiveGroupTracker:
    completed_groups: set[str]

    def __init__(self) -> None:
        self.completed_groups = set()

    def find_current_group(self, groups: tuple[GroupBlock, ...]) -> GroupBlock | None:
        for group in groups:
            if group.group_key in self.completed_groups:
                continue
            if group.started() and not group.completed():
                return group
        return None

    def find_newly_completed_groups(self, groups: tuple[GroupBlock, ...]) -> list[GroupBlock]:
        completed: list[GroupBlock] = []
        for group in groups:
            if group.group_key in self.completed_groups:
                continue
            if group.completed():
                self.completed_groups.add(group.group_key)
                completed.append(group)
        return completed


def render_change_lines(changed_athletes: list[AthleteRow]) -> list[str]:
    lines: list[str] = []
    for athlete in sorted(changed_athletes, key=lambda item: (item.sheet_name, item.group_name, item.start_number)):
        last_passed = athlete.last_passed_track()
        if last_passed.is_empty:
            continue
        line = (
            f"{athlete.full_name} | {athlete.club} | последний заезд: {last_passed.to_display()} "
            f"| итог: {athlete.effective_total().to_display() or '-'}"
        )
        lines.append(_highlight_if_kanaev(athlete.club, line))
    return lines


def render_group_table(group: GroupBlock, header: str) -> list[str]:
    ranking = rank_group(group.athletes)
    rows: list[tuple[str, str, str, str, str, str, str, str]] = []
    for place, athlete, interval in ranking:
        interval_display = f"+{format_seconds(interval)}" if interval is not None else "-"
        rows.append(
            (
                str(place),
                str(athlete.start_number),
                athlete.full_name,
                athlete.club,
                athlete.run1.to_display() or "-",
                athlete.run2.to_display() or "-",
                athlete.effective_total().to_display() or "-",
                interval_display,
            )
        )

    headers = ("место", "ст.№", "ФИО", "клуб", "1 заезд", "2 заезд", "итог", "интервал")
    widths = [len(h) for h in headers]
    for row in rows:
        for index, cell in enumerate(row):
            widths[index] = max(widths[index], len(cell))

    def _fmt_row(cells: tuple[str, str, str, str, str, str, str, str]) -> str:
        return (
            f"{cells[0].rjust(widths[0])} | "
            f"{cells[1].rjust(widths[1])} | "
            f"{cells[2].ljust(widths[2])} | "
            f"{cells[3].ljust(widths[3])} | "
            f"{cells[4].rjust(widths[4])} | "
            f"{cells[5].rjust(widths[5])} | "
            f"{cells[6].rjust(widths[6])} | "
            f"{cells[7].rjust(widths[7])}"
        )

    lines = [
        "",
        f"{header}: {group.sheet_name} -> {group.group_name}",
        _fmt_row(headers),
        "-+-".join("-" * width for width in widths),
    ]
    for row in rows:
        lines.append(_highlight_if_kanaev(row[3], _fmt_row(row)))
    return lines


def rank_group(athletes: tuple[AthleteRow, ...]) -> list[tuple[int, AthleteRow, float | None]]:
    second_run_phase = any(athlete.has_second_run_result() for athlete in athletes)
    if second_run_phase:
        second_run_done = [athlete for athlete in athletes if athlete.has_second_run_result()]
        second_run_waiting = [athlete for athlete in athletes if not athlete.has_second_run_result()]
        ranked = sorted(second_run_done, key=lambda athlete: (athlete.ranking_value().sort_key(), athlete.start_number))
        ranked.extend(sorted(second_run_waiting, key=lambda athlete: (athlete.run1.sort_key(), athlete.start_number)))
    else:
        ranked = sorted(athletes, key=lambda athlete: (athlete.ranking_value().sort_key(), athlete.start_number))
    leader_value = _leader_time(ranked)
    result: list[tuple[int, AthleteRow, float | None]] = []

    for index, athlete in enumerate(ranked, start=1):
        rank_value = athlete.ranking_value()
        interval = None
        if second_run_phase and not athlete.has_second_run_result():
            interval = None
        elif leader_value is not None and rank_value.is_time and rank_value.seconds is not None:
            interval = max(rank_value.seconds - leader_value, 0.0)
        result.append((index, athlete, interval))

    return result


def _leader_time(athletes: list[AthleteRow]) -> float | None:
    for athlete in athletes:
        rank_value = athlete.ranking_value()
        if rank_value.is_time and rank_value.seconds is not None:
            return rank_value.seconds
    return None


def render_tick_header(changed_count: int) -> str:
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return f"\n[{ts}] Изменений: {changed_count}"


def has_result_update(before: AthleteRow | None, after: AthleteRow) -> bool:
    if before is None:
        return after.has_any_progress()
    return (
        (before.run1.raw != after.run1.raw)
        or (before.run2.raw != after.run2.raw)
        or (before.effective_total().raw != after.effective_total().raw)
    )


def _highlight_if_kanaev(club: str, text: str) -> str:
    normalized = " ".join(club.lower().split())
    if "канаев ски клаб" not in normalized:
        return text
    return f"{ANSI_KANAEV_ROW}{text}{ANSI_RESET}"
