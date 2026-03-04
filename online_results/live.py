from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Literal

from .models import AthleteRow, GroupBlock, format_seconds


ANSI_RESET = "\033[0m"
ANSI_KANAEV_ROW = "\033[48;5;25m\033[97m"


SheetPhase = Literal["not_started", "run1", "break_after_run1", "run2", "completed"]


@dataclass(frozen=True)
class SheetProgress:
    sheet_name: str
    athletes: tuple[AthleteRow, ...]
    phase: SheetPhase
    current_run: int | None


@dataclass(frozen=True)
class GroupAnalytics:
    headers: tuple[str, ...]
    values_by_athlete: dict[str, tuple[str, ...]]


@dataclass
class LiveGroupTracker:
    completed_groups: set[str]

    def __init__(self) -> None:
        self.completed_groups = set()
        self._result_seen_at: dict[tuple[str, int, str], datetime] = {}
        self._global_club_stats_printed = False

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

    def register_seen_results(self, groups: tuple[GroupBlock, ...], now: datetime) -> None:
        for group in groups:
            for athlete in group.athletes:
                if not athlete.run1.is_empty:
                    key = (athlete.sheet_name, 1, athlete.athlete_key)
                    self._result_seen_at.setdefault(key, now)
                if not athlete.run2.is_empty:
                    key = (athlete.sheet_name, 2, athlete.athlete_key)
                    self._result_seen_at.setdefault(key, now)

    def estimate_result_time(
        self,
        sheet_athletes: tuple[AthleteRow, ...],
        sheet_name: str,
        run_number: int,
        athlete_key: str,
        now: datetime,
    ) -> datetime | None:
        target = next((athlete for athlete in sheet_athletes if athlete.athlete_key == athlete_key), None)
        if target is None:
            return None
        if run_number == 1 and not target.run1.is_empty:
            return None
        if run_number == 2 and not target.run2.is_empty:
            return None

        completed = [
            athlete
            for athlete in sheet_athletes
            if (run_number == 1 and not athlete.run1.is_empty) or (run_number == 2 and not athlete.run2.is_empty)
        ]
        if len(completed) < 2:
            return None

        seen_times = [
            self._result_seen_at.get((sheet_name, run_number, athlete.athlete_key))
            for athlete in completed
        ]
        known = [ts for ts in seen_times if ts is not None]
        if len(known) < 2:
            return None

        total_interval = (max(known) - min(known)).total_seconds()
        avg_interval = total_interval / (len(known) - 1)
        if avg_interval <= 0:
            return None

        waiting = [
            athlete
            for athlete in sheet_athletes
            if (run_number == 1 and athlete.run1.is_empty) or (run_number == 2 and athlete.run2.is_empty)
        ]
        queue_index = next((index for index, athlete in enumerate(waiting) if athlete.athlete_key == athlete_key), None)
        if queue_index is None:
            return None

        seconds_until = (queue_index + 1) * avg_interval
        return now + timedelta(seconds=seconds_until)

    def should_render_global_club_stats(self, groups: tuple[GroupBlock, ...]) -> bool:
        if self._global_club_stats_printed:
            return False
        if not groups:
            return False
        return all(group.completed() for group in groups)

    def mark_global_club_stats_rendered(self) -> None:
        self._global_club_stats_printed = True


def build_sheet_progress(groups: tuple[GroupBlock, ...]) -> dict[str, SheetProgress]:
    sheet_athletes: dict[str, list[AthleteRow]] = defaultdict(list)
    for group in groups:
        sheet_athletes[group.sheet_name].extend(group.athletes)

    progress: dict[str, SheetProgress] = {}
    for sheet_name, athletes in sheet_athletes.items():
        run1_started = any(not athlete.run1.is_empty for athlete in athletes)
        run1_completed = all(not athlete.run1.is_empty for athlete in athletes) if athletes else False
        run2_started = any(not athlete.run2.is_empty for athlete in athletes)
        sheet_completed = all(athlete.is_finished() for athlete in athletes) if athletes else False

        if sheet_completed:
            phase: SheetPhase = "completed"
            current_run = None
        elif run2_started:
            phase = "run2"
            current_run = 2
        elif run1_started and run1_completed:
            phase = "break_after_run1"
            current_run = None
        elif run1_started:
            phase = "run1"
            current_run = 1
        else:
            phase = "not_started"
            current_run = None

        progress[sheet_name] = SheetProgress(
            sheet_name=sheet_name,
            athletes=tuple(athletes),
            phase=phase,
            current_run=current_run,
        )
    return progress


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


def render_kanaev_sheet_summary(
    sheet_name: str,
    groups: tuple[GroupBlock, ...],
    sheet_progress: SheetProgress,
    tracker: LiveGroupTracker,
    now: datetime,
) -> list[str]:
    kanaev_athletes = [athlete for athlete in sheet_progress.athletes if _is_kanaev_club(athlete.club)]
    if not kanaev_athletes:
        return []

    place_by_key: dict[str, str] = {}
    for group in groups:
        if group.sheet_name != sheet_name:
            continue
        for place, athlete, _ in rank_group(group.athletes):
            place_by_key[athlete.athlete_key] = f"{place} ({group.group_name})"

    headers = ("ст.№", "ФИО", "1 заезд", "2 заезд", "итог", "место", "прогноз")
    rows: list[tuple[str, str, str, str, str, str, str]] = []

    for athlete in kanaev_athletes:
        forecast = "-"
        if sheet_progress.current_run in {1, 2}:
            run_number = int(sheet_progress.current_run)
            run_value = athlete.run1 if run_number == 1 else athlete.run2
            if run_value.is_empty:
                eta = tracker.estimate_result_time(
                    sheet_athletes=sheet_progress.athletes,
                    sheet_name=sheet_name,
                    run_number=run_number,
                    athlete_key=athlete.athlete_key,
                    now=now,
                )
                forecast = eta.strftime("%H:%M:%S") if eta else "н/д"

        rows.append(
            (
                str(athlete.start_number),
                athlete.full_name,
                athlete.run1.to_display() or "-",
                athlete.run2.to_display() or "-",
                athlete.effective_total().to_display() or "-",
                place_by_key.get(athlete.athlete_key, "-"),
                forecast,
            )
        )

    widths = [len(header) for header in headers]
    for row in rows:
        for idx, cell in enumerate(row):
            widths[idx] = max(widths[idx], len(cell))

    def _fmt(cells: tuple[str, str, str, str, str, str, str]) -> str:
        return (
            f"{cells[0].rjust(widths[0])} | "
            f"{cells[1].ljust(widths[1])} | "
            f"{cells[2].rjust(widths[2])} | "
            f"{cells[3].rjust(widths[3])} | "
            f"{cells[4].rjust(widths[4])} | "
            f"{cells[5].rjust(widths[5])} | "
            f"{cells[6].rjust(widths[6])}"
        )

    current_run_label = (
        f"{sheet_progress.current_run} заезд"
        if sheet_progress.current_run in {1, 2}
        else "перерыв"
    )
    lines = [
        "",
        f"Саммари Канаев Ски Клаб: {sheet_name} | текущий заезд: {current_run_label}",
        _fmt(headers),
        "-+-".join("-" * width for width in widths),
    ]
    lines.extend(_fmt(row) for row in rows)
    return lines


def render_group_table(group: GroupBlock, header: str, analytics: GroupAnalytics | None = None) -> list[str]:
    ranking = rank_group(group.athletes)
    extra_headers = analytics.headers if analytics else tuple()
    rows: list[tuple[str, ...]] = []

    for place, athlete, interval in ranking:
        interval_display = f"+{format_seconds(interval)}" if interval is not None else "-"
        base_row: list[str] = [
            str(place),
            str(athlete.start_number),
            athlete.full_name,
            athlete.club,
            athlete.run1.to_display() or "-",
            athlete.run2.to_display() or "-",
            athlete.effective_total().to_display() or "-",
            interval_display,
        ]
        if analytics:
            base_row.extend(analytics.values_by_athlete.get(athlete.athlete_key, tuple("-" for _ in extra_headers)))
        rows.append(tuple(base_row))

    headers: tuple[str, ...] = ("место", "ст.№", "ФИО", "клуб", "1 заезд", "2 заезд", "итог", "интервал") + extra_headers
    widths = [len(h) for h in headers]
    for row in rows:
        for index, cell in enumerate(row):
            widths[index] = max(widths[index], len(cell))

    def _fmt_row(cells: tuple[str, ...]) -> str:
        rendered: list[str] = []
        for index, cell in enumerate(cells):
            if index in {0, 1, 4, 5, 6, 7}:
                rendered.append(cell.rjust(widths[index]))
            else:
                rendered.append(cell.ljust(widths[index]))
        return " | ".join(rendered)

    lines = [
        "",
        f"{header}: {group.sheet_name} -> {group.group_name}",
        _fmt_row(headers),
        "-+-".join("-" * width for width in widths),
    ]
    for row in rows:
        lines.append(_highlight_if_kanaev(row[3], _fmt_row(row)))
    return lines


def build_group_analytics(group: GroupBlock, sheet_phase: SheetPhase) -> GroupAnalytics | None:
    if sheet_phase == "break_after_run1":
        return _build_run1_gap_analytics(group)
    if sheet_phase in {"run2", "completed"}:
        return _build_run2_analytics(group)
    return None


def _build_run1_gap_analytics(group: GroupBlock) -> GroupAnalytics | None:
    if not all(not athlete.run1.is_empty for athlete in group.athletes):
        return None

    run1_times = [athlete.run1.seconds for athlete in group.athletes if athlete.run1.is_time and athlete.run1.seconds is not None]
    leader = min(run1_times) if run1_times else None
    values_by_athlete: dict[str, tuple[str, str]] = {}

    for athlete in group.athletes:
        if leader is None or not athlete.run1.is_time or athlete.run1.seconds is None:
            values_by_athlete[athlete.athlete_key] = ("-", "STATUS")
            continue
        gap = max(athlete.run1.seconds - leader, 0.0)
        values_by_athlete[athlete.athlete_key] = (f"+{format_seconds(gap)}", _run1_segment_label(gap))

    return GroupAnalytics(headers=("отст.1", "сегмент1"), values_by_athlete=values_by_athlete)


def _build_run2_analytics(group: GroupBlock) -> GroupAnalytics:
    run1_ranked = sorted(group.athletes, key=lambda athlete: (athlete.run1.sort_key(), athlete.start_number))
    run1_place = {athlete.athlete_key: index for index, athlete in enumerate(run1_ranked, start=1)}

    run2_ranked = rank_group(group.athletes)
    run2_place = {athlete.athlete_key: place for place, athlete, _ in run2_ranked}

    top6_run1 = {athlete.athlete_key for athlete in run1_ranked[:6]}
    top10_run2 = {athlete.athlete_key for place, athlete, _ in run2_ranked if place <= 10}
    kanaev_team = {athlete.athlete_key for athlete in group.athletes if _is_kanaev_club(athlete.club)}

    best_run1 = _best_time_keys(group.athletes, run_number=1)
    best_run2 = _best_time_keys(group.athletes, run_number=2)

    values_by_athlete: dict[str, tuple[str, str, str]] = {}
    for athlete in group.athletes:
        p1 = run1_place.get(athlete.athlete_key)
        p2 = run2_place.get(athlete.athlete_key)
        delta = (p1 - p2) if p1 is not None and p2 is not None else None

        include = (
            athlete.athlete_key in top6_run1
            or athlete.athlete_key in top10_run2
            or athlete.athlete_key in kanaev_team
            or (delta is not None and abs(delta) > 7)
        )
        if not include:
            values_by_athlete[athlete.athlete_key] = (str(p1) if p1 is not None else "-", "-", "-")
            continue

        if athlete.effective_total().is_status:
            values_by_athlete[athlete.athlete_key] = (str(p1) if p1 is not None else "-", "-", "-")
            continue

        if not athlete.has_second_run_result():
            values_by_athlete[athlete.athlete_key] = (str(p1) if p1 is not None else "-", "-", "Ожидает 2 заезд")
            continue

        notes: list[str] = []
        if p1 == 1 and p2 == 1:
            notes.append("Сохранил лидерство")
        elif delta is not None and delta > 0:
            notes.append(f"Отыграл {delta} {_place_word(delta)}")
        elif delta is not None and delta < 0:
            lost = abs(delta)
            notes.append(f"Потерял {lost} {_place_word(lost)}")
        else:
            notes.append("Удержал позицию")

        if athlete.athlete_key in best_run1:
            notes.append("Лучшее время первого заезда")
        if athlete.athlete_key in best_run2:
            notes.append("Лучшее время второго заезда")

        delta_display = f"{delta:+d}" if delta is not None else "-"
        values_by_athlete[athlete.athlete_key] = (
            str(p1) if p1 is not None else "-",
            delta_display,
            "; ".join(notes),
        )

    return GroupAnalytics(headers=("место1", "Δмест", "аналитика2"), values_by_athlete=values_by_athlete)


def render_group_club_stats(group: GroupBlock) -> list[str]:
    ranking = rank_group(group.athletes)
    place_by_key = {athlete.athlete_key: place for place, athlete, _ in ranking}

    stats: dict[str, dict[str, float | int]] = defaultdict(
        lambda: {
            "participants": 0,
            "finishers": 0,
            "dns": 0,
            "dnf": 0,
            "dsq": 0,
            "sum_time": 0.0,
            "top3": 0,
            "top10": 0,
        }
    )

    for athlete in group.athletes:
        club_stats = stats[athlete.club]
        club_stats["participants"] += 1

        final_value = athlete.effective_total()
        if final_value.is_time and final_value.seconds is not None:
            club_stats["finishers"] += 1
            club_stats["sum_time"] += final_value.seconds
        elif final_value.status == "DNS":
            club_stats["dns"] += 1
        elif final_value.status == "DNF":
            club_stats["dnf"] += 1
        elif final_value.status == "DSQ":
            club_stats["dsq"] += 1

        place = place_by_key.get(athlete.athlete_key)
        if place is not None and place <= 3:
            club_stats["top3"] += 1
        if place is not None and place <= 10:
            club_stats["top10"] += 1

    return _render_club_stats_table(
        title=f"Статистика по клубам: {group.sheet_name} -> {group.group_name}",
        stats=stats,
    )


def render_overall_club_stats(groups: tuple[GroupBlock, ...]) -> list[str]:
    stats: dict[str, dict[str, float | int]] = defaultdict(
        lambda: {
            "participants": 0,
            "finishers": 0,
            "dns": 0,
            "dnf": 0,
            "dsq": 0,
            "sum_time": 0.0,
            "top3": 0,
            "top10": 0,
        }
    )

    for group in groups:
        ranking = rank_group(group.athletes)
        place_by_key = {athlete.athlete_key: place for place, athlete, _ in ranking}

        for athlete in group.athletes:
            club_stats = stats[athlete.club]
            club_stats["participants"] += 1

            final_value = athlete.effective_total()
            if final_value.is_time and final_value.seconds is not None:
                club_stats["finishers"] += 1
                club_stats["sum_time"] += final_value.seconds
            elif final_value.status == "DNS":
                club_stats["dns"] += 1
            elif final_value.status == "DNF":
                club_stats["dnf"] += 1
            elif final_value.status == "DSQ":
                club_stats["dsq"] += 1

            place = place_by_key.get(athlete.athlete_key)
            if place is not None and place <= 3:
                club_stats["top3"] += 1
            if place is not None and place <= 10:
                club_stats["top10"] += 1

    return _render_club_stats_table(title="Сводная статистика по всем клубам", stats=stats)


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


def _best_time_keys(athletes: tuple[AthleteRow, ...], run_number: int) -> set[str]:
    if run_number == 1:
        candidates = [athlete for athlete in athletes if athlete.run1.is_time and athlete.run1.seconds is not None]
        if not candidates:
            return set()
        best = min(athlete.run1.seconds for athlete in candidates if athlete.run1.seconds is not None)
        return {athlete.athlete_key for athlete in candidates if athlete.run1.seconds == best}

    candidates = [athlete for athlete in athletes if athlete.run2.is_time and athlete.run2.seconds is not None]
    if not candidates:
        return set()
    best = min(athlete.run2.seconds for athlete in candidates if athlete.run2.seconds is not None)
    return {athlete.athlete_key for athlete in candidates if athlete.run2.seconds == best}


def _run1_segment_label(gap_seconds: float) -> str:
    if gap_seconds <= 0.50:
        return "0-0.50"
    if gap_seconds <= 1.50:
        return "0.51-1.50"
    if gap_seconds <= 3.00:
        return "1.51-3.00"
    return "3.01+"


def _render_club_stats_table(title: str, stats: dict[str, dict[str, float | int]]) -> list[str]:
    headers = ("клуб", "уч.", "финиш", "DNS", "DNF", "DSQ", "ср.итог", "топ3", "топ10")
    rows: list[tuple[str, ...]] = []

    for club, data in sorted(stats.items(), key=lambda item: item[0].lower()):
        finishers = int(data["finishers"])
        avg = "-"
        if finishers > 0:
            avg_seconds = float(data["sum_time"]) / finishers
            avg = format_seconds(avg_seconds)

        rows.append(
            (
                club,
                str(int(data["participants"])),
                str(finishers),
                str(int(data["dns"])),
                str(int(data["dnf"])),
                str(int(data["dsq"])),
                avg,
                str(int(data["top3"])),
                str(int(data["top10"])),
            )
        )

    widths = [len(header) for header in headers]
    for row in rows:
        for idx, cell in enumerate(row):
            widths[idx] = max(widths[idx], len(cell))

    def _fmt(cells: tuple[str, ...]) -> str:
        rendered: list[str] = []
        for idx, cell in enumerate(cells):
            if idx == 0:
                rendered.append(cell.ljust(widths[idx]))
            else:
                rendered.append(cell.rjust(widths[idx]))
        return " | ".join(rendered)

    lines = ["", title, _fmt(headers), "-+-".join("-" * width for width in widths)]
    lines.extend(_fmt(row) for row in rows)
    return lines


def _highlight_if_kanaev(club: str, text: str) -> str:
    normalized = " ".join(club.lower().split())
    if "канаев ски клаб" not in normalized:
        return text
    return f"{ANSI_KANAEV_ROW}{text}{ANSI_RESET}"


def _is_kanaev_club(club: str) -> bool:
    normalized = " ".join(club.lower().split())
    return "канаев ски клаб" in normalized


def _place_word(value: int) -> str:
    value = abs(value)
    if value % 10 == 1 and value % 100 != 11:
        return "место"
    if value % 10 in {2, 3, 4} and value % 100 not in {12, 13, 14}:
        return "места"
    return "мест"
