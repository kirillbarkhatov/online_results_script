from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta
import math
from typing import Literal

from .models import AthleteRow, GroupBlock, ParsedValue, format_seconds


ANSI_RESET = "\033[0m"
ANSI_KANAEV_ROW = "\033[48;5;25m\033[97m"
ANSI_JUDGE_NOTE_ROW = "\033[48;5;226m\033[30m"


SheetPhase = Literal["not_started", "run1", "break_after_run1", "run2", "completed"]
AUTO_DNS_VALUE = ParsedValue(raw="DNS", value_type="status", status="DNS")


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

    def __init__(self, finalize_timeout_sec: int = 300, finalize_max_missing: int = 2) -> None:
        self.completed_groups = set()
        self._result_seen_at: dict[tuple[str, int, str], datetime] = {}
        self._group_last_update_at: dict[str, datetime] = {}
        self._group_started_at: dict[str, datetime] = {}
        self._auto_finalized_groups: set[str] = set()
        self._finalize_timeout = timedelta(seconds=max(finalize_timeout_sec, 0))
        self._finalize_max_missing = max(finalize_max_missing, 0)
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

    def register_result_updates(self, changed_athletes: list[AthleteRow], now: datetime) -> None:
        # Track only truly new updates after script start.
        # Existing values from initial snapshot must not distort pace model.
        for athlete in changed_athletes:
            self._group_last_update_at[f"{athlete.sheet_name}|{athlete.group_name}"] = now
            if not athlete.run1.is_empty:
                key = (athlete.sheet_name, 1, athlete.athlete_key)
                self._result_seen_at.setdefault(key, now)
            if not athlete.run2.is_empty:
                key = (athlete.sheet_name, 2, athlete.athlete_key)
                self._result_seen_at.setdefault(key, now)

    def apply_auto_finalize(self, groups: tuple[GroupBlock, ...], now: datetime) -> tuple[GroupBlock, ...]:
        indexed = list(enumerate(groups))
        started_run1_by_index = {
            idx: any(not athlete.run1.is_empty for athlete in group.athletes)
            for idx, group in indexed
        }
        started_run2_by_index = {
            idx: any(not athlete.run2.is_empty for athlete in group.athletes)
            for idx, group in indexed
        }

        result: list[GroupBlock] = []
        for idx, group in indexed:
            if group.started():
                self._group_started_at.setdefault(group.group_key, now)
            if group.completed():
                result.append(group)
                continue

            phase = group_phase(group)
            if phase == "run1":
                run_number = 1
                missing = [athlete for athlete in group.athletes if athlete.run1.is_empty]
            elif phase == "run2":
                run_number = 2
                missing = [
                    athlete
                    for athlete in group.athletes
                    if (not athlete.run1.is_empty) and _is_eligible_for_run2(athlete) and athlete.run2.is_empty
                ]
            else:
                result.append(group)
                continue

            if not missing:
                result.append(group)
                continue

            missing_count = len(missing)
            by_next_started = self._has_next_started_group(
                indexed=indexed,
                current_index=idx,
                run_number=run_number,
                started_run1_by_index=started_run1_by_index,
                started_run2_by_index=started_run2_by_index,
            )
            by_timeout = False
            if missing_count <= self._finalize_max_missing:
                last_touch = self._group_last_update_at.get(group.group_key) or self._group_started_at.get(group.group_key)
                if last_touch is not None:
                    by_timeout = (now - last_touch) >= self._finalize_timeout

            can_finalize_by_progress = missing_count <= self._finalize_max_missing
            if (by_next_started and can_finalize_by_progress) or by_timeout:
                self._auto_finalized_groups.add(f"{group.group_key}|run{run_number}")
                result.append(_finalize_group_with_dns(group, run_number=run_number))
                continue

            result.append(group)

        return tuple(result)

    def _has_next_started_group(
        self,
        indexed: list[tuple[int, GroupBlock]],
        current_index: int,
        run_number: int,
        started_run1_by_index: dict[int, bool],
        started_run2_by_index: dict[int, bool],
    ) -> bool:
        for idx, group in indexed:
            if idx <= current_index:
                continue
            if run_number == 1 and started_run1_by_index.get(idx, False):
                return True
            if run_number == 2 and started_run2_by_index.get(idx, False):
                return True
        return False

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
        if run_number == 2 and not _is_eligible_for_run2(target):
            return None

        athletes_for_queue = (
            tuple(athlete for athlete in sheet_athletes if _is_eligible_for_run2(athlete))
            if run_number == 2
            else sheet_athletes
        )
        waiting = [
            athlete
            for athlete in athletes_for_queue
            if (run_number == 1 and athlete.run1.is_empty) or (run_number == 2 and athlete.run2.is_empty)
        ]
        queue_index = next((index for index, athlete in enumerate(waiting) if athlete.athlete_key == athlete_key), None)
        if queue_index is None:
            return None

        valid_time_values = [
            athlete.run1.seconds
            for athlete in athletes_for_queue
            if run_number == 1 and athlete.run1.is_time and athlete.run1.seconds is not None
        ]
        if run_number == 2:
            valid_time_values = [
                athlete.run2.seconds
                for athlete in athletes_for_queue
                if athlete.run2.is_time and athlete.run2.seconds is not None
            ]

        if not valid_time_values:
            return None

        # Initial tempo model at run start:
        # first valid time / 2 (parallel occupancy on track).
        first_valid_time = valid_time_values[0]
        base_tempo_seconds = first_valid_time / 2.0

        # Simplified model near start window (<5 athletes before target):
        # current average run time / 2.
        if queue_index < 5:
            avg_run_time = sum(valid_time_values) / len(valid_time_values)
            tempo_seconds = avg_run_time / 2.0
            seconds_until = max(tempo_seconds * queue_index, 0.0)
            return now + timedelta(seconds=seconds_until)

        completed = [
            athlete
            for athlete in athletes_for_queue
            if (run_number == 1 and not athlete.run1.is_empty) or (run_number == 2 and not athlete.run2.is_empty)
        ]
        seen_times = [
            self._result_seen_at.get((sheet_name, run_number, athlete.athlete_key))
            for athlete in completed
        ]
        known = sorted(ts for ts in seen_times if ts is not None)
        tempo_seconds = base_tempo_seconds

        if len(known) >= 3:
            intervals = [
                (known[index + 1] - known[index]).total_seconds()
                for index in range(len(known) - 1)
            ]
            # Filter out operator pauses / batch input spikes.
            positive_intervals = [
                interval
                for interval in intervals
                if (interval > 0.1) and (interval <= base_tempo_seconds * 2.5)
            ]
            if positive_intervals:
                observed_interval = _median(sorted(positive_intervals))
                # Keep observed pace as correction around base model, not full replacement.
                lower = base_tempo_seconds * 0.55
                upper = base_tempo_seconds * 1.60
                observed_interval = min(max(observed_interval, lower), upper)
                # Gradually enrich base model with observed pace.
                observed_weight = min(len(positive_intervals), 6) / 6.0
                tempo_seconds = (base_tempo_seconds * (1.0 - observed_weight)) + (observed_interval * observed_weight)

        seconds_until = max(tempo_seconds * queue_index, 0.0)
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


def group_phase(group: GroupBlock) -> SheetPhase:
    run1_started = any(not athlete.run1.is_empty for athlete in group.athletes)
    run1_completed = all(not athlete.run1.is_empty for athlete in group.athletes) if group.athletes else False
    run2_started = any(not athlete.run2.is_empty for athlete in group.athletes)
    completed = all(athlete.is_finished() for athlete in group.athletes) if group.athletes else False

    if completed:
        return "completed"
    if run2_started:
        return "run2"
    if run1_started and run1_completed:
        return "break_after_run1"
    if run1_started:
        return "run1"
    return "not_started"


def _finalize_group_with_dns(group: GroupBlock, run_number: int) -> GroupBlock:
    finalized: list[AthleteRow] = []
    for athlete in group.athletes:
        should_finalize = False
        if run_number == 1:
            should_finalize = athlete.run1.is_empty
        elif run_number == 2:
            should_finalize = (not athlete.run1.is_empty) and _is_eligible_for_run2(athlete) and athlete.run2.is_empty

        if not should_finalize:
            finalized.append(athlete)
            continue

        run1 = athlete.run1
        run2 = athlete.run2
        if run_number == 1 and run1.is_empty:
            run1 = AUTO_DNS_VALUE
        elif run_number == 2 and run2.is_empty:
            run2 = AUTO_DNS_VALUE

        auto_note = "автофинализация: DNS"
        judge_note = f"{athlete.judge_note}; {auto_note}" if athlete.judge_note else auto_note
        finalized.append(
            AthleteRow(
                athlete_key=athlete.athlete_key,
                sheet_name=athlete.sheet_name,
                group_name=athlete.group_name,
                sheet_row=athlete.sheet_row,
                start_number=athlete.start_number,
                full_name=athlete.full_name,
                club=athlete.club,
                run1=run1,
                run2=run2,
                total=athlete.total,
                runs_count=athlete.runs_count,
                event_name=athlete.event_name,
                event_date=athlete.event_date,
                judge_note=judge_note,
            )
        )

    return GroupBlock(
        group_key=group.group_key,
        sheet_name=group.sheet_name,
        group_name=group.group_name,
        athletes=tuple(finalized),
    )


def render_change_lines(changed_athletes: list[AthleteRow]) -> list[str]:
    lines: list[str] = []
    for athlete in sorted(changed_athletes, key=lambda item: (item.sheet_name, item.group_name, item.start_number)):
        last_passed = athlete.last_passed_track()
        if last_passed.is_empty:
            continue
        note_suffix = f" | пометка судьи: {athlete.judge_note}" if athlete.judge_note else ""
        line = (
            f"{athlete.full_name} | {athlete.club} | последний заезд: {last_passed.to_display()} "
            f"| итог: {athlete.effective_total().to_display() or '-'}{note_suffix}"
        )
        lines.append(_highlight_row(athlete, line))
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
    rows: list[tuple[AthleteRow, tuple[str, str, str, str, str, str, str]]] = []

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
                athlete,
                (
                str(athlete.start_number),
                athlete.full_name,
                athlete.run1.to_display() or "-",
                athlete.run2.to_display() or "-",
                athlete.effective_total().to_display() or "-",
                place_by_key.get(athlete.athlete_key, "-"),
                forecast,
                ),
            )
        )

    widths = [len(header) for header in headers]
    for _athlete, row in rows:
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
    for athlete, row in rows:
        lines.append(_highlight_row(athlete, _fmt(row)))
    return lines


def render_group_table(group: GroupBlock, header: str, analytics: GroupAnalytics | None = None) -> list[str]:
    ranking = rank_group(group.athletes)
    extra_headers = analytics.headers if analytics else tuple()
    rows: list[tuple[AthleteRow, tuple[str, ...]]] = []

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
        rows.append((athlete, tuple(base_row)))

    headers: tuple[str, ...] = ("место", "ст.№", "ФИО", "клуб", "1 заезд", "2 заезд", "итог", "интервал") + extra_headers
    widths = [len(h) for h in headers]
    for _athlete, row in rows:
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
    for athlete, row in rows:
        lines.append(_highlight_row(athlete, _fmt_row(row)))
    return lines


def build_group_analytics(group: GroupBlock, sheet_phase: SheetPhase) -> GroupAnalytics | None:
    if _is_single_run_group(group):
        return None
    if sheet_phase == "break_after_run1":
        return _build_run1_gap_analytics(group)
    if sheet_phase == "completed":
        return _build_run2_analytics(group)
    return None


def _build_run1_gap_analytics(group: GroupBlock) -> GroupAnalytics | None:
    if not all(not athlete.run1.is_empty for athlete in group.athletes):
        return None

    sigma = _estimate_run2_sigma(group.athletes)
    run1_ranked_times = sorted(
        [
            athlete
            for athlete in group.athletes
            if athlete.run1.is_time and athlete.run1.seconds is not None
        ],
        key=lambda athlete: (athlete.run1.seconds if athlete.run1.seconds is not None else float("inf"), athlete.start_number),
    )
    place_by_key = {
        athlete.athlete_key: place
        for place, athlete in enumerate(run1_ranked_times, start=1)
    }
    values_by_athlete: dict[str, tuple[str, ...]] = {}

    for athlete in group.athletes:
        if not athlete.run1.is_time or athlete.run1.seconds is None:
            note_suffix = _judge_note_text(athlete)
            values_by_athlete[athlete.athlete_key] = (f"STATUS{note_suffix}",)
            continue
        segment = _run1_battle_segment(
            athlete=athlete,
            run1_ranked=run1_ranked_times,
            place=place_by_key.get(athlete.athlete_key, 0),
            sigma=sigma,
        )
        note_suffix = _judge_note_text(athlete)
        values_by_athlete[athlete.athlete_key] = (f"{segment}{note_suffix}",)

    return GroupAnalytics(headers=("сегмент1",), values_by_athlete=values_by_athlete)


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
        delta_display = f"{delta:+d}" if delta is not None else "-"

        include = (
            athlete.athlete_key in top6_run1
            or athlete.athlete_key in top10_run2
            or athlete.athlete_key in kanaev_team
            or (delta is not None and abs(delta) > 7)
        )
        note_text = _judge_note_text(athlete)
        if not include:
            values_by_athlete[athlete.athlete_key] = (
                str(p1) if p1 is not None else "-",
                delta_display,
                note_text.lstrip("; ") if note_text else "-",
            )
            continue

        if athlete.effective_total().is_status:
            values_by_athlete[athlete.athlete_key] = (
                str(p1) if p1 is not None else "-",
                delta_display,
                note_text.lstrip("; ") if note_text else "-",
            )
            continue

        if not athlete.has_second_run_result():
            base_wait = "Ожидает 2 заезд"
            values_by_athlete[athlete.athlete_key] = (
                str(p1) if p1 is not None else "-",
                delta_display,
                f"{base_wait}{note_text}",
            )
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

        values_by_athlete[athlete.athlete_key] = (
            str(p1) if p1 is not None else "-",
            delta_display,
            f"{'; '.join(notes)}{note_text}",
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
    club_labels: dict[str, str] = {}

    for athlete in group.athletes:
        club_key = _normalize_club_key(athlete.club)
        club_labels[club_key] = _preferred_club_label(club_labels.get(club_key), athlete.club)
        club_stats = stats[club_key]
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

    display_stats = {club_labels[key]: value for key, value in stats.items()}
    return _render_club_stats_table(
        title=f"Статистика по клубам: {group.sheet_name} -> {group.group_name}",
        stats=display_stats,
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
            "top10_finishers": 0,
            "top10_sum_time": 0.0,
            "top25pct": 0,
            "top50pct": 0,
        }
    )
    club_labels: dict[str, str] = {}
    sheet_finish_stats: dict[tuple[str, str], dict[str, float | int]] = defaultdict(
        lambda: {"sum_time": 0.0, "count": 0}
    )
    sheet_top10_stats: dict[tuple[str, str], dict[str, float | int]] = defaultdict(
        lambda: {"sum_time": 0.0, "count": 0}
    )

    for group in groups:
        ranking = rank_group(group.athletes)
        place_by_key = {athlete.athlete_key: place for place, athlete, _ in ranking}

        for athlete in group.athletes:
            club_key = _normalize_club_key(athlete.club)
            club_labels[club_key] = _preferred_club_label(club_labels.get(club_key), athlete.club)
            club_stats = stats[club_key]
            club_stats["participants"] += 1

            final_value = athlete.effective_total()
            if final_value.is_time and final_value.seconds is not None:
                club_stats["finishers"] += 1
                club_stats["sum_time"] += final_value.seconds
                key = (club_key, group.sheet_name)
                sheet_finish_stats[key]["sum_time"] += final_value.seconds
                sheet_finish_stats[key]["count"] += 1
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
                if final_value.is_time and final_value.seconds is not None:
                    club_stats["top10_finishers"] += 1
                    club_stats["top10_sum_time"] += final_value.seconds
                    key = (club_key, group.sheet_name)
                    sheet_top10_stats[key]["sum_time"] += final_value.seconds
                    sheet_top10_stats[key]["count"] += 1

        # Group-relative bands among finishers only.
        finishers_ranked = [
            athlete
            for _, athlete, _ in ranking
            if athlete.effective_total().is_time and athlete.effective_total().seconds is not None
        ]
        fin_count = len(finishers_ranked)
        if fin_count > 0:
            top25_limit = max(1, math.ceil(fin_count * 0.25))
            top50_limit = max(1, math.ceil(fin_count * 0.50))
            for index, athlete in enumerate(finishers_ranked, start=1):
                club_key = _normalize_club_key(athlete.club)
                if index <= top25_limit:
                    stats[club_key]["top25pct"] += 1
                if index <= top50_limit:
                    stats[club_key]["top50pct"] += 1

    display_stats = {club_labels[key]: value for key, value in stats.items()}
    avg_time_by_club_sheet: dict[str, float] = {}
    avg_time_per_sheet_by_club: dict[str, dict[str, float]] = {}

    for club_key, label in club_labels.items():
        sheet_avgs: list[float] = []
        top10_sheet_avgs: list[float] = []

        for (key_club, _sheet_name), values in sheet_finish_stats.items():
            if key_club != club_key:
                continue
            count = int(values["count"])
            if count > 0:
                sheet_avgs.append(float(values["sum_time"]) / count)

        for (key_club, _sheet_name), values in sheet_top10_stats.items():
            if key_club != club_key:
                continue
            count = int(values["count"])
            if count > 0:
                top10_sheet_avgs.append(float(values["sum_time"]) / count)

        if sheet_avgs:
            avg_time_by_club_sheet[label] = sum(sheet_avgs) / len(sheet_avgs)
            per_sheet_items: list[tuple[str, float]] = []
            for (key_club, sheet_name), values in sheet_finish_stats.items():
                if key_club != club_key:
                    continue
                count = int(values["count"])
                if count <= 0:
                    continue
                per_sheet_items.append((sheet_name, float(values["sum_time"]) / count))
            per_sheet_items.sort(key=lambda item: item[0].lower())
            avg_time_per_sheet_by_club[label] = {sheet_name: avg_value for sheet_name, avg_value in per_sheet_items}
        if top10_sheet_avgs:
            # kept for potential further scoring/model updates
            _ = sum(top10_sheet_avgs) / len(top10_sheet_avgs)

    return _render_overall_club_stats_table(
        title="Сводная статистика по всем клубам",
        stats=display_stats,
        avg_time_by_club_sheet=avg_time_by_club_sheet,
        avg_time_per_sheet_by_club=avg_time_per_sheet_by_club,
    )


def rank_group(athletes: tuple[AthleteRow, ...]) -> list[tuple[int, AthleteRow, float | None]]:
    second_run_phase = any(athlete.has_second_run_result() for athlete in athletes)
    if second_run_phase:
        second_run_done = [athlete for athlete in athletes if athlete.has_second_run_result()]
        second_run_waiting = [athlete for athlete in athletes if not athlete.has_second_run_result()]
        ranked = sorted(second_run_done, key=lambda athlete: (athlete.ranking_value().sort_key(), athlete.start_number))
        # During run2, keep waiting athletes in the same order as in the source Google sheet.
        ranked.extend(sorted(second_run_waiting, key=lambda athlete: (athlete.sheet_row, athlete.start_number)))
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
        return after.has_any_progress() or bool(after.judge_note)
    return (
        (before.run1.raw != after.run1.raw)
        or (before.run2.raw != after.run2.raw)
        or (before.effective_total().raw != after.effective_total().raw)
        or (before.judge_note != after.judge_note)
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


def _estimate_run2_sigma(athletes: tuple[AthleteRow, ...]) -> float:
    times = sorted(
        athlete.run1.seconds
        for athlete in athletes
        if athlete.run1.is_time and athlete.run1.seconds is not None
    )
    if len(times) < 4:
        return 0.80
    median = _median(times)
    deviations = sorted(abs(value - median) for value in times)
    mad = _median(deviations)
    sigma = 1.4826 * mad
    return min(max(sigma, 0.35), 1.80)


def _run1_battle_segment(
    athlete: AthleteRow,
    run1_ranked: list[AthleteRow],
    place: int,
    sigma: float,
) -> str:
    if not athlete.run1.is_time or athlete.run1.seconds is None or place <= 0:
        return "STATUS"
    if len(run1_ranked) < 3:
        return "Борьба не определена"

    index = place - 1
    athlete_time = athlete.run1.seconds

    ahead = run1_ranked[:index]
    behind = run1_ranked[index + 1 :]

    gain_score = 0.0
    for rival in ahead[-5:]:
        if rival.run1.seconds is None:
            continue
        gap = athlete_time - rival.run1.seconds
        gain_score += _win_probability(gap, sigma)

    loss_score = 0.0
    for rival in behind[:5]:
        if rival.run1.seconds is None:
            continue
        gap = rival.run1.seconds - athlete_time
        loss_score += _win_probability(gap, sigma)

    net_shift = gain_score - loss_score
    likely_place = max(1, min(len(run1_ranked), int(round(place - net_shift))))

    gap_up: float | None = None
    nearest_up_prob = 0.0
    if index > 0 and run1_ranked[index - 1].run1.seconds is not None:
        gap_up = athlete_time - float(run1_ranked[index - 1].run1.seconds)
        nearest_up_prob = _win_probability(gap_up, sigma)

    gap_down: float | None = None
    nearest_down_prob = 0.0
    if index < len(run1_ranked) - 1 and run1_ranked[index + 1].run1.seconds is not None:
        gap_down = float(run1_ranked[index + 1].run1.seconds) - athlete_time
        nearest_down_prob = _win_probability(gap_down, sigma)

    tight_gap = max(1.0, 1.1 * sigma)
    comfort_gap = max(1.8, 2.2 * sigma)
    dominant_gap = max(2.8, 3.0 * sigma)
    is_pressure = (gap_down is not None and gap_down <= tight_gap) or nearest_down_prob >= 0.42
    is_comfort = (gap_down is not None and gap_down >= comfort_gap) and nearest_down_prob <= 0.28

    if place == 1:
        if gap_down is not None and gap_down >= dominant_gap and nearest_down_prob <= 0.15:
            base = "Лидер с комфортным преимуществом"
        elif is_pressure:
            base = "Лидер под давлением"
        else:
            base = "Лидер с рабочим запасом"
    elif place <= 3:
        if is_comfort:
            base = "Подиум: комфортное преимущество"
        elif net_shift >= 0.4 and gap_up is not None and gap_up <= comfort_gap:
            base = "Подиум: шанс усилить позицию"
        elif is_pressure or loss_score >= 0.8:
            base = "Подиум: риск потери"
        else:
            base = "Подиум: стабильная позиция"
    elif place <= 10:
        if is_pressure:
            base = "Топ-10: зона давления"
        elif is_comfort:
            base = "Топ-10: позиция с запасом"
        elif net_shift >= 0.8 and gap_up is not None and gap_up <= comfort_gap:
            base = "Топ-10: шанс отыгрыша"
        else:
            base = "Топ-10: стабильная зона"
    else:
        if is_comfort:
            base = "Стабильная позиция с запасом"
        elif net_shift >= 1.2 and gap_up is not None and gap_up <= comfort_gap:
            base = "Высокий шанс отыгрыша"
        elif net_shift >= 0.5 and gap_up is not None and gap_up <= comfort_gap:
            base = "Вероятный отыгрыш"
        elif is_pressure or loss_score >= 1.2:
            base = "Риск потери позиций"
        else:
            base = "Стабильная позиция"

    return (
        f"{base}; прогн.место: {likely_place}; "
        f"P↑:{nearest_up_prob * 100:.0f}% P↓:{nearest_down_prob * 100:.0f}%"
    )


def _win_probability(gap_seconds: float, sigma: float) -> float:
    sigma = max(sigma, 0.10)
    z = gap_seconds / (2.0 * sigma)
    return 0.5 * math.erfc(z)


def _median(values: list[float]) -> float:
    if not values:
        return 0.0
    middle = len(values) // 2
    if len(values) % 2 == 1:
        return values[middle]
    return (values[middle - 1] + values[middle]) / 2.0


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
    for row in rows:
        lines.append(_highlight_if_kanaev(row[0], _fmt(row)))
    return lines


def _render_overall_club_stats_table(
    title: str,
    stats: dict[str, dict[str, float | int]],
    avg_time_by_club_sheet: dict[str, float] | None = None,
    avg_time_per_sheet_by_club: dict[str, dict[str, float]] | None = None,
) -> list[str]:
    sheet_columns: list[str] = []
    if avg_time_per_sheet_by_club:
        all_sheets = {sheet_name for by_sheet in avg_time_per_sheet_by_club.values() for sheet_name in by_sheet.keys()}
        sheet_columns = sorted(all_sheets, key=str.lower)

    headers = (
        "клуб",
        "уч.",
        "финиш",
        "DNS",
        "DNF",
        "DSQ",
        "топ3",
        "топ10",
        "топ25%",
        "топ50%",
    ) + tuple(f"ср. {sheet_name}" for sheet_name in sheet_columns)
    headers = headers + ("скоринг",)
    rows: list[tuple[str, ...]] = []

    total_participants = sum(int(data["participants"]) for data in stats.values())
    total_finishers = sum(int(data["finishers"]) for data in stats.values())
    total_top3 = sum(int(data["top3"]) for data in stats.values())
    total_top10 = sum(int(data["top10"]) for data in stats.values())
    total_top25pct = sum(int(data["top25pct"]) for data in stats.values())
    total_top50pct = sum(int(data["top50pct"]) for data in stats.values())
    total_dns = sum(int(data["dns"]) for data in stats.values())
    total_dnf = sum(int(data["dnf"]) for data in stats.values())
    total_dsq = sum(int(data["dsq"]) for data in stats.values())
    global_finish_rate = (total_finishers / total_participants) if total_participants else 0.0
    global_top3_rate = (total_top3 / total_participants) if total_participants else 0.0
    global_top10_rate = (total_top10 / total_participants) if total_participants else 0.0
    global_top25pct_rate = (total_top25pct / total_finishers) if total_finishers else 0.0
    global_top50pct_rate = (total_top50pct / total_finishers) if total_finishers else 0.0
    global_dns_rate = (total_dns / total_participants) if total_participants else 0.0
    global_dnf_rate = (total_dnf / total_participants) if total_participants else 0.0
    global_dsq_rate = (total_dsq / total_participants) if total_participants else 0.0

    avg_by_club: dict[str, float] = {}
    best_by_sheet: dict[str, float] = {}
    for club, data in stats.items():
        finishers = int(data["finishers"])
        if avg_time_by_club_sheet and club in avg_time_by_club_sheet:
            avg_by_club[club] = avg_time_by_club_sheet[club]
        elif finishers > 0:
            avg_by_club[club] = float(data["sum_time"]) / finishers
    best_avg = min(avg_by_club.values()) if avg_by_club else None
    if avg_time_per_sheet_by_club:
        for sheet_name in sheet_columns:
            values = [
                by_sheet[sheet_name]
                for by_sheet in avg_time_per_sheet_by_club.values()
                if sheet_name in by_sheet
            ]
            if values:
                best_by_sheet[sheet_name] = min(values)

    scored: list[tuple[str, tuple[str, ...]]] = []
    for club, data in stats.items():
        participants = int(data["participants"])
        finishers = int(data["finishers"])
        dns = int(data["dns"])
        dnf = int(data["dnf"])
        dsq = int(data["dsq"])
        top3 = int(data["top3"])
        top10 = int(data["top10"])
        top25pct = int(data["top25pct"])
        top50pct = int(data["top50pct"])

        finish_rate = (finishers / participants) if participants else 0.0
        podium_rate = (top3 / participants) if participants else 0.0
        top10_rate = (top10 / participants) if participants else 0.0
        if best_avg is not None and club in avg_by_club and avg_by_club[club] > 0:
            speed_index = best_avg / avg_by_club[club]
        else:
            speed_index = 0.0

        sheet_cells: list[str] = []
        for sheet_name in sheet_columns:
            value = None
            if avg_time_per_sheet_by_club and club in avg_time_per_sheet_by_club:
                value = avg_time_per_sheet_by_club[club].get(sheet_name)
            if value is None:
                sheet_cells.append("-")
                continue
            best_sheet = best_by_sheet.get(sheet_name)
            if best_sheet and best_sheet > 0:
                rel = (best_sheet / value) * 100.0
                sheet_cells.append(f"{format_seconds(value)} ({rel:.1f}%)")
            else:
                sheet_cells.append(format_seconds(value))

        sheet_tempo_values: list[float] = []
        if avg_time_per_sheet_by_club and club in avg_time_per_sheet_by_club:
            for sheet_name in sheet_columns:
                value = avg_time_per_sheet_by_club[club].get(sheet_name)
                best_sheet = best_by_sheet.get(sheet_name)
                if value is None or best_sheet is None or value <= 0:
                    continue
                sheet_tempo_values.append(best_sheet / value)
        if sheet_tempo_values:
            tempo_index = sum(sheet_tempo_values) / len(sheet_tempo_values)
        else:
            tempo_index = speed_index

        # Empirical-Bayes smoothing reduces sample-size volatility.
        prior_strength = 12.0
        smoothed_finish_rate = ((finishers + (prior_strength * global_finish_rate)) / (participants + prior_strength)) if (participants + prior_strength) > 0 else 0.0
        smoothed_podium_rate = ((top3 + (prior_strength * global_top3_rate)) / (participants + prior_strength)) if (participants + prior_strength) > 0 else 0.0
        smoothed_top10_rate = ((top10 + (prior_strength * global_top10_rate)) / (participants + prior_strength)) if (participants + prior_strength) > 0 else 0.0
        smoothed_dns_rate = ((dns + (prior_strength * global_dns_rate)) / (participants + prior_strength)) if (participants + prior_strength) > 0 else 0.0
        smoothed_dnf_rate = ((dnf + (prior_strength * global_dnf_rate)) / (participants + prior_strength)) if (participants + prior_strength) > 0 else 0.0
        smoothed_dsq_rate = ((dsq + (prior_strength * global_dsq_rate)) / (participants + prior_strength)) if (participants + prior_strength) > 0 else 0.0

        finish_prior_strength = 10.0
        smoothed_top25pct_rate = ((top25pct + (finish_prior_strength * global_top25pct_rate)) / (finishers + finish_prior_strength)) if (finishers + finish_prior_strength) > 0 else 0.0
        smoothed_top50pct_rate = ((top50pct + (finish_prior_strength * global_top50pct_rate)) / (finishers + finish_prior_strength)) if (finishers + finish_prior_strength) > 0 else 0.0

        speed_prior_strength = 8.0
        smoothed_speed_index = (
            ((tempo_index * finishers) + speed_prior_strength) / (finishers + speed_prior_strength)
            if (finishers + speed_prior_strength) > 0
            else 0.0
        )

        # Composite quality score with stronger emphasis on depth (top50/top25) and stability penalties.
        base_score = 100.0 * (
            0.20 * smoothed_top10_rate
            + 0.15 * smoothed_podium_rate
            + 0.18 * smoothed_top25pct_rate
            + 0.16 * smoothed_top50pct_rate
            + 0.17 * smoothed_finish_rate
            + 0.10 * smoothed_speed_index
            - 0.02 * smoothed_dns_rate
            - 0.015 * smoothed_dnf_rate
            - 0.005 * smoothed_dsq_rate
        )
        base_score = min(max(base_score, 0.0), 100.0)

        # Reliability compresses extremes for very small teams.
        reliability = math.sqrt(participants / (participants + prior_strength)) if participants > 0 else 0.0
        score = 50.0 + ((base_score - 50.0) * reliability)

        score_strength = min(max(score / 100.0, 0.0), 1.0)
        score_display = f"{score:.1f} ({score_strength * 100:.1f}%)"

        row = (
            club,
            str(participants),
            f"{finishers} ({finish_rate * 100:.1f}%)",
            f"{dns} ({(dns / participants * 100.0) if participants else 0.0:.1f}%)",
            f"{dnf} ({(dnf / participants * 100.0) if participants else 0.0:.1f}%)",
            f"{dsq} ({(dsq / participants * 100.0) if participants else 0.0:.1f}%)",
            f"{top3} ({(top3 / participants * 100.0) if participants else 0.0:.1f}%)",
            f"{top10} ({(top10 / participants * 100.0) if participants else 0.0:.1f}%)",
            f"{top25pct} ({(top25pct / finishers * 100.0) if finishers else 0.0:.1f}%)",
            f"{top50pct} ({(top50pct / finishers * 100.0) if finishers else 0.0:.1f}%)",
        ) + tuple(sheet_cells) + (score_display,)
        scored.append((club, row))

    for _, row in sorted(scored, key=lambda item: item[0].lower()):
        rows.append(row)

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
    for row in rows:
        lines.append(_highlight_if_kanaev(row[0], _fmt(row)))
    lines.append("")
    lines.append("Скоринг: 20% топ10 + 15% топ3 + 18% топ25% + 16% топ50% + 17% финиш + 10% темп - штраф DNS/DNF/DSQ.")
    lines.append("Используется сглаживание (Bayes) и коэффициент надежности по размеру команды.")
    return lines


def _judge_note_text(athlete: AthleteRow) -> str:
    if not athlete.judge_note:
        return ""
    return f"; Пометка судьи: {athlete.judge_note}"


def _highlight_row(athlete: AthleteRow, text: str) -> str:
    if athlete.judge_note:
        return f"{ANSI_JUDGE_NOTE_ROW}{text}{ANSI_RESET}"
    return _highlight_if_kanaev(athlete.club, text)


def _highlight_if_kanaev(club: str, text: str) -> str:
    normalized = " ".join(club.lower().split())
    if ("канаев" not in normalized) and ("kanaev" not in normalized):
        return text
    return f"{ANSI_KANAEV_ROW}{text}{ANSI_RESET}"


def _is_kanaev_club(club: str) -> bool:
    normalized = " ".join(club.lower().split())
    return ("канаев" in normalized) or ("kanaev" in normalized)


def _normalize_club_key(club: str) -> str:
    return " ".join(club.lower().split())


def _preferred_club_label(current: str | None, candidate: str) -> str:
    normalized = " ".join(candidate.split())
    if not current:
        return normalized
    if current.isupper() and not normalized.isupper():
        return normalized
    return current


def _is_eligible_for_run2(athlete: AthleteRow) -> bool:
    # Athletes with terminal status in run1 are treated as non-starters for run2 forecast queue.
    return athlete.runs_count > 1 and (not athlete.run1.is_status)


def _is_single_run_group(group: GroupBlock) -> bool:
    return bool(group.athletes) and all(athlete.runs_count <= 1 for athlete in group.athletes)


def _place_word(value: int) -> str:
    value = abs(value)
    if value % 10 == 1 and value % 100 != 11:
        return "место"
    if value % 10 in {2, 3, 4} and value % 100 not in {12, 13, 14}:
        return "места"
    return "мест"
