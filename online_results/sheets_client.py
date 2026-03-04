from __future__ import annotations

from dataclasses import dataclass

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import Resource, build


SCOPES = ("https://www.googleapis.com/auth/spreadsheets.readonly",)


@dataclass
class GoogleSheetsClient:
    spreadsheet_id: str
    service_account_file: str

    def __post_init__(self) -> None:
        credentials = Credentials.from_service_account_file(self.service_account_file, scopes=SCOPES)
        self._service: Resource = build("sheets", "v4", credentials=credentials, cache_discovery=False)
        self._sheet_titles: list[str] | None = None

    def load_sheet_titles(self) -> list[str]:
        response = (
            self._service.spreadsheets()
            .get(spreadsheetId=self.spreadsheet_id, fields="sheets(properties(title))")
            .execute()
        )
        titles = [item["properties"]["title"] for item in response.get("sheets", [])]
        self._sheet_titles = titles
        return titles

    def fetch_all_sheets(self) -> dict[str, list[list[str]]]:
        if not self._sheet_titles:
            self.load_sheet_titles()
        assert self._sheet_titles is not None

        ranges = [f"'{title}'!A:Z" for title in self._sheet_titles]
        response = (
            self._service.spreadsheets()
            .values()
            .batchGet(
                spreadsheetId=self.spreadsheet_id,
                ranges=ranges,
                valueRenderOption="UNFORMATTED_VALUE",
                dateTimeRenderOption="FORMATTED_STRING",
            )
            .execute()
        )

        values_by_sheet: dict[str, list[list[str]]] = {}
        for sheet_title, range_values in zip(self._sheet_titles, response.get("valueRanges", []), strict=False):
            rows = range_values.get("values", [])
            normalized_rows = [[_stringify(cell) for cell in row] for row in rows]
            values_by_sheet[sheet_title] = normalized_rows
        return values_by_sheet


def _stringify(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, float):
        text = f"{value:.12f}".rstrip("0").rstrip(".")
        return text if text else "0"
    return str(value)

