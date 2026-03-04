from __future__ import annotations

import os
import tempfile
import zipfile
from dataclasses import dataclass
from pathlib import PurePosixPath
from typing import Literal
from xml.etree import ElementTree as ET

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import Resource, build
from googleapiclient.errors import HttpError


SCOPES = (
    "https://www.googleapis.com/auth/spreadsheets.readonly",
    "https://www.googleapis.com/auth/drive.readonly",
)

SHEET_MIME_TYPE = "application/vnd.google-apps.spreadsheet"
XLSX_MIME_TYPE = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
SourceKind = Literal["google_sheet", "drive_xlsx"]

NS_MAIN = {"m": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
NS_PKG_REL = {"p": "http://schemas.openxmlformats.org/package/2006/relationships"}


@dataclass(frozen=True)
class DriveFileMeta:
    mime_type: str
    name: str
    modified_time: str
    md5_checksum: str
    size: str


@dataclass
class GoogleSheetsClient:
    spreadsheet_id: str
    service_account_file: str

    def __post_init__(self) -> None:
        credentials = Credentials.from_service_account_file(self.service_account_file, scopes=SCOPES)
        self._sheets_service: Resource = build("sheets", "v4", credentials=credentials, cache_discovery=False)
        self._drive_service: Resource = build("drive", "v3", credentials=credentials, cache_discovery=False)
        self._sheet_titles: list[str] | None = None
        self._source_kind: SourceKind | None = None
        self._cached_values: dict[str, list[list[str]]] | None = None
        self._last_revision: tuple[str, str, str] | None = None

    def load_sheet_titles(self) -> list[str]:
        source_kind = self._ensure_source_kind()
        if source_kind == "google_sheet":
            try:
                response = (
                    self._sheets_service.spreadsheets()
                    .get(spreadsheetId=self.spreadsheet_id, fields="sheets(properties(title))")
                    .execute()
                )
            except HttpError as exc:
                _raise_readable_sheets_error(exc, self.spreadsheet_id)
            titles = [item["properties"]["title"] for item in response.get("sheets", [])]
            self._sheet_titles = titles
            return titles

        if self._sheet_titles is not None:
            return self._sheet_titles
        values = self.fetch_all_sheets()
        self._sheet_titles = list(values.keys())
        return self._sheet_titles

    def fetch_all_sheets(self) -> dict[str, list[list[str]]]:
        source_kind = self._ensure_source_kind()
        if source_kind == "google_sheet":
            return self._fetch_google_sheet_values()
        return self._fetch_drive_xlsx_values()

    def _fetch_google_sheet_values(self) -> dict[str, list[list[str]]]:
        if not self._sheet_titles:
            self.load_sheet_titles()
        assert self._sheet_titles is not None

        ranges = [f"'{title}'!A:Z" for title in self._sheet_titles]
        try:
            response = (
                self._sheets_service.spreadsheets()
                .values()
                .batchGet(
                    spreadsheetId=self.spreadsheet_id,
                    ranges=ranges,
                    valueRenderOption="UNFORMATTED_VALUE",
                    dateTimeRenderOption="FORMATTED_STRING",
                )
                .execute()
            )
        except HttpError as exc:
            _raise_readable_sheets_error(exc, self.spreadsheet_id)

        values_by_sheet: dict[str, list[list[str]]] = {}
        for sheet_title, range_values in zip(self._sheet_titles, response.get("valueRanges", []), strict=False):
            rows = range_values.get("values", [])
            normalized_rows = [[_stringify(cell) for cell in row] for row in rows]
            values_by_sheet[sheet_title] = normalized_rows
        self._cached_values = values_by_sheet
        return values_by_sheet

    def _fetch_drive_xlsx_values(self) -> dict[str, list[list[str]]]:
        meta = self._get_drive_file_meta()
        revision = (meta.modified_time, meta.md5_checksum, meta.size)
        if self._cached_values is not None and revision == self._last_revision:
            return self._cached_values

        temp_path: str | None = None
        try:
            media_bytes = (
                self._drive_service.files()
                .get_media(fileId=self.spreadsheet_id, supportsAllDrives=True)
                .execute()
            )
            if not isinstance(media_bytes, (bytes, bytearray)):
                raise RuntimeError("Drive API вернул неожиданный формат файла.")

            with tempfile.NamedTemporaryFile(prefix="online_protocol_", suffix=".xlsx", delete=False) as tmp:
                tmp.write(media_bytes)
                temp_path = tmp.name

            parsed_values = _read_xlsx_values(temp_path)
            if not parsed_values:
                raise RuntimeError("Не удалось прочитать листы из XLSX-файла.")

            self._cached_values = parsed_values
            self._last_revision = revision
            self._sheet_titles = list(parsed_values.keys())
            return parsed_values
        except HttpError as exc:
            _raise_readable_sheets_error(exc, self.spreadsheet_id)
        finally:
            if temp_path and os.path.exists(temp_path):
                os.remove(temp_path)

    def _ensure_source_kind(self) -> SourceKind:
        if self._source_kind is not None:
            return self._source_kind

        meta = self._get_drive_file_meta()
        mime = meta.mime_type.strip().lower()
        if mime == SHEET_MIME_TYPE:
            self._source_kind = "google_sheet"
            return self._source_kind
        if mime == XLSX_MIME_TYPE:
            self._source_kind = "drive_xlsx"
            return self._source_kind

        raise RuntimeError(
            "Неподдерживаемый тип файла в Google Drive.\n"
            f"ID: {self.spreadsheet_id}\n"
            f"MIME: {meta.mime_type}\n"
            "Поддерживается: Google Таблица или XLSX."
        )

    def _get_drive_file_meta(self) -> DriveFileMeta:
        try:
            response = (
                self._drive_service.files()
                .get(
                    fileId=self.spreadsheet_id,
                    fields="name,mimeType,modifiedTime,md5Checksum,size",
                    supportsAllDrives=True,
                )
                .execute()
            )
        except HttpError as exc:
            _raise_readable_sheets_error(exc, self.spreadsheet_id)
        return DriveFileMeta(
            mime_type=_stringify(response.get("mimeType")),
            name=_stringify(response.get("name")),
            modified_time=_stringify(response.get("modifiedTime")),
            md5_checksum=_stringify(response.get("md5Checksum")),
            size=_stringify(response.get("size")),
        )


def _read_xlsx_values(xlsx_path: str) -> dict[str, list[list[str]]]:
    with zipfile.ZipFile(xlsx_path, "r") as archive:
        workbook_xml = _read_xml(archive, "xl/workbook.xml")
        workbook_rels_xml = _read_xml(archive, "xl/_rels/workbook.xml.rels")
        shared_strings = _read_shared_strings(archive)

        rel_target_by_id: dict[str, str] = {}
        for rel_node in workbook_rels_xml.findall(".//p:Relationship", NS_PKG_REL):
            rel_id = rel_node.attrib.get("Id", "")
            target = rel_node.attrib.get("Target", "")
            if rel_id and target:
                rel_target_by_id[rel_id] = target

        values_by_sheet: dict[str, list[list[str]]] = {}
        for sheet_node in workbook_xml.findall(".//m:sheets/m:sheet", NS_MAIN):
            sheet_name = sheet_node.attrib.get("name", "").strip()
            rel_id = sheet_node.attrib.get("{http://schemas.openxmlformats.org/officeDocument/2006/relationships}id", "")
            if not sheet_name or not rel_id:
                continue
            target = rel_target_by_id.get(rel_id, "")
            if not target:
                continue
            sheet_path = _resolve_xl_target(target)
            if sheet_path not in archive.namelist():
                continue
            values_by_sheet[sheet_name] = _read_sheet_rows(archive, sheet_path, shared_strings)
        return values_by_sheet


def _read_sheet_rows(
    archive: zipfile.ZipFile,
    sheet_path: str,
    shared_strings: list[str],
) -> list[list[str]]:
    sheet_xml = _read_xml(archive, sheet_path)
    rows: list[list[str]] = []
    for row_node in sheet_xml.findall(".//m:sheetData/m:row", NS_MAIN):
        row_values: list[str] = []
        for cell_node in row_node.findall("m:c", NS_MAIN):
            cell_ref = cell_node.attrib.get("r", "")
            col_index = _column_index_from_ref(cell_ref) if cell_ref else len(row_values)
            while len(row_values) <= col_index:
                row_values.append("")
            row_values[col_index] = _parse_cell_value(cell_node, shared_strings)
        while row_values and row_values[-1] == "":
            row_values.pop()
        rows.append(row_values)
    return rows


def _parse_cell_value(cell_node: ET.Element, shared_strings: list[str]) -> str:
    cell_type = (cell_node.attrib.get("t") or "").strip().lower()
    if cell_type == "inlinestr":
        text_parts = [(_stringify(node.text)) for node in cell_node.findall(".//m:is//m:t", NS_MAIN)]
        return "".join(text_parts).strip()

    value_node = cell_node.find("m:v", NS_MAIN)
    value_text = _stringify(value_node.text if value_node is not None else "").strip()

    if cell_type == "s":
        if not value_text.isdigit():
            return ""
        index = int(value_text)
        return shared_strings[index] if 0 <= index < len(shared_strings) else ""

    if cell_type == "b":
        return "1" if value_text == "1" else "0"

    return value_text


def _read_shared_strings(archive: zipfile.ZipFile) -> list[str]:
    if "xl/sharedStrings.xml" not in archive.namelist():
        return []
    shared_xml = _read_xml(archive, "xl/sharedStrings.xml")
    strings: list[str] = []
    for item in shared_xml.findall(".//m:si", NS_MAIN):
        text_parts = [(_stringify(node.text)) for node in item.findall(".//m:t", NS_MAIN)]
        strings.append("".join(text_parts).strip())
    return strings


def _read_xml(archive: zipfile.ZipFile, path: str) -> ET.Element:
    with archive.open(path) as file_obj:
        return ET.parse(file_obj).getroot()


def _resolve_xl_target(target: str) -> str:
    normalized = PurePosixPath(target.lstrip("/"))
    if normalized.parts and normalized.parts[0] == "xl":
        return str(normalized)
    return str(PurePosixPath("xl") / normalized)


def _column_index_from_ref(cell_ref: str) -> int:
    letters = "".join(ch for ch in cell_ref if ch.isalpha()).upper()
    result = 0
    for ch in letters:
        result = (result * 26) + (ord(ch) - ord("A") + 1)
    return max(result - 1, 0)


def _stringify(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, float):
        text = f"{value:.12f}".rstrip("0").rstrip(".")
        return text if text else "0"
    return str(value)


def _raise_readable_sheets_error(exc: HttpError, spreadsheet_id: str) -> None:
    message = str(exc)
    message_lower = message.lower()
    if "operation is not supported for this document" in message_lower:
        raise RuntimeError(
            "Google Sheets API не может читать этот документ как Google Таблицу.\n"
            f"ID: {spreadsheet_id}\n"
            "Скрипт теперь поддерживает XLSX по этой же ссылке, "
            "но сервисному аккаунту нужен доступ к файлу в Google Drive."
        ) from exc
    if "file not found" in message_lower or "404" in message_lower:
        raise RuntimeError(
            f"Файл не найден или недоступен. Проверьте ID и доступ сервисного аккаунта. ID: {spreadsheet_id}"
        ) from exc
    if "insufficient permissions" in message_lower or "403" in message_lower:
        raise RuntimeError(
            f"Недостаточно прав у сервисного аккаунта для чтения файла. ID: {spreadsheet_id}"
        ) from exc
    raise RuntimeError(f"Ошибка Google API: {exc}") from exc
