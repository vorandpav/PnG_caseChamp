from __future__ import annotations

import argparse
import csv
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable
from urllib.parse import urlsplit

DELIMITER_RE = re.compile(r"^\s*=== ===\s*$", re.MULTILINE)
SOURCE_LINE_RE = re.compile(r"^\s*\[(\d+)]\s+(\S+)\s*$", re.MULTILINE)
URL_RE = re.compile(r"(?i)\b((?:https?://|www\.)[^\s<>'\"\]]+)")
DOMAIN_RE = re.compile(
    r"(?i)\b((?:[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.)+[a-z]{2,}(?:/[^\s<>'\"]*)?)"
)
POPULAR_TLDS = {
    "ru",
    "com",
    "net",
    "org",
    "info",
    "biz",
    "su",
    "io",
    "ai",
    "me",
    "dev",
    "app",
    "pro",
    "co",
    "xyz",
    "site",
    "online",
    "store",
    "top",
    "рф",
}


@dataclass
class NeuroRecord:
    query: str
    answer_text: str
    links_in_text: list[str]
    sites_in_text: list[str]
    links_from_sources: list[str]
    sites_from_sources: list[str]
    product_category: str
    brand: str
    question_category: str


def unique_preserve_order(values: Iterable[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        if value not in seen:
            seen.add(value)
            result.append(value)
    return result


def normalize_url(url: str) -> str:
    clean = url.strip().rstrip(".,;)")
    if clean.lower().startswith("www."):
        return f"https://{clean}"
    return clean


def split_neuro_sections(content: str) -> tuple[str, str, str]:
    normalized = content.replace("\r\n", "\n").replace("\r", "\n")
    parts = DELIMITER_RE.split(normalized)

    if len(parts) < 3:
        first_line = normalized.split("\n", 1)[0].strip()
        return first_line, normalized.strip(), ""

    header_part = parts[0].strip("\n")
    query = header_part.split("\n", 1)[0].strip()
    answer_text = parts[1].strip()
    sources_block = "\n".join(parts[2:]).strip()
    return query, answer_text, sources_block


def parse_host(value: str) -> str:
    parsed = urlsplit(value)
    host = parsed.netloc or parsed.path.split("/", 1)[0]
    host = host.split("@")[-1].split(":", 1)[0].lower().strip(".")
    if host.startswith("www."):
        host = host[4:]
    return host


def is_allowed_host(host: str) -> bool:
    parts = [part for part in host.split(".") if part]
    if len(parts) < 2:
        return False
    return parts[-1] in POPULAR_TLDS


def extract_links_from_text(text: str) -> list[str]:
    links: list[str] = []

    for match in URL_RE.findall(text):
        normalized = normalize_url(match)
        if is_allowed_host(parse_host(normalized)):
            links.append(normalized)

    for match in DOMAIN_RE.findall(text):
        normalized = normalize_url(f"https://{match}")
        if is_allowed_host(parse_host(normalized)):
            links.append(normalized)

    return unique_preserve_order(links)


def extract_links_from_sources(sources_block: str) -> list[str]:
    if not sources_block or "No data" in sources_block:
        return []

    numbered_links = [normalize_url(match[1]) for match in SOURCE_LINE_RE.findall(sources_block)]
    if numbered_links:
        return unique_preserve_order(
            link for link in numbered_links if is_allowed_host(parse_host(link))
        )

    fallback_links = [normalize_url(match) for match in URL_RE.findall(sources_block)]
    return unique_preserve_order(link for link in fallback_links if is_allowed_host(parse_host(link)))


def extract_site_from_url(url: str) -> str:
    host = parse_host(url)
    if not host or not is_allowed_host(host):
        return ""
    return host


def extract_sites_from_links(links: Iterable[str]) -> list[str]:
    return unique_preserve_order(site for site in (extract_site_from_url(link) for link in links) if site)


def parse_input_folder_meta(folder_path: Path) -> tuple[str, str, str]:
    effective_folder = folder_path.parent if folder_path.suffix.lower() == ".txt" else folder_path
    parts = [part.strip() for part in effective_folder.name.split("_", 2)]

    if len(parts) == 3:
        return parts[0], parts[1], parts[2]

    if len(parts) == 2:
        return parts[0], parts[1], ""

    if len(parts) == 1:
        return parts[0], "", ""

    return "", "", ""


def resolve_meta_source_for_file(file_path: Path, input_path: Path) -> Path:
    if input_path.is_file():
        return file_path.parent

    try:
        relative = file_path.relative_to(input_path)
    except ValueError:
        return file_path.parent

    if len(relative.parts) >= 2:
        return input_path / relative.parts[0]

    return file_path.parent


def parse_neuro_file(
    file_path: Path,
    product_category: str = "",
    brand: str = "",
    question_category: str = "",
) -> NeuroRecord:
    content = file_path.read_text(encoding="utf-8")
    query, answer_text, sources_block = split_neuro_sections(content)

    links_in_text = extract_links_from_text(answer_text)
    links_from_sources = extract_links_from_sources(sources_block)

    return NeuroRecord(
        query=query,
        answer_text=answer_text,
        links_in_text=links_in_text,
        sites_in_text=extract_sites_from_links(links_in_text),
        links_from_sources=links_from_sources,
        sites_from_sources=extract_sites_from_links(links_from_sources),
        product_category=product_category,
        brand=brand,
        question_category=question_category,
    )


def collect_input_files(input_path: Path) -> list[Path]:
    if input_path.is_file():
        return [input_path]

    if not input_path.exists():
        raise FileNotFoundError(f"File not found: {input_path}")

    return sorted(path for path in input_path.rglob("*.txt") if path.is_file())


def write_csv(records: list[NeuroRecord], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "категория_товара",
                "бренд",
                "категория_вопроса",
                "запрос",
                "текст_ответа",
                "ссылки_в_тексте",
                "сайты_в_тексте",
                "ссылки_из_блока_источников",
                "сайты_из_блока_источников",
            ],
            delimiter=",",
        )
        writer.writeheader()
        for record in records:
            writer.writerow(
                {
                    "категория_товара": record.product_category,
                    "бренд": record.brand,
                    "категория_вопроса": record.question_category,
                    "запрос": record.query,
                    "текст_ответа": record.answer_text,
                    "ссылки_в_тексте": "\n".join(record.links_in_text),
                    "сайты_в_тексте": "\n".join(record.sites_in_text),
                    "ссылки_из_блока_источников": "\n".join(record.links_from_sources),
                    "сайты_из_блока_источников": "\n".join(record.sites_from_sources),
                }
            )


def write_xlsx(records: list[NeuroRecord], output_path: Path) -> None:
    try:
        from openpyxl import Workbook
    except ImportError as exc:
        raise RuntimeError(
            "pip install -r requirements.txt"
        ) from exc

    output_path.parent.mkdir(parents=True, exist_ok=True)
    workbook = Workbook()
    sheet = workbook.active
    sheet.title = "neuro_data"
    headers = [
        "категория_товара",
        "бренд",
        "категория_вопроса",
        "запрос",
        "текст_ответа",
        "ссылки_в_тексте",
        "сайты_в_тексте",
        "ссылки_из_блока_источников",
        "сайты_из_блока_источников",
    ]
    sheet.append(headers)

    for record in records:
        sheet.append(
            [
                record.product_category,
                record.brand,
                record.question_category,
                record.query,
                record.answer_text,
                "\n".join(record.links_in_text),
                "\n".join(record.sites_in_text),
                "\n".join(record.links_from_sources),
                "\n".join(record.sites_from_sources),
            ]
        )

    workbook.save(output_path)


def write_json(records: list[NeuroRecord], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    payload = [
        {
            "product_category": record.product_category,
            "brand": record.brand,
            "question_category": record.question_category,
            "query": record.query,
            "answer_text": record.answer_text,
            "links_in_text": record.links_in_text,
            "sites_in_text": record.sites_in_text,
            "links_from_sources": record.links_from_sources,
            "sites_from_sources": record.sites_from_sources,
        }
        for record in records
    ]
    output_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Парсит txt-выгрузки из Yandex Neuro и строит таблицу: "
            "запрос, текст ответа, ссылки из текста, ссылки из блока источников."
        )
    )
    parser.add_argument(
        "--input",
        required=True,
        help="Путь к txt-файлу или папке-источнику (автоматически обработает все txt в подпапках)",
    )
    parser.add_argument("--output", required=True, help="Файл результата: .csv, .xlsx или .json")
    return parser


def detect_format(output_path: Path) -> str:
    suffix = output_path.suffix.lower()
    if suffix == ".csv":
        return "csv"
    if suffix == ".xlsx":
        return "xlsx"
    if suffix == ".json":
        return "json"
    raise ValueError("Only .csv, .xlsx, .json are supported as output formats.")


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    input_path = Path(args.input)
    output_path = Path(args.output)
    output_format = detect_format(output_path)

    input_files = collect_input_files(input_path)
    if not input_files:
        raise RuntimeError(f"Files not found: {input_path}")

    records: list[NeuroRecord] = []
    for path in input_files:
        meta_source = resolve_meta_source_for_file(path, input_path)
        product_category, brand, question_category = parse_input_folder_meta(meta_source)
        records.append(parse_neuro_file(path, product_category, brand, question_category))

    if output_format == "csv":
        write_csv(records, output_path)
    elif output_format == "xlsx":
        write_xlsx(records, output_path)
    else:
        write_json(records, output_path)

    print(f"Done: {len(records)} files are processed, {output_path}")


if __name__ == "__main__":
    main()

