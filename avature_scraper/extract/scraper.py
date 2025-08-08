import csv
from pathlib import Path

import requests
from bs4 import BeautifulSoup
from requests import Response
from requests.adapters import HTTPAdapter, Retry


def _get_table__headings(bs_soup: BeautifulSoup) -> list[str]:
    """Parse table headings from HTML tree that has been parsed by BeautifulSoup.

    Args:
        bs_soup (BeautifulSoup): HTML tree contents.

    Returns:
        headers (list[str]): List of column headers.
    """
    return [header.get_text(strip=True) for header in bs_soup.find_all("th")]


def _get_row_data(table_row) -> list[str]:
    """Parse table values from a <tr> HTML.

    Args:
        table_row (_type_): A table row enclosed in <tr> HTML tags.

    Returns:
        row_data (list[str]): Values found in the table record.
    """
    return [data.get_text(strip=True) for data in table_row.find_all("td")]


def _save_table_data(
    target_file: Path,
    col_headings: list[str],
    tbl_rows: list[list[str]],
) -> None:
    """Save tabular data.

    Args:
        file_path (Path): Directory where data should be written.
        col_headings (list[str]): Column headers.
        tbl_rows (list[list[str]]): Table records.
    """
    with open(target_file, "w", newline="", encoding="utf-8") as csv_file:
        csv_writer = csv.writer(csv_file, dialect="excel")
        csv_writer.writerow(col_headings)
        for row in tbl_rows:
            csv_writer.writerow(row)


def _extract_html_data(html_link, target_file: Path) -> None:
    """Extract tabular data given an HTML URL.
    The URL must point to an HTML rendered table.

    Args:
        html_link (_type_): Web URL.
        target_file (Path): File in which contents should be written.
    """
    all_data = []
    soup = BeautifulSoup(html_link.content, features="lxml")

    # It is possible that Avature could return an empty table despite
    # a successful connection to the page.
    # In this case, an empty file is written.
    if len(soup) == 0:
        with open(target_file, "w"):
            print(
                f"Creating an empty file in: {target_file} because {html_link} returned empty content."
            )
            return

    column_headings = _get_table__headings(soup)
    table_rows = soup.find_all("tr")
    for row in table_rows:
        all_data.append(_get_row_data(row))

    _save_table_data(target_file, column_headings, all_data[1:])


def ingest_html_table(
    html_url: str,
    target_file: Path,
    with_verify: bool = True,
    request_retries: int = 3,
) -> None:
    """Download HTML data given a URL that points to an HTML-rendered table.

    Args:
        html_url (str): Web URL that leads to HTML table contents.
        target_dir (Path): File to which table should be written.
        with_verify (bool, optional): Toggle SSL verification. Defaults to True.
        request_retries (int, optional): Maximum number of requests attempted in case of request errors.
            Defaults to 3.
    """
    s = requests.Session()
    retries: Retry = Retry(
        total=request_retries,
        backoff_factor=1,
        status_forcelist=[502, 503, 504],
        raise_on_status=True,
    )
    s.mount("https://", HTTPAdapter(max_retries=retries))

    try:
        html_res: Response = s.get(html_url, verify=with_verify)
    except (
        requests.exceptions.HTTPError,
        requests.exceptions.Timeout,
        requests.exceptions.RequestException,
    ) as e:
        print(f"{html_url} responded with: {e}. The data cannot be ingested.")
    else:
        _extract_html_data(html_res, target_file)
        print(f"{target_file} has been created.")
