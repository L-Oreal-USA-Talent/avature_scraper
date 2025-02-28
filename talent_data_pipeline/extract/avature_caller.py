from __future__ import annotations

import csv
from pathlib import Path

import requests
from bs4 import BeautifulSoup
from requests import Response
from requests.adapters import HTTPAdapter, Retry


def _get_table__headings(bs_soup):
    """Get table column headings (labels) in HTML table."""
    return [header.get_text(strip=True) for header in bs_soup.find_all("th")]


def _get_row_data(table_row):
    """Get each data values that exist in HTML table rows."""
    return [data.get_text(strip=True) for data in table_row.find_all("td")]


def _save_table_data(file_path: Path, col_headings: list, tbl_rows: list) -> None:
    """
    Save table data to csv file.
    :param file_path: Absolute path to the resulting file.
    :param col_headings: Header row.
    :param tbl_rows: Data points to store in table.
    :return: None
    """
    with open(file_path, "w", newline="", encoding="utf-8") as csv_file:
        csv_writer = csv.writer(csv_file, dialect="excel")
        csv_writer.writerow(col_headings)
        for row in tbl_rows:
            csv_writer.writerow(row)


def _extract_html_data(html_link, source_path: Path) -> None:
    """
    Extract HTML tabular data located in html_link and save it to the
    indicated source_path. If html_link is an empty page, an empty file
    is created at source_path.
    :param html_link: Page leading to HTML tabular data.
    :param source_path: The absolute path of the resulting file.
    :return: None
    """
    all_data = []
    soup = BeautifulSoup(html_link.content, features="lxml")

    # It is possible that Avature could return an empty table despite
    # a successful connection to the page.
    if len(soup) == 0:
        with open(source_path, "w"):
            print(
                f"Creating an empty file in: {source_path} because {html_link} returned empty content."
            )
            return

    column_headings = _get_table__headings(soup)
    table_rows = soup.find_all("tr")
    for row in table_rows:
        all_data.append(_get_row_data(row))

    _save_table_data(source_path, column_headings, all_data[1:])


def download_html_data(
    data_html_link: str,
    data_path: Path,
    with_verify: bool = True,
    request_retries: int = 3,
) -> None:
    """
    Download Avature HTML links as data_path as CSV. It will save
    to the warehouse.
    :param data_path: The absolute path to the file that will contain ingested data.
    :param data_html_link: The HTML public link.
    :param with_verify: Boolean toggle for SSL verification. Default is True.
    :param request_retries: The maximum number of requests attempted. Default is 3.
    :return: None
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
        html_res: Response = s.get(data_html_link, verify=with_verify)
    except (
        requests.exceptions.HTTPError,
        requests.exceptions.Timeout,
        requests.exceptions.RequestException,
    ) as e:
        print(f"{data_html_link} responded with: {e}. The data cannot be ingested.")
    else:
        _extract_html_data(html_res, data_path)
        print(f"{data_path} has been created.")
