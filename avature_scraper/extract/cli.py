import argparse
import json
from pathlib import Path

from .scraper import ingest_html_table


def main(argv=None) -> None:
    """Entry point for CLI

    Args:
        argv (_type_, optional): _description_. Defaults to None.
    """

    parser = argparse.ArgumentParser(
        prog="HTML web scraper.",
        description="Scrape tabular data from a webpage and save it to a CSV file",
    )

    parser.add_argument(
        "-t",
        "--type",
        required=True,
        help="""Type of ingestion. Choose between 'single' or 'multi'.
        If 'single', only one URL is processed.
        If 'multi', a mapping of file_name passed via -m or --url_map is expected.""",
        choices=["single", "multi"],
        type=str,
    )

    parser.add_argument(
        "-i",
        "--input_path",
        required=False,
        nargs="?",
        help="""Directory from which mapping of URLs should be read.
        Use only when --type is 'multi'.""",
        type=str,
    )

    parser.add_argument(
        "-o",
        "--output_path",
        required=False,
        nargs="?",
        help="""Directory to which data should be written.""",
        type=str,
    )

    parser.add_argument(
        "-u",
        "--url",
        required=False,
        nargs="?",
        help="URL leading to HTML table data.",
        type=str,
    )

    parser.add_argument(
        "-n",
        "--file_name",
        required=False,
        nargs="?",
        help="""File where data should be written, e.g. 'data.csv'. 
        It is prepended with the output directory.
        Use only when --type is 'single'""",
        type=str,
    )

    parser.add_argument(
        "-m",
        "--url_map",
        required=False,
        action="append",
        help="""JSON file mapping pairs of file names and URLs. 
        Use only when --type is 'multi'.
        Pass -m or --url_map multiple times to specify multiple mappings.""",
        type=str,
    )

    # Arg that toggles SSL verification for http requests.
    # By default, SSL verification is enabled.
    # Pass --disable_ssl to disable.
    parser.add_argument(
        "--disable_ssl",
        action="store_true",
        help="Disable SSL verification. By default, SSL verification is enabled.",
        type=bool,
    )

    args = parser.parse_args(argv)

    # If --disable_ssl is present, args.disable_ssl is True, so ssl_verify becomes False.
    # If --disable_ssl is not present, args.disable_ssl is False, so ssl_verify becomes True.
    ssl_verify: bool = not args.disable_ssl

    # Operating in single mode is simple:
    # Ingest the link, save to the output_path
    if args.type == "single":
        if not args.url:
            raise ValueError("URL is required for single ingestion mode.")
        url_file: Path = Path(args.output_path) / args.file_name
        ingest_html_table(
            html_url=args.url,
            target_file=url_file,
            with_verify=ssl_verify,
        )

    # Multi mode requires iterating though the source jsons
    # that are located at map_path
    # Then, iterate through each url_map and read each file_name:url pair
    elif args.type == "multi":
        maps_to_ingest: list[dict[str, str]] = []
        json_paths: list[str] = args.url_map

        # Iterate over all the json_paths
        # and collect url mappings
        for json_path in json_paths:
            # Prepend with input_path
            url_mapping: Path = Path(args.input_path) / json_path

            if url_mapping.suffix != ".json":
                raise ValueError(
                    "Unsupported mapping. Pass a JSON file of file_name -> URL."
                )
            if not url_mapping.exists():
                raise FileNotFoundError(f"{url_mapping} could not be located")
            with open(url_mapping, "r", encoding="utf-8") as f:
                data_: dict[str, str] = json.load(f)
                maps_to_ingest.append(data_)
        # Iterate over all collected mappings and ingest data
        for map in maps_to_ingest:
            for file_name, url in map.items():
                file_path: Path = Path(args.output_path) / f"{file_name}.csv"
                ingest_html_table(html_url=url, target_file=file_path)


if __name__ == "__main__":
    main()
