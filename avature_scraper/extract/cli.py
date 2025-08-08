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
        help="Type of ingestion. Choose between 'list' or 'single'",
        choices=["single", "multi"],
        type=str,
    )

    parser.add_argument(
        "-w",
        "--input_path",
        required=False,
        nargs="?",
        help="""Parent directory path where data should be written.""",
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
        help="File where data should be written, e.g. 'data.csv'",
        type=str,
    )

    parser.add_argument(
        "-k",
        "--map_path",
        required=False,
        nargs="?",
        help="""Parent directory path where the listJSON is located. 
        By default, it uses the environment AVATURE_SOURCE_DIR.""",
        type=str,
    )

    parser.add_argument(
        "-m",
        "--url_map",
        required=False,
        action="append",
        help="JSON file mapping pairs of file names and URLs.",
        type=str,
    )

    ssl_verify = True
    parser.add_argument(
        "-s",
        "--no_ssl",
        required=False,
        action="store_const",
        default=ssl_verify,
        const=not ssl_verify,
        help="Toggle SSL verification. Default is True. Pass to disable.",
    )

    # Parse args and set pipeline options
    # Args are exclusive to this script and are required.
    # Pipeline options can be passed only when required, i.e. after ingesting
    # the latest data
    args, pipeline_opts = parser.parse_known_args(argv)

    # Operating in single mode is simple:
    # Ingest the link, save to the input_path
    if args.type == "single":
        ingest_html_table(
            html_url=args.url,
            target_file=args.input_path,
            with_verify=args.no_ssl,
        )

    # Mutli mode requires iterating though the source jsons
    # that are located at map_path
    # Then, iterate through each url_map and read each file_name:url pair
    elif args.type == "multi":
        maps_to_ingest: list[dict[str, str]] = []
        json_paths: list[str] = args.url_map

        # Iterate over all the json_paths
        # and collect url mappings
        for json_path in json_paths:
            # Prepend with source_dir
            url_mapping: Path = Path(args.map_path) / json_path

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
                file_path: Path = Path(args.input_path) / f"{file_name}.csv"
                ingest_html_table(html_url=url, target_file=file_path)


if __name__ == "__main__":
    main()
