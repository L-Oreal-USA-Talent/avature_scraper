import argparse
from pathlib import Path
import sys
from .scraper import ingest_link, ingest_multi_links


def main():
    parser = argparse.ArgumentParser(
        prog="HTML tabluar scarper.",
        description="Scrape tabular data from a webpage and save it to a CSV file",
    )

    parser.add_argument(
        "-t",
        "--type",
        required=True,
        help="Type of ingestion. Choose between 'list' or 'single'",
        choices=["list", "single"],
        type=str,
    )

    parser.add_argument(
        "-l",
        "--listLink",
        required=False,
        nargs="?",
        help="URL of the webpage containing the data.",
        type=str,
    )

    parser.add_argument(
        "-n",
        "--fileName",
        required=False,
        nargs="?",
        help="Name of the file to save the data (e.g. data.csv)",
        type=str,
    )

    parser.add_argument(
        "-p",
        "--parentDir",
        required=False,
        nargs="?",
        help="Parent directory path where the file(s) should be saved",
        type=str,
    )

    parser.add_argument(
        "-k",
        "--sourceJSON",
        required=False,
        nargs="?",
        help="Parent directory path where the listJSON is located",
        type=str,
    )

    parser.add_argument(
        "-j",
        "--listJSON",
        required=False,
        nargs="+",
        help="JSON file that contains multiple URLs",
        type=str,
    )

    ssl_verify = True
    parser.add_argument(
        "-s",
        "--no-ssl",
        required=False,
        action="store_const",
        default=ssl_verify,
        const=not ssl_verify,
        help="Toggle SSL verification. Default is SSL verification. Pass to disable.",
    )

    args: dict = vars(parser.parse_args())

    if args["type"] == "list":
        ingest_multi_links(
            json_paths=args["listJSON"],
            src_dir=args["sourceJSON"],
            storage_dir=args["parentDir"],
            ssl_verify=args["no_ssl"],
        )
    elif args["type"] == "single":
        # list_path: Path = Path(f"{args["parentDir"]}{args["fileName"]}")
        list_path: Path = Path(args["parentDir"] + args["fileName"])
        ingest_link(
            list_url=args["listLink"],
            data_file_path=list_path,
            ssl_verify=args["no_ssl"],
        )


if __name__ == "__main__":
    sys.exit(main())
