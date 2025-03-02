import argparse
import logging
import sys
from logging import Logger
from os import getenv
from pathlib import Path

from dotenv import load_dotenv

from .scraper import ingest_link, ingest_multi_links


# Set path to env file
def load_environment() -> bool:
    """Load environment variables from .env if it exists."""
    env_path: Path = Path(".") / ".env"
    logger: Logger = logging.getLogger("CLI")
    if env_path.exists():
        load_dotenv(dotenv_path=env_path)
        logger.debug(f"Read environment variables from {env_path}")
        return True
    logger.debug(
        "Please set an environment file as .env in the project root directory."
    )
    return False


def check_environment() -> bool:
    """Check the configuration of the environment."""
    logger: Logger = logging.getLogger()

    load_environment()

    required_vars: list[str] = [
        "AVATURE_MART_DIR",
        "AVATURE_SOURCE_DIR",
        "AVATURE_WAREHOUSE_DIR",
    ]

    # Check for missing variables
    missing_vars: list[str] = [var for var in required_vars if not getenv(var)]
    if missing_vars:
        logger.error(
            f"Missing required environment variables: {', '.join(missing_vars)}"
        )
        return False

    return True


def main():
    """Entry point for CLI"""
    if not load_environment():
        sys.exit(1)
    if not check_environment():
        sys.exit(1)

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
        default=getenv("AVATURE_WAREHOUSE_DIR"),
        nargs="?",
        help="Parent directory path where the file(s) should be saved. By default, it uses the environment's 'AVATURE_WAREHOUSE_DIR' value.",
        type=str,
    )

    parser.add_argument(
        "-k",
        "--sourceJSON",
        required=False,
        default=getenv("AVATURE_SOURCE_DIR"),
        nargs="?",
        help="Parent directory path where the listJSON is located. By default, it uses the environment's 'AVATURE_SOURCE_DIR' value.",
        type=str,
    )

    parser.add_argument(
        "-j",
        "--listJSON",
        required=False,
        nargs="+",
        help="Name of the JSON file that contains multiple URLs (e.g. talent_links.json)",
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
        list_path: Path = Path(args["parentDir"]) / args["fileName"]
        ingest_link(
            list_url=args["listLink"],
            data_file_path=list_path,
            ssl_verify=args["no_ssl"],
        )


if __name__ == "__main__":
    sys.exit(main())
