from .extract.cli import main as cli_main
from .extract.scraper import ingest_html_table

__all__ = [
    "cli_main",
    "ingest_html_table",
]
