import json
from pathlib import Path
from typing import Any

import pandas as pd
from pandas import DataFrame, read_csv
from pandas.errors import EmptyDataError


class Transformer:
    def __init__(self, index_file: Path):
        """
        Construct instance with index_file that points to data files.
        Utilize the `index_lib` instance attribute to move through the index file.
        For example: self.index_lib['offers'] will return the specified path(s)
        indicated at this location. Check the structure of your index_file to make sure
        if a list of paths or a singular string path is being accessed.
        :param index_file: Directory of JSON file that points to other files in the drive.
        """
        self.index_file: Path = index_file
        self.index_lib: dict[str, Any] = self._read_index_file()

    def _read_index_file(self) -> dict[str, Any]:
        """
        Parse the given index file in the constructor and return dict of index.
        :return: Dict of index file.
        """
        with open(self.index_file, "r") as idx_f:
            return json.load(idx_f)

    def load_frame(
        self,
        data_file_label: str,
        storage_path: Path,
        load_cols: list[str] | None = None,
        col_dtypes: dict | None = None,
        date_cols: list[str] | None = None,
        date_frmt: str | None = None,
        load_index: bool = False,
        ignore_index: bool = True,
    ) -> DataFrame:
        """
        Load a file or set of files from the index set in config. If the `data_file_label`
        points to a list of paths, a concatenated DataFrame is returned.
        :param data_file_label: File label given in the index.
        :param storage_path: Directory that contains file.
        :param load_cols: Specify which column labels to load. Default loads all columns.
        :param col_dtypes: Dict of data types for columns. Default is None.
        :param date_cols: List of date columns to parse. Default is None.
        :param date_frmt: Format string to parse date/datetime. Default is None.
        :param load_index: Boolean to load index. Default is False.
        :param ignore_index: Boolean to ignore index when concatenating multiple files. Default is True.
        :return: DataFrame of data found at `data_file_label`.
        """

        list_of_paths: list[str] = self.index_lib[data_file_label]
        if not isinstance(list_of_paths, list):
            list_of_paths = [self.index_lib[data_file_label]]

        frames: list[DataFrame] = []

        for path_ in list_of_paths:
            data_path: Path = storage_path / path_
            try:
                data_frame: DataFrame = read_csv(
                    data_path,
                    parse_dates=date_cols,
                    date_format=date_frmt,
                    dtype=col_dtypes,
                    index_col=load_index,
                    usecols=load_cols,
                    on_bad_lines="warn",
                )
            except FileNotFoundError:
                print(f"{data_path} does not exist. Returning empty DataFrame")
                frames.append(DataFrame())
            except EmptyDataError:
                print(f"{data_path} is empty. Appending empty DataFrame.")
                frames.append(DataFrame())
            else:
                frames.append(data_frame)

        return pd.concat(frames, ignore_index=ignore_index)

    def save_frame_to_storage(
        self,
        data_frame: DataFrame,
        storage_path: Path,
        data_file_label: str,
        keep_index: bool = False,
    ) -> None:
        """
        Save a file to the mart set in config.
        :param storage_path:
        :param data_frame: DataFrame to save.
        :param data_file_label: File label given in index.
        :param keep_index: Boolean to keep index in frame. Default is False.
        :return: None
        """
        file_save_path: Path = storage_path / self.index_lib[data_file_label]
        data_frame.to_csv(file_save_path, index=keep_index)
