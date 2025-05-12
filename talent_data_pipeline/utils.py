import pandas as pd
from pandas import DataFrame


def make_application_pairs(transaction_frame: DataFrame) -> DataFrame:
    """Create a DataFrame with a new column containing the Job ID and Person ID pair.

    Args:
        transaction_frame (DataFrame): Transaction DataFrame containing Job ID and Person ID columns.
    Raises:
        KeyError: Missing Job ID and/or Person ID columns in the DataFrame.

    Returns:
        DataFrame: DataFrame with a new column 'Pair' containing the Job ID and Person ID pair.
    """
    if not {"Job ID", "Person ID"}.issubset(transaction_frame.columns):
        print("Job ID and/or Person ID columns are missing in the dataframe.")
        raise KeyError

    transaction_pairs: DataFrame = transaction_frame.copy()
    transaction_pairs["Pair"] = (
        transaction_pairs["Job ID"].astype(str)
        + "-"
        + transaction_pairs["Person ID"].astype(str)
    )

    return transaction_pairs


def explode_date_range(
    transaction_frame: DataFrame,
    start_date_col: str,
    end_date_col: str,
) -> DataFrame:
    """This function takes a DataFrame with a start date and end date column and 
    returns a new DataFrame with a new column "Calendar Date" that contains a record 
    for each day between the start date and end date.

    Args:
        transaction_frame (DataFrame): Transaction DataFrame containing a date range.
        start_date_col (str): Name of the start date column.
        end_date_col (str): Name of the end date col.

    Returns:
        DataFrame: DataFrame with new column "Calendar Date" with a record per day between start_date_col and end_date_col.
    """

    transactions_extended: DataFrame = transaction_frame.copy()
    transactions_extended["Calendar Date"] = [
        pd.date_range(start_date, end_date, freq="D")
        for start_date, end_date in zip(
            transactions_extended[start_date_col], transactions_extended[end_date_col]
        )
    ]

    return transactions_extended
