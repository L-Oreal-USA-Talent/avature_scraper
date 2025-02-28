from pandas import DataFrame


def make_application_pairs(transaction_frame: DataFrame) -> DataFrame:
    """
    Create a `Pair` column of concatenated "Job ID" and "Person ID".
    :param transaction_frame: DataFrame containing the "Job ID" and "Person ID" column.
    :return: DataFrame with a concatenation of "Job ID" and "Person ID" as "Pair" column.
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
