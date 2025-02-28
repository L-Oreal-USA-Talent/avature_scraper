import pandas as pd
from pandas import DataFrame


def transform_offers(
    offer_frame: DataFrame,
    applicant_type: str,
    offer_status: str,
    dedup_cols: list[str],
    col_rename: dict,
    use_cols: list[str] | None = None,
) -> DataFrame:
    """
    Transform a dataset of offers.
    :param offer_frame: Dataset that requires transformation.
    :param applicant_type: Choose between "External" or "Internal".
    :param offer_status: Choose between "Accepted", "Rejected", or "Reneged".
    :param dedup_cols: The column by which to deduplicate the DataFrame.
    :param col_rename: A dict of column that specifies how to relabel original name.
    :param use_cols: A list of column labels that should be kept.
    :return: A transformed DataFrame of offers.
    """
    if offer_frame.empty:
        print("DataFrame is empty. Returning original empty frame.")
        return offer_frame

    offer_frame["Applicant Type"] = applicant_type
    offer_frame["Offer Status"] = offer_status
    offer_frame.loc[
        offer_frame["JI | Recruitment Start Date"].isna(), "JI | Recruitment Start Date"
    ] = offer_frame.loc[offer_frame["JI | Recruitment Start Date"].isna(), "Date"]

    if "USA | PRISM Req ID" in offer_frame:
        blank_prism: bool = offer_frame["USA | PRISM Req ID"].isna()
        offer_frame.loc[blank_prism, "USA | PRISM Req ID"] = "LUSA-" + offer_frame.loc[
            blank_prism, "Job ID"
        ].astype(str)

    offer_frame["Offer Date"] = pd.NaT
    offer_frame["Offer Year"] = pd.NA
    offer_frame["Offer Pair"] = pd.NA
    offer_frame["Offer Source"] = pd.NA

    if "Offer Accepted Date (OAD)" in offer_frame:
        offer_frame["Offer Date"] = offer_frame["Offer Accepted Date (OAD)"]
        blank_offer_date: bool = offer_frame["Offer Date"].isna()
        offer_frame.loc[blank_offer_date, "Offer Date"] = offer_frame.loc[
            blank_offer_date, "Last job step update"
        ]
    else:
        offer_frame["Offer Date"] = offer_frame["Last job step update"]

    if "Select the source of recruitment :" in offer_frame:
        offer_frame["Offer Source"] = offer_frame["Select the source of recruitment :"]

    if "Source" in offer_frame:
        blank_offer_source: bool = offer_frame["Offer Source"].isna()
        offer_frame.loc[blank_offer_source, "Offer Source"] = offer_frame.loc[
            blank_offer_source, "Source"
        ]

    offer_frame["Time in Process"] = (
        offer_frame["Offer Date"] - offer_frame["JI | Recruitment Start Date"]
    ).dt.days.astype(int)

    offer_frame.loc[offer_frame["Time in Process"] < 0, "Time in Process"] = 0
    offer_frame["Offer Year"] = offer_frame["Offer Date"].dt.strftime("%Y")
    offer_frame["Offer Pair"] = (
        offer_frame["Job ID"].astype(str) + "-" + offer_frame["Person ID"].astype(str)
    )

    if use_cols:
        missing_cols: list[str] = list(
            set(use_cols) - set(offer_frame.columns.to_list())
        )
        for missing_col in missing_cols:
            offer_frame[missing_col] = pd.NA

        offer_frame = offer_frame.loc[:, use_cols]

    offer_frame = offer_frame.rename(columns=col_rename)

    return offer_frame.drop_duplicates(subset=dedup_cols)
