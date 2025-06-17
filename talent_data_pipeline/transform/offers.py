import pandas as pd
from pandas import DataFrame, Series


def transform_offers(
    offer_frame: DataFrame,
    applicant_type: str,
    offer_status: str,
    dedup_cols: list[str],
    offer_cols: list[str] | None = None,
) -> DataFrame:
    """Apply a series of transformations to offer data.

    Args:
        offer_frame (DataFrame): DataFrame that must contain `Person ID` and `Job ID` and other columns noted in the specification.
        applicant_type (str): Choose betweeen "External" or "Internal"
        offer_status (str): Choose between "Accepted", "Rejected", "Reneged"
        dedup_cols (list[str]): Columns to deduplicate the frame. Most likely, ['Person ID', 'Job ID']
        offer_cols (list[str] | None, optional):
            Defaults to None. Columns that should be returned in the final frame.
            If the columni n offer_cols does not exist, it will be created as a blank column.

    Returns:
        DataFrame: Transformed DataFrame.
    """
    if offer_frame.empty:
        print("DataFrame is empty. Returning original empty frame.")
        return offer_frame

    offer_frame_cols = offer_frame.columns

    offer_frame["Applicant Type"] = applicant_type
    offer_frame["Offer Status"] = offer_status
    offer_frame.loc[
        offer_frame["JI | Recruitment Start Date"].isna(), "JI | Recruitment Start Date"
    ] = offer_frame.loc[offer_frame["JI | Recruitment Start Date"].isna(), "Date"]

    # USA | PRISM Req ID was the de-facto way of reporting out
    # jobs prior to One Profile
    if "USA | PRISM Req ID" in offer_frame_cols:
        blank_prism: Series[bool] = offer_frame["USA | PRISM Req ID"].isna()
        offer_frame.loc[blank_prism, "USA | PRISM Req ID"] = "LUSA-" + offer_frame.loc[
            blank_prism, "Job ID"
        ].astype(str)

    # Instantiate columns
    offer_frame["Offer Date"] = pd.NaT
    offer_frame["Offer Year"] = pd.NA
    offer_frame["Offer Pair"] = pd.NA
    offer_frame["Offer Source"] = pd.NA
    offer_frame["Pipeline candidate?"] = pd.NA

    if "Offer Accepted Date (OAD)" in offer_frame_cols:
        offer_frame["Offer Date"] = offer_frame["Offer Accepted Date (OAD)"]
        blank_offer_date: Series[bool] = offer_frame["Offer Date"].isna()
        offer_frame.loc[blank_offer_date, "Offer Date"] = offer_frame.loc[
            blank_offer_date, "Last job step update"
        ]
    else:
        offer_frame["Offer Date"] = offer_frame["Last job step update"]

    # As of 2025, there is a dedicated Pipeline question in the integration form
    if "Was the candidate in your pipeline before being hired?" in offer_frame_cols:
        offer_frame["Pipeline candidate?"] = offer_frame[
            "Was the candidate in your pipeline before being hired?"
        ]
        offer_frame = offer_frame.drop(
            columns=["Was the candidate in your pipeline before being hired?"]
        )
    # Before the One Profile form, a custom question to get the pipeline results.
    if "Did the candidate come from a pipeline?" in offer_frame_cols:
        offer_frame["Pipeline candidate?"] = offer_frame[
            "Did the candidate come from a pipeline?"
        ]
        offer_frame = offer_frame.drop(
            columns=["Did the candidate come from a pipeline?"]
        )

    # As of 2025, `Select the source of recruitment` is the primary source
    # but in the case that the source is blank, we can use the source indicated by the
    # applicant as the backup
    if "Select the source of recruitment :" in offer_frame_cols:
        offer_frame["Offer Source"] = offer_frame["Select the source of recruitment :"]
    if "Source" in offer_frame_cols:
        blank_offer_source: Series[bool] = offer_frame["Offer Source"].isna()
        offer_frame.loc[blank_offer_source, "Offer Source"] = offer_frame.loc[
            blank_offer_source, "Source"
        ]

        # Pipeline results were also captured in the overall "Source" question but have a
        # dedicated field in the integration form as of 2025
        pipeline_as_source: Series[bool] = offer_frame["Offer Source"] == "Pipeline"
        offer_frame.loc[pipeline_as_source, "Pipeline candidate?"] = "Yes"

    offer_frame["Pipeline candidate?"] = offer_frame["Pipeline candidate?"].fillna("No")

    offer_frame["Time in Process"] = (
        offer_frame["Offer Date"] - offer_frame["JI | Recruitment Start Date"]
    ).dt.days.astype(int)

    # In cases where the dates are modified such that the TTH could be negative
    offer_frame.loc[offer_frame["Time in Process"] < 0, "Time in Process"] = 0
    offer_frame["Offer Year"] = offer_frame["Offer Date"].dt.strftime("%Y")
    offer_frame["Offer Pair"] = (
        offer_frame["Job ID"].astype(str) + "-" + offer_frame["Person ID"].astype(str)
    )
    # offer_cols is useful to maintain a consistent schema across offer data from
    # various years where the schema has likely differed
    if offer_cols:
        missing_cols: list[str] = list(
            set(offer_cols) - set(offer_frame.columns.to_list())
        )
        for missing_col in missing_cols:
            offer_frame[missing_col] = pd.NA

        offer_frame = offer_frame.loc[:, offer_cols]

    return offer_frame.drop_duplicates(subset=dedup_cols)
