import pandas as pd
from pandas import DataFrame, StringDtype


def create_applicant_table(
    app_frame: DataFrame,
    applicant_type: str,
    applicant_cols: list[str] | None = None,
) -> DataFrame:
    """Run a series of transformation steps to clean applicant data.

    Args:
        app_frame (DataFrame): DataFrame that must contain `Person ID` among other columns noted in the specification.
        applicant_type (str): Choose between "External" or "Internal".
        applicant_cols (list[str] | None, optional):
            Defaults to None. Columns that should be returned in the final frame.
            If the column in applicant_cols does not exist, it will be created as a blank column.
    Returns:
        DataFrame: Transformed DataFrame of applicant data.
    """
    if app_frame.empty:
        print("Applicant frame was empty. Returning empty frame.")
        return app_frame

    applicant_frame: DataFrame = app_frame.copy()
    applicant_frame_cols = applicant_frame.columns

    if applicant_type == "External":
        # Early careers team generally asks mandatory form questions that
        # provide diversity and university info
        applicant_frame["Temp University"] = pd.NA

        if "University" in applicant_frame_cols:
            applicant_frame["Temp University"] = applicant_frame["University"]
            applicant_frame.loc[
                applicant_frame["Temp University"].str.contains("Other"),
                "Temp University",
            ] = applicant_frame.loc[
                applicant_frame["Temp University"].str.contains("Other"), "University.1"
            ]

        # In case the form is not available, extract univeresity info
        # from the native Avature university table
        if "PI | University" in applicant_frame_cols:
            applicant_frame.loc[
                applicant_frame["Temp University"].isna(), "Temp University"
            ] = applicant_frame.loc[
                applicant_frame["Temp University"].isna(), "PI | University"
            ]

        applicant_frame = applicant_frame.drop(
            columns=["University", "University.1", "PI | University"], errors="ignore"
        )
        applicant_frame = applicant_frame.rename(
            columns={"Temp University": "University"}
        )

        applicant_frame["University"] = applicant_frame["University"].astype(
            StringDtype()
        )
        applicant_frame["University"] = applicant_frame["University"].str.upper()

        if "What is your ethnicity?" in applicant_frame_cols:
            applicant_frame.loc[
                applicant_frame["DD | Race"].isin([pd.NA, "I Prefer Not to Answer"]),
                "DD | Race",
            ] = applicant_frame.loc[
                applicant_frame["DD | Race"].isin([pd.NA, "I Prefer Not to Answer"]),
                "What is your ethnicity?",
            ]

        if {"Source", "Other source"}.issubset(applicant_frame_cols):
            applicant_frame.loc[
                applicant_frame["Source"].str.contains("Other"), "Source"
            ] = applicant_frame.loc[
                applicant_frame["Source"].str.contains("Other"), "Other source"
            ]

        cols_to_fill: dict[str, str] = {
            "DD | Gender": "Prefer not say",
            "DD | Race": "I Prefer Not to Answer",
            "DD | US Veterans": "I prefer not to answer",
            "DD | Disability": "I don’t wish to answer",
        }

        applicant_frame = applicant_frame.fillna(cols_to_fill)

    # Internal applicant data is generally of high quality so
    # it only requires fixing the default gender to match external applicant
    # gender choices
    elif applicant_type == "Internal":
        applicant_frame = applicant_frame.replace(
            {"EI | Carol Gender 🔒": {"F": "Female", "M": "Male"}}
        )

    # applicant_cols is useful to maintain a consistent schema across
    # applicant data from various years where
    # the schema has likely differed
    if applicant_cols:
        missing_applicant_cols: list[str] = list(
            set(applicant_cols) - set(applicant_frame.columns.to_list())
        )
        for missing_col in missing_applicant_cols:
            applicant_frame[missing_col] = pd.NA

        applicant_frame: DataFrame = applicant_frame.loc[:, applicant_cols]

    # Normalize the date columns as a last step

    # Treat the cases where the date field is exported as a datetime stamp from Avature
    # as opposed to the expected `DD-MM-YYYY` format that predominates in Avature

    applicant_frame_cols = applicant_frame.columns
    if "Link to job date" in applicant_frame_cols:
        applicant_frame["New Date"] = pd.to_datetime(
            applicant_frame["Link to job date"].dt.date
        )
        applicant_frame = applicant_frame.drop(columns=["Link to job date"])
        applicant_frame = applicant_frame.rename(
            columns={"New Date": "Link to job date"}
        )

    if "Visit date" in applicant_frame_cols:
        applicant_frame["New Visit Date"] = pd.to_datetime(
            applicant_frame["Visit date"].dt.date
        )
        applicant_frame = applicant_frame.drop(columns=["Visit date"])
        applicant_frame = applicant_frame.rename(
            columns={"New Visit Date": "Visit date"}
        )

    # By this point, the dates have been corrected so they can be used to
    # retrieve the latest records of applicant data
    if "Link to job date" or "Visit date" in applicant_frame_cols:
        applicant_frame = applicant_frame.rename(
            columns={"Link to job date": "Date", "Visit date": "Date"}
        )

        # Sort by Date so that the latest entry is kept
        applicant_frame = applicant_frame.sort_values(
            by=["Person ID", "Date"], ascending=[True, False]
        )

    applicant_frame["Applicant Type"] = applicant_type

    return applicant_frame.drop_duplicates(subset=["Person ID"], keep="first")
