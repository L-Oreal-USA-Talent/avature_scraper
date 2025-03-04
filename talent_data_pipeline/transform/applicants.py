import pandas as pd
from pandas import DataFrame, StringDtype, Int64Dtype


def create_applicant_table(
    app_frame: DataFrame,
    applicant_cols: list[str],
    applicant_type: str,
    rename_cols: dict[str, str],
) -> DataFrame:
    """
    Create a dimensional table of applicants.
    The transformed table will using the columns specified in `BASE_APPLICANTS_COLS`.
    Columns will be renamed according to AVATURE_RENAMED_COLUMNS specified in environment.

    :param rename_cols:
    :param app_frame: DataFrame containing the applicant data.
    :param applicant_cols: Dict that indicates the type of Avature profile and columns to keep for that profile
        (e.g. `"External": ["Person ID", "Gender]`).
    :param applicant_type: A string indicating the type of applicant. Choose between "External" or "Internal".
    :return: DataFrame containing the transformed and deduplicated applicant data.
    """

    if app_frame.empty:
        print("Applicant frame was empty. Returning empty frame.")
        return app_frame

    applicant_frame: DataFrame = app_frame.copy()

    if applicant_type == "External":
        # Early careers team generally asks mandatory form questions that
        # provide diversity and university info
        if "University" in applicant_frame.columns:
            applicant_frame.loc[
                applicant_frame["University"].str.contains("Other"), "University"
            ] = applicant_frame.loc[
                applicant_frame["University"].str.contains("Other"), "University.1"
            ]
            applicant_frame["University"] = applicant_frame["University"].str.upper()

        if "What is your ethnicity?" in applicant_frame.columns:
            applicant_frame.loc[
                applicant_frame["DD | Race"].isin([pd.NA, "I Prefer Not to Answer"]),
                "DD | Race",
            ] = applicant_frame.loc[
                applicant_frame["DD | Race"].isin([pd.NA, "I Prefer Not to Answer"]),
                "What is your ethnicity?",
            ]

        if {"Source", "Other source"}.issubset(applicant_frame.columns):
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

    elif applicant_type == "Internal":
        applicant_frame = applicant_frame.replace(
            {"EI | Carol Gender 🔒": {"F": "Female", "M": "Male"}}
        )

    applicant_frame = applicant_frame.drop(
        columns=[
            "What is your ethnicity?",
            "Other source",
            "University.1",
        ],
        errors="ignore",
    )

    # Standardize schema in case app_frame does not
    # contain all columns specified in applicant_cols
    missing_applicant_cols: list[str] = list(
        set(applicant_cols) - set(applicant_frame.columns.to_list())
    )
    if missing_applicant_cols:
        for missing_col in missing_applicant_cols:
            applicant_frame[missing_col] = pd.NA

    applicant_frame: DataFrame = applicant_frame.loc[:, applicant_cols]
    applicant_frame["Applicant Type"] = applicant_type
    applicant_frame = applicant_frame.rename(columns=rename_cols)

    return applicant_frame.drop_duplicates(subset=["Person ID"])
