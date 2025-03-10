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
    `Link to job date` and `Visit date` are standardized to `YYYY-MM-DD` and renamed to `Date`.

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

    elif applicant_type == "Internal":
        applicant_frame = applicant_frame.replace(
            {"EI | Carol Gender 🔒": {"F": "Female", "M": "Male"}}
        )

    # Cases where the date field comes across as a datetime stamp
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
    applicant_frame = applicant_frame.rename(
        columns={"Link to job date": "Date", "Visit date": "Date"}
    )

    # Now, sort by Date so that the latest entry is kept
    applicant_frame = applicant_frame.sort_values(
        by=["Person ID", "Date"], ascending=[True, False]
    )

    return applicant_frame.drop_duplicates(subset=["Person ID"], keep="first")
