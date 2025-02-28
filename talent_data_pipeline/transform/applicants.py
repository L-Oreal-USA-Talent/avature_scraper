import pandas as pd
from pandas import DataFrame, StringDtype, Int64Dtype

BASE_APPLICANT_COLS: dict[str, list[str]] = {
    "External": [
        "Person ID",
        "DD | Gender",
        "DD | Race",
        "DD | US Veterans",
        "DD | Disability",
        "PI | Education",
        "PI | Work History",
        "Source",
        "Other source",
    ],
    "Internal": [
        "Person ID",
        "EI | Employee ID (CAROL)",
        "EI | Person ID (GPZ)",
        "EI | Employee current zone 🔒",
        "EI | Employee Entity 🔒",
        "EI | Carol Gender 🔒",
    ],
    "Internship": [
        "Person ID",
        "DD | Gender",
        "DD | Race",
        "DD | US Veterans",
        "DD | Disability",
        "PI | Education",
        "PI | Work History",
        "Source",
        "Other source",
        "What is your ethnicity?",
        "University",
        "University.1",
    ],
    "Registrant": [
        "Person ID",
        "DD | Gender",
        "DD | Race",
        "DD | US Veterans",
        "DD | Disability",
    ],
}


def create_applicant_table(
    app_frame: DataFrame,
    base_col_type: str,
    applicant_type: str,
    rename_cols: dict[str, str],
) -> DataFrame:
    """
    Create a dimensional table of applicants. The transformed table will
    use the columns specified in `BASE_APPLICANTS_COLS`.
    Columns will be renamed according to FlowConfig.AVATURE_RENAMED_COLUMNS.
    :param rename_cols:
    :param app_frame: DataFrame containing the applicant data.
    :param base_col_type: A string indicating the type of applicant columns to use.
                          Choose between "External", "Internal", or "Internship".
    :param applicant_type: A string indicating the type of applicant.
                           Choose between "External" or "Internal".
    :return: DataFrame containing the transformed and deduplicated applicant data.
    """

    if app_frame.empty:
        return app_frame

    if base_col_type not in BASE_APPLICANT_COLS:
        raise ValueError(
            f"{base_col_type} is not a recognized applicant column set. \n"
            f" Please pick between 'External', 'Internal', or 'Internship'."
        )

    applicant_cols: list[str] = BASE_APPLICANT_COLS[base_col_type]

    # Standardize schema among various annual datasets
    missing_applicant_cols: list[str] = list(
        set(applicant_cols) - set(app_frame.columns.to_list())
    )
    for missing_col in missing_applicant_cols:
        app_frame[missing_col] = pd.NA
    applicant_frame: DataFrame = app_frame.loc[:, applicant_cols]

    if base_col_type == "External" or base_col_type == "Registrant":
        cols_to_replace: dict[str, str] = {
            "DD | Gender": "Prefer not say",
            "DD | Race": "I Prefer Not to Answer",
            "DD | US Veterans": "I prefer not to answer",
            "DD | Disability": "I don’t wish to answer",
        }

        for col_, fill_val in cols_to_replace.items():
            applicant_frame[col_] = applicant_frame[col_].fillna(fill_val)

    elif base_col_type == "Internal":
        applicant_frame = applicant_frame.replace(
            {"EI | Carol Gender 🔒": {"F": "Female", "M": "Male"}}
        )
    else:
        # Early careers team generally asks mandatory form questions that
        # provide diversity and university info
        applicant_frame.loc[
            applicant_frame["DD | Race"].isin([pd.NA, "I Prefer Not to Answer"]),
            "DD | Race",
        ] = applicant_frame.loc[
            applicant_frame["DD | Race"].isin([pd.NA, "I Prefer Not to Answer"]),
            "What is your ethnicity?",
        ]

        applicant_frame.loc[
            applicant_frame["University"].str.contains("Other"), "University"
        ] = applicant_frame.loc[
            applicant_frame["University"].str.contains("Other"), "University.1"
        ]

        applicant_frame["University"] = applicant_frame["University"].str.title()

        applicant_frame.loc[
            applicant_frame["Source"].str.contains("Other"), "Source"
        ] = applicant_frame.loc[
            applicant_frame["Source"].str.contains("Other"), "Other source"
        ]

        applicant_frame = applicant_frame.drop(
            columns=["What is your ethnicity?", "University.1"],
            errors="ignore",
        )

    applicant_frame["Applicant Type"] = applicant_type
    applicant_frame = applicant_frame.rename(columns=rename_cols)

    return applicant_frame.drop_duplicates(subset=["Person ID"])
