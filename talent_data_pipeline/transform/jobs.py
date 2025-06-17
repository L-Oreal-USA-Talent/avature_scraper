import numpy as np
import pandas as pd
from pandas import DataFrame, Series, Timestamp


def transform_jobs(
    jobs_frame: DataFrame, dedup_field: list[str], run_date: Timestamp
) -> DataFrame:
    """Run a series of transformation steps on job data.

    Args:
        jobs_frame (DataFrame): DataFrame that must contain the `Job ID` and other columns noted in the specification.
        dedup_field (list[str]): A list of field(s) by which to deduplicate data. More than likely, you will only pass ['Job ID']
        run_date (Timestamp): The reference datetime that the report should use to calculate `Days open`.

    Returns:
        DataFrame: Transformed DataFrame of jobs.
    """

    if jobs_frame.empty:
        print("DataFrame is empty. Returning original empty frame.")
        return jobs_frame

    # Create a PRISM Rwq ID column to maintain backwards compability with
    # historical data prior to SucecssFactors
    if "USA | PRISM Req ID" not in jobs_frame.columns:
        jobs_frame.loc[:, "USA | PRISM Req ID"] = "LUSA-" + jobs_frame["Job ID"].astype(
            str
        )

    # JI | Recruitment Start Date was introduced in mid-2024 so reqs prior to this
    # period will not have this column
    jobs_frame.loc[:, "JI | Recruitment Start Date"] = (
        jobs_frame["JI | Recruitment Start Date"]
        .fillna(jobs_frame["Date"])
        .fillna(jobs_frame["Creation date"])
    )

    # In some cases, the JI | Code will display as "Closed" if the req was closed
    # and then re-opened.
    jobs_frame.loc[
        jobs_frame["JI | Code"].astype(str).str.contains("Closed"), "JI | Code"
    ] = jobs_frame.loc[
        jobs_frame["JI | Code"].astype(str).str.contains("Closed"),
        "JI | Code (Reference)",
    ]

    # Roles that are still in the draft stage may still have an empty job title
    jobs_frame.loc[
        jobs_frame["Ad | (Default) Job Title"].isna(), "Ad | (Default) Job Title"
    ] = jobs_frame.loc[jobs_frame["Ad | (Default) Job Title"].isna(), "Name"]

    # Group the job workflows into four distinct statuses
    conditions: list[Series[bool]] = [
        jobs_frame["Job workflow step"].str.contains("Open", na=False),
        jobs_frame["Job workflow step"].str.contains("Draft", na=False),
        jobs_frame["Job workflow step"].str.contains("On Hold", na=False),
        jobs_frame["Job workflow step"].str.contains("Closed|Filled|Cancel", na=False),
    ]

    choices = ["Open", "Draft", "On Hold", "Closed"]

    # Use np.select to create the new column
    jobs_frame["Job Status"] = np.select(conditions, choices, default="Closed")

    # Replace the native 'Days open' column as it is does not
    # begin counting days relative to the "JI | Recruitment Start Date"
    # Days open should count from the day the report is run
    # or the day it was closed in the system
    if "Days open" in jobs_frame.columns:
        jobs_frame = jobs_frame.drop(columns="Days open")
    if "Date closed" not in jobs_frame:
        jobs_frame.loc[:, "Job Year"] = run_date.year
        jobs_frame.loc[:, "Days open"] = (
            run_date - jobs_frame["JI | Recruitment Start Date"]
        ).dt.days  # type: ignore
    else:
        job_closed_mask: Series[bool] = jobs_frame["Job Status"] == "Closed"
        jobs_frame.loc[:, "Job Year"] = jobs_frame["Date closed"].dt.strftime("%Y")
        jobs_frame.loc[job_closed_mask, "Days open"] = (
            jobs_frame.loc[job_closed_mask, "Date closed"]
            - jobs_frame.loc[job_closed_mask, "JI | Recruitment Start Date"]
        ).dt.days

    jobs_frame.loc[jobs_frame["Days open"] < 0, "Days open"] = 0
    # Days open range in the form of [x, y).
    bins = [0, 30, 60, 90, 180, float("inf")]  # Define the bin edges
    labels = [
        "0 - 30 Days",
        "30 - 60 Days",
        "60 - 90 Days",
        "90 - 180 Days",
        "180+ Days",
    ]
    jobs_frame["Days Open Range"] = pd.cut(
        jobs_frame["Days open"], bins=bins, labels=labels, right=False
    )

    # Historical case where Canadian jobs were assigned to the Tech/IT recruitment team
    jobs_frame.loc[
        jobs_frame["JI | Country"] == "Canada", "USA | Job Mapping Level"
    ] = "Non Manager"

    jobs_frame.loc[:, "USA | Job Mapping Level"] = jobs_frame[
        "USA | Job Mapping Level"
    ].str.title()

    jobs_frame.loc[jobs_frame["JI | Location"].isna(), "JI | Location"] = (
        jobs_frame.loc[jobs_frame["JI | Location"].isna(), "JI | Contractual Location"]
    )

    # Temporary column to avoid .loc errors
    jobs_frame["Job Level Temp"] = jobs_frame["USA | Job Mapping Level"]

    # Split frames into those records that already have the
    # level, and those records that do not
    jobs_frame_with_level: DataFrame = jobs_frame.loc[
        jobs_frame["Job Level Temp"].notna()
    ].copy()

    jobs_frame_no_level: DataFrame = jobs_frame.loc[
        jobs_frame["Job Level Temp"].isna()
    ].copy()

    # Attempt to correct or fill the USA | Job Mapping Level using regex matchings
    level_conditions: list[Series[bool]] = [
        jobs_frame_no_level["Ad | (Default) Job Title"].str.contains(
            "Analyst", na=False
        ),
        jobs_frame_no_level["Ad | (Default) Job Title"].str.contains(
            "Senior Analyst", na=False
        ),
        jobs_frame_no_level["Ad | (Default) Job Title"].str.contains(
            "Account Executive", na=False
        ),
        jobs_frame_no_level["Ad | (Default) Job Title"].str.contains(
            "Asst Mgr|Assistant Manager|Assistant  Manager", na=False
        ),
        jobs_frame_no_level["Ad | (Default) Job Title"].str.contains(
            "Manager|Mgr", na=False
        ),
        jobs_frame_no_level["Ad | (Default) Job Title"].str.contains(
            "Sr. Mgr|Sr Mgr|Senior Manager|Sr. Manager", na=False
        ),
        jobs_frame_no_level["Ad | (Default) Job Title"].str.contains(
            "Dir|Director", na=False
        ),
        jobs_frame_no_level["Ad | (Default) Job Title"].str.contains(
            "AVP|Assistant Vice President|Asst. Vice President", na=False
        ),
        jobs_frame_no_level["Ad | (Default) Job Title"].str.contains(
            "VP|Vice President", na=False
        ),
        jobs_frame_no_level["Ad | (Default) Job Title"].str.contains(
            "SVP|Senior Vice President", na=False
        ),
    ]
    level_matches = [
        "Analyst",
        "Senior Analyst",
        "Account Executive",
        "Assistant Manager",
        "Manager",
        "Senior Manager",
        "Director",
        "Assistant Vice President",
        "Vice President",
        "Senior Vice President",
    ]
    jobs_frame_no_level["Job Level Temp"] = np.select(
        level_conditions, level_matches, default="Blank Level"
    )

    jobs_frame_fixed_levels: DataFrame = pd.concat(
        [jobs_frame_with_level, jobs_frame_no_level]
    )

    jobs_frame_fixed_levels = jobs_frame_fixed_levels.drop(
        columns=["USA | Job Mapping Level"]
    ).rename(columns={"Job Level Temp": "USA | Job Mapping Level"})

    return jobs_frame_fixed_levels.drop_duplicates(subset=dedup_field)
