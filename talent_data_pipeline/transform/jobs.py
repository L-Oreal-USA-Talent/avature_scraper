import pandas as pd
from pandas import DataFrame, Timestamp


def transform_jobs(
    jobs_frame: DataFrame, dedup_field: list[str], run_date: Timestamp
) -> DataFrame:
    """
    Return a transformed dataset of jobs.
    :param jobs_frame: DataFrame of jobs.
    :param dedup_field: The field(s) by which to deduplicate data.
    :param run_date: The date the script is executed.
    :return: Dataframe of transformed jobs.
    """

    if jobs_frame.empty:
        print("DataFrame is empty. Returning original empty frame.")
        return jobs_frame

    if "USA | PRISM Req ID" not in jobs_frame.columns:
        jobs_frame.loc[:, "USA | PRISM Req ID"] = "LUSA-" + jobs_frame["Job ID"].astype(
            str
        )

    jobs_frame.loc[:, "JI | Recruitment Start Date"] = (
        jobs_frame["JI | Recruitment Start Date"]
        .fillna(jobs_frame["Date"])
        .fillna(jobs_frame["Creation date"])
    )

    jobs_frame.loc[
        jobs_frame["JI | Code"].astype(str).str.contains("Closed"), "JI | Code"
    ] = jobs_frame.loc[
        jobs_frame["JI | Code"].astype(str).str.contains("Closed"),
        "JI | Code (Reference)",
    ]

    jobs_frame.loc[
        jobs_frame["Ad | (Default) Job Title"].isna(), "Ad | (Default) Job Title"
    ] = jobs_frame.loc[jobs_frame["Ad | (Default) Job Title"].isna(), "Name"]

    job_status: dict = {
        "Open": "Open",
        "Draft": "Draft",
        "On Hold": "On Hold",
        "Closed|Filled|Cancel": "Closed",
    }
    for workflow_text, status in job_status.items():
        jobs_frame.loc[
            jobs_frame["Job workflow step"].str.contains(workflow_text), "Job Status"
        ] = status

    if "Date closed" not in jobs_frame:
        jobs_frame.loc[:, "Job Year"] = run_date.year
        jobs_frame.loc[:, "Days open"] = (
            run_date - jobs_frame["JI | Recruitment Start Date"]
        ).dt.days

    else:
        job_closed_mask: bool = jobs_frame["Job Status"] == "Closed"
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

    # Old case where Canadian jobs were assigned to the Tech Team. Kept for historical data.
    jobs_frame.loc[
        jobs_frame["JI | Country"] == "Canada", "USA | Job Mapping Level"
    ] = "Non Manager"

    jobs_frame.loc[:, "USA | Job Mapping Level"] = jobs_frame[
        "USA | Job Mapping Level"
    ].str.title()

    jobs_frame.loc[jobs_frame["JI | Location"].isna(), "JI | Location"] = (
        jobs_frame.loc[jobs_frame["JI | Location"].isna(), "JI | Contractual Location"]
    )

    job_levels: dict[str, str] = {
        "Analyst": "Analyst",
        "Senior Analyst": "Senior Analyst",
        "Account Executive": "Account Executive",
        "Asst Mgr|Assistant Manager|Assistant  Manager": "Assistant Manager",
        "Manager|Mgr": "Manager",
        "Sr. Mgr|Sr Mgr|Senior Manager|Sr. Manager": "Senior Manager",
        "Dir|Director": "Director",
        "AVP|Assistant Vice President|Asst. Vice President": "Assistant Vice President",
        "VP|Vice President": "Vice President",
        "SVP|Senior Vice President": "Senior Vice President",
    }

    jobs_frame["Job Level Temp"] = jobs_frame["USA | Job Mapping Level"]

    jobs_frame_with_level: DataFrame = jobs_frame.loc[
        ~(jobs_frame["Job Level Temp"].isna())
    ].copy()

    jobs_frame_no_level: DataFrame = jobs_frame.loc[
        jobs_frame["Job Level Temp"].isna()
    ].copy()

    for level_regex, level in job_levels.items():
        jobs_frame_no_level.loc[
            jobs_frame_no_level["Ad | (Default) Job Title"].str.contains(level_regex),
            "Job Level Temp",
        ] = level

    jobs_frame_fixed_levels: DataFrame = pd.concat(
        [jobs_frame_with_level, jobs_frame_no_level]
    )

    jobs_frame_fixed_levels = jobs_frame_fixed_levels.drop(
        columns=["USA | Job Mapping Level"]
    ).rename(columns={"Job Level Temp": "USA | Job Mapping Level"})

    jobs_frame_fixed_levels["USA | Job Mapping Level"] = jobs_frame_fixed_levels[
        "USA | Job Mapping Level"
    ].fillna("Blank Level")

    return jobs_frame_fixed_levels.drop_duplicates(subset=dedup_field)
