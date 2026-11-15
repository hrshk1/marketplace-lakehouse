"""Lookup data used to conform free-text address fields."""

from __future__ import annotations

from pyspark.sql import SparkSession

# Former or colloquial city names mapped to the current official name.
CITY_ALIASES: dict[str, str] = {
    "bangalore": "Bengaluru",
    "bombay": "Mumbai",
    "calcutta": "Kolkata",
    "madras": "Chennai",
    "gurgaon": "Gurugram",
    "mysore": "Mysuru",
    "cochin": "Kochi",
    "baroda": "Vadodara",
    "trivandrum": "Thiruvananthapuram",
    "vizag": "Visakhapatnam",
}

STATE_CODES: dict[str, str] = {
    "AP": "Andhra Pradesh",
    "AS": "Assam",
    "BR": "Bihar",
    "CH": "Chandigarh",
    "DL": "Delhi",
    "GJ": "Gujarat",
    "HR": "Haryana",
    "KA": "Karnataka",
    "KL": "Kerala",
    "MH": "Maharashtra",
    "MP": "Madhya Pradesh",
    "OD": "Odisha",
    "PB": "Punjab",
    "RJ": "Rajasthan",
    "TN": "Tamil Nadu",
    "TS": "Telangana",
    "UK": "Uttarakhand",
    "UP": "Uttar Pradesh",
    "WB": "West Bengal",
}

CITY_ALIAS_VIEW = "ref_city_alias"
STATE_CODE_VIEW = "ref_state_code"


def register_reference_views(spark: SparkSession) -> None:
    """Expose the lookups as temp views so the Silver SQL can join to them."""
    spark.createDataFrame(
        list(CITY_ALIASES.items()), "alias string, city string"
    ).createOrReplaceTempView(CITY_ALIAS_VIEW)
    spark.createDataFrame(
        [(code.lower(), state) for code, state in STATE_CODES.items()], "alias string, state string"
    ).createOrReplaceTempView(STATE_CODE_VIEW)
