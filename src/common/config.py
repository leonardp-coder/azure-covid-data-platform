"""
Central configuration for the Azure COVID Data Engineering project.
"""

# Databricks Unity Catalog
CATALOG = "dbw_covid_de_dev"

# Unity Catalog schemas
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"
GOLD_SCHEMA = "gold"


# Bronze tables
BRONZE_TABLES = {
    "cases_deaths": f"{CATALOG}.{BRONZE_SCHEMA}.cases_deaths",
    "hospital_admissions": f"{CATALOG}.{BRONZE_SCHEMA}.hospital_admissions",
    "population": f"{CATALOG}.{BRONZE_SCHEMA}.population",
    "country_lookup": f"{CATALOG}.{BRONZE_SCHEMA}.country_lookup",
}


# Silver tables
SILVER_TABLES = {
    "cases_deaths": f"{CATALOG}.{SILVER_SCHEMA}.cases_deaths",
    "hospital_admissions": f"{CATALOG}.{SILVER_SCHEMA}.hospital_admissions",
    "population": f"{CATALOG}.{SILVER_SCHEMA}.population",
    "country_lookup": f"{CATALOG}.{SILVER_SCHEMA}.country_lookup",
}


# Gold schema reference
GOLD_SCHEMA_NAME = f"{CATALOG}.{GOLD_SCHEMA}"
