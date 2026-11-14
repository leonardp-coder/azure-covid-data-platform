
"""
Central configuration for the Azure COVID Data Engineering project.

Contains non-sensitive project settings and Unity Catalog references.
"""

# Unity Catalog
CATALOG = "dbw_covid_de_dev"

# Unity Catalog schemas
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"
GOLD_SCHEMA = "gold"

# ADLS Gen2 landing volume
LANDING_PATH = "/Volumes/dbw_covid_de_dev/silver/covid_landing"

# Bronze table references
BRONZE_TABLES = {
    "cases_deaths": f"{CATALOG}.{BRONZE_SCHEMA}.cases_deaths",
    "hospital_admissions": f"{CATALOG}.{BRONZE_SCHEMA}.hospital_admissions",
    "country_lookup": f"{CATALOG}.{BRONZE_SCHEMA}.country_lookup",
    "population_by_age": f"{CATALOG}.{BRONZE_SCHEMA}.population_by_age",
}

# Silver table references
SILVER_TABLES = {
    "cases_deaths": f"{CATALOG}.{SILVER_SCHEMA}.cases_deaths",
    "hospital_admissions": f"{CATALOG}.{SILVER_SCHEMA}.hospital_admission",
    "country_lookup": f"{CATALOG}.{SILVER_SCHEMA}.country_lookup",
    "population_by_age": f"{CATALOG}.{SILVER_SCHEMA}.population_by_age",
}

# Gold schema reference
GOLD_SCHEMA_NAME = f"{CATALOG}.{GOLD_SCHEMA}"
