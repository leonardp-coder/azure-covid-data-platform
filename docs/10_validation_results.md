# Data Validation Results — Azure COVID Data Engineering Platform

## 1. Overview

Data validation was performed on the original Azure COVID Data Engineering project to verify the correctness and consistency of the analytical datasets.

The validation covered:
- Gold dimension and fact table record counts.
- Fact table business-grain uniqueness.
- Dimension-key and foreign-key integrity.
- Null and negative measure profiling.
- Population percentage validation.
- Parquet export record-count reconciliation.

The results documented below represent the validated original project outputs.

## 2. Gold Table Record Counts

The Gold layer consists of three dimension tables and four fact tables.

| Table | Expected Records |
|---|---:|
| dim_country | 211 |
| dim_date | 298 |
| dim_age_group | 6 |
| fact_covid_daily | 64,635 |
| fact_hospital_daily | 8,110 |
| fact_hospital_weekly | 867 |
| fact_population_age | 3,816 |
| **Total** | **77,943** |

The expected record counts were established from the original project validation.

## 3. Fact Table Grain Validation

Each fact table was validated against its defined business grain.

| Fact Table | Business Grain | Validation |
|---|---|---|
| fact_covid_daily | Geography and date | Duplicate-grain validation |
| fact_hospital_daily | Country, date and indicator | Duplicate-grain validation |
| fact_hospital_weekly | Country, year-week and indicator | Duplicate-grain validation |
| fact_population_age | Geography, age group and year | Duplicate-grain validation |

The original validation confirmed that the fact tables satisfied their expected grain requirements.

## 4. Foreign-Key Validation

Foreign-key checks were performed to validate relationships between fact tables and their corresponding dimensions.

The validation confirmed that applicable non-null foreign keys matched their dimension records.

Some fact records intentionally contain null country keys.

These include:
- Regional aggregate records in `fact_covid_daily`.
- Regional and special-geography records in `fact_population_age`.

These null keys are intentional and should not be treated as orphaned country references.

The hospital daily fact passed its dimension-key matching checks.

The weekly hospital fact does not use a date dimension foreign key.

## 5. COVID Daily Fact Validation

The `fact_covid_daily` table contains 64,635 records.

The output consists of:

| Record Category | Records |
|---|---:|
| Country-level records | 62,878 |
| Regional aggregate records | 1,757 |
| **Total** | **64,635** |

Regional aggregate records retain null country keys because their geographical grain does not correspond to an individual country.

## 6. Hospital Fact Validation

### Daily Hospital Fact

The `fact_hospital_daily` table contains 8,110 records.

Validation confirmed:
- Expected record count.
- Business-grain checks.
- Successful matching of applicable dimension keys.

### Weekly Hospital Fact

The `fact_hospital_weekly` table contains 867 records.

The validation identified:
- 22 null measure values.
- No negative measure values.
- No duplicate business-grain violations in the validated output.

Weekly records are represented using the reporting period rather than a date dimension key.

## 7. Population Fact Validation

The `fact_population_age` table contains 3,816 records.

The dataset covers:

| Attribute | Validated Coverage |
|---|---:|
| Age groups | 6 |
| Geographies | 53 |
| Years | 12 |
| Year range | 2008–2019 |

### Data Quality Findings

| Validation Metric | Result |
|---|---:|
| Null population percentages | 264 |
| Null country keys | 432 |
| Negative population percentages | 0 |
| Population percentages above 100 | 0 |
| Null age-group keys | 0 |

The null population percentages and country keys were retained where applicable rather than incorrectly treating them as invalid records.

## 8. Parquet Export Validation

All seven Gold tables were exported to Parquet for downstream Azure SQL loading.

The exported record counts were reconciled against the corresponding Gold table counts.

| Exported Dataset | Expected Records |
|---|---:|
| dim_country | 211 |
| dim_date | 298 |
| dim_age_group | 6 |
| fact_covid_daily | 64,635 |
| fact_hospital_daily | 8,110 |
| fact_hospital_weekly | 867 |
| fact_population_age | 3,816 |
| **Total** | **77,943** |

The original project validation confirmed that the Parquet exports matched the Gold output counts.

## 9. Validation Summary

The original project validation established the following:

- All seven Gold datasets matched their expected record counts.
- Fact-table grain checks passed.
- Applicable non-null foreign-key checks passed.
- Intentional null country keys were preserved.
- Population percentages contained no negative or above-100 values.
- The weekly hospital fact contained 22 null measures and no negative values.
- All seven Parquet exports matched their corresponding Gold record counts.

## 10. Validation Scope and Limitations

These results belong to the original Azure project execution.

The GitHub repository contains the refactored modular Python implementation and SQL scripts. The refactored code has not yet been independently redeployed and executed against Azure.

Therefore, the results above are reference validation baselines and should not be interpreted as fresh execution results from the GitHub version.
