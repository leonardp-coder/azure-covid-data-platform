# Gold Layer — Dimensional Data Modelling

## 1. Overview

The Gold layer is the final transformation layer of the Azure COVID Data Engineering Platform.

It converts standardised Silver datasets into an analytical dimensional model designed for reporting and business intelligence.

The Gold layer consists of three dimension tables and four fact tables.

The transformation logic is maintained under:

`src/gold/`

## 2. Dimensional Model

### Dimension Tables

| Dimension | Description | Expected Records |
|---|---|---:|
| `dim_country` | Country and geographical reference information | 211 |
| `dim_date` | Date dimension covering the project period | 298 |
| `dim_age_group` | Population age-group reference information | 6 |

### Fact Tables

| Fact Table | Description | Expected Records |
|---|---|---:|
| `fact_covid_daily` | Daily COVID cases and deaths | 64,635 |
| `fact_hospital_daily` | Daily hospital metrics | 8,110 |
| `fact_hospital_weekly` | Weekly hospital metrics | 867 |
| `fact_population_age` | Population percentages by age group and geography | 3,816 |

The expected record counts are based on the validated original project outputs.

## 3. Dimension Table Design

### 3.1 dim_country

Stores country reference information used by the fact tables.

The dimension supports geographical analysis using country identifiers and reference attributes.

The country dimension includes 211 records.

### 3.2 dim_date

Provides a reusable date dimension for time-based analysis.

The dimension covers the period from 2 January 2020 to 25 October 2020, containing 298 records.

It supports consistent date-based reporting across applicable fact tables.

### 3.3 dim_age_group

Stores the age-group reference values used by the population fact table.

The dimension contains six age groups and provides a standardised reference for population analysis.

## 4. Fact Table Design

### 4.1 fact_covid_daily

Stores daily COVID case and death metrics.

**Grain:** One record per geography and date.

Business grain:

`(geography_name, date_key)`

The fact contains 64,635 records, comprising:
- 62,878 country-level records.
- 1,757 regional aggregate records.

Regional aggregate records intentionally have a null `country_key` because they do not map to a country-level dimension record.

### 4.2 fact_hospital_daily

Stores daily hospital admission metrics.

**Grain:** One record per country, date and indicator.

Business grain:

`(country, date, indicator)`

The fact contains 8,110 records.

Dimension-key validation confirmed that all applicable dimension keys matched successfully.

### 4.3 fact_hospital_weekly

Stores weekly hospital admission metrics.

**Grain:** One record per country, reporting week and indicator.

Business grain:

`(country, year_week, indicator)`

The fact contains 867 records.

The reporting period is represented using `year_week`, with separate ISO year and week attributes.

The weekly fact does not use a date dimension foreign key.

The validated output contains 22 null measure values and no negative values.

### 4.4 fact_population_age

Stores population percentages by geography, year and age group.

**Grain:** One record per geography, age group and year.

Business grain:

`(geo_code, age_group_key, year)`

The fact contains 3,816 records and covers:
- Six age groups.
- 53 geographies.
- Twelve years, from 2008 to 2019.

The validated output contains:
- 264 null population percentages.
- 432 null country keys.
- No negative population percentages.
- No population percentages above 100.
- No null age-group keys.

Null country keys are intentionally allowed for regional and special geographical codes that do not have a matching country dimension record.

## 5. Dimension Relationships

The dimensional model uses the following relationships where applicable:

| Fact Table | Dimension Relationship |
|---|---|
| `fact_covid_daily` | Country and date |
| `fact_hospital_daily` | Country and date |
| `fact_hospital_weekly` | Country |
| `fact_population_age` | Country and age group |

The country relationship for population data uses the two-digit country code reference.

The COVID and hospital datasets use the three-digit country code reference.

Regional and special geographical records may not have a matching country key. These null keys are intentional and are preserved in the model.

## 6. Gold Transformation Modules

The Gold transformation logic is organised into reusable modules:

| Module | Responsibility |
|---|---|
| `dim_country.py` | Creates the country dimension |
| `dim_date.py` | Creates the date dimension |
| `dim_age_group.py` | Creates the age-group dimension |
| `fact_covid_daily.py` | Builds the daily COVID fact |
| `fact_hospital_daily.py` | Builds the daily hospital fact |
| `fact_hospital_weekly.py` | Builds the weekly hospital fact |
| `fact_population_age.py` | Builds the population-age fact |
| `gold_audit.py` | Performs Gold layer validation and profiling |

## 7. Data Quality and Auditing

The Gold audit module includes:

- Expected row-count validation.
- Schema and required-column validation.
- Dimension-key validation.
- Foreign-key orphan checks.
- Fact-grain duplicate checks.
- Null and negative measure profiling.
- Population percentage range validation.

The audit functions return DataFrames containing validation results and profiling information.

Null foreign keys are excluded from orphan-key checks where they are intentionally permitted by the project model.

## 8. Export to Parquet

After Gold table creation and validation, all seven analytical tables are exported as Parquet datasets.

These exports are stored under the Azure SQL export directory in ADLS Gen2.

The exported record counts were reconciled against the corresponding Gold tables.

## 9. Design Considerations

- Maintain a clear and documented grain for every fact table.
- Reuse conformed dimensions where applicable.
- Preserve valid regional and special-geography records.
- Separate daily and weekly analytical grains.
- Validate dimension relationships and fact uniqueness.
- Maintain consistent source-to-target record counts.
- Keep transformation and audit logic modular.

## 10. Validated Gold Output Summary

| Category | Tables | Total Records |
|---|---:|---:|
| Dimensions | 3 | 515 |
| Facts | 4 | 77,428 |
| **Total** | **7** | **77,943** |

The record counts represent the validated original project outputs and serve as reference baselines for the portfolio implementation.
