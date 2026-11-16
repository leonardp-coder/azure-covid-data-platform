# Data Quality and Auditing — Azure COVID Data Platform

## 1. Overview

Data quality and validation are important components of the Azure COVID Data Engineering Platform.

The project implements validation at multiple stages of the data pipeline to identify missing values, duplicate records, invalid measures, schema inconsistencies and referential integrity issues.

The validation approach is distributed across the Bronze, Silver, Gold and Azure SQL layers.

## 2. Bronze Layer — Ingestion Validation

The Bronze ingestion process focuses on ingestion integrity and source-to-target reconciliation.

Key checks include:

- Dataset name validation.
- Source file availability and reading.
- Ingestion metadata capture.
- Source and target record count reconciliation.
- Logging of ingestion activities and failures.

The objective is to ensure that source records are successfully ingested into Bronze Delta tables.

## 3. Silver Layer — Data Quality Checks

The Silver layer performs dataset-specific validation before publishing transformed data.

### COVID Cases and Deaths

- Required-field null validation.
- Negative daily count validation.
- Unexpected indicator validation.
- Duplicate business-key validation.
- Geography classification validation.

Business key:

`(country, date, indicator)`

### Hospital Admissions

- Daily and weekly indicator validation.
- Daily duplicate checks.
- Weekly duplicate checks.
- Null-value profiling.
- Reporting-period format validation.

Business keys:

- Daily: `(country, date, indicator)`
- Weekly: `(country, year_week, indicator)`

### Population

- Required field and metadata validation.
- Numeric value conversion and quality profiling.
- Geography classification.
- Age-group and year coverage checks.
- Population percentage profiling.

### Country Lookup

- Required country reference validation.
- Country-code duplicate checks.
- Population data type validation.
- Country-name standardisation.
- Additional reference-record validation.

Some Silver checks are fail-fast validations, while others produce warnings and profiling information.

## 4. Gold Layer — Analytical Validation

The Gold audit module is maintained in:

`src/gold/gold_audit.py`

It provides reusable audit functions for the final analytical model.

### 4.1 Row Count Validation

The audit compares actual Gold table counts against expected project baselines.

| Table | Expected Records |
|---|---:|
| `dim_country` | 211 |
| `dim_date` | 298 |
| `dim_age_group` | 6 |
| `fact_covid_daily` | 64,635 |
| `fact_hospital_daily` | 8,110 |
| `fact_hospital_weekly` | 867 |
| `fact_population_age` | 3,816 |

### 4.2 Schema Validation

Checks that the Gold tables contain the expected columns required by downstream processing and reporting.

### 4.3 Dimension-Key Validation

Validates the availability and integrity of dimension keys.

### 4.4 Foreign-Key Validation

Checks fact-table foreign keys against the corresponding dimension tables.

Intentionally null foreign keys are excluded from orphan-key validation.

### 4.5 Fact-Grain Validation

Checks for duplicate records at the defined business grain of each fact table.

### 4.6 Measure Profiling

Profiles null and negative measure values.

Population percentages are additionally checked against the valid range of 0 to 100.

The audit functions return DataFrames containing validation and profiling results.

## 5. Azure SQL Validation

The Azure SQL loading process includes checks before data is transferred from staging into analytics tables.

Key validations include:

- Duplicate business-key checks.
- Foreign-key validation.
- Required data checks.
- Source and target record-count comparisons.
- Rejected-record tracking.
- Load execution and failure auditing.

These checks help maintain analytical warehouse consistency.

## 6. Operational Auditing

The project includes dedicated audit objects under the SQL `audit` schema.

These objects support:

- Pipeline execution tracking.
- Load-control information.
- Rejected-record tracking.
- Failure logging.

The failure-logging stored procedure captures SQL execution failures and supports troubleshooting.

## 7. Logging Strategy

The Python application uses a centralised logging utility:

`src/common/logging_utils.py`

The logger provides consistent timestamp, severity, module name and message formatting.

The current implementation uses a stream handler. In Databricks execution, logs can be viewed through the relevant driver or job logs, subject to workspace log configuration.

Centralised Azure Monitor or Log Analytics integration is not part of the current implementation.

## 8. Data Quality Design Principles

- Validate data as early as possible.
- Separate technical ingestion checks from business validation.
- Define explicit business keys for duplicate detection.
- Validate fact and dimension relationships.
- Preserve legitimate null values where the model permits them.
- Use fail-fast validation for critical data integrity rules.
- Use warning-based profiling for non-blocking quality observations.
- Maintain audit information for operational troubleshooting.

## 9. Validation Results

The original Azure project was validated using the expected Gold table counts and data-quality checks.

The seven Gold outputs contained a total of 77,943 records.

The results serve as reference baselines for the GitHub portfolio implementation.

The refactored GitHub code has not yet been independently executed against Azure. Its code review and the original project's validation results should therefore be understood as separate activities.
