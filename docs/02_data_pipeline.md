# End-to-End Data Pipeline — Azure COVID Data Platform

## 1. Pipeline Overview

The Azure COVID Data Engineering Platform implements an end-to-end batch ingestion and transformation pipeline using Azure Data Factory, Azure Data Lake Storage Gen2, Azure Databricks, PySpark and Azure SQL Database.

The pipeline processes four source datasets:

1. COVID cases and deaths
2. Hospital admissions
3. Population by age group
4. Country reference data

Azure Data Factory orchestrates ingestion and data movement, while Databricks performs the data transformations and analytical modelling.

## 2. Source Datasets

| Dataset             | Source Format | Description                                       |
| ------------------- | ------------- | ------------------------------------------------- |
| Cases and deaths    | CSV           | COVID case and death statistics                   |
| Hospital admissions | CSV           | Daily and weekly hospital admission indicators    |
| Population by age   | GZIP TSV      | Population statistics by age group and geography  |
| Country lookup      | CSV           | Country names, codes and continent reference data |

## 3. Pipeline Execution Flow

The pipeline follows these major stages:

### Stage 1 — Data Ingestion Using Azure Data Factory

Azure Data Factory is responsible for orchestrating the ingestion process.

The ingestion process includes:

- Reading source files from their configured source locations.
- Using Copy activities to move source datasets into ADLS Gen2.
- Using Mapping Data Flows for the configured source transformations.
- Organising ingested data into the landing layer.

The source datasets are stored separately to maintain clear data organisation.

### Stage 2 — ADLS Gen2 Landing Layer

The landing layer acts as the initial storage location for incoming datasets.

Folder structure:

```text
covid/
└── landing/
    ├── cases_deaths/
    ├── hospital_admissions/
    ├── population/
    └── country_lookup/
```

The landing data is used as the input for Bronze ingestion.

### Stage 3 — Bronze Layer Processing

The Bronze ingestion module reads the landing datasets and writes them into Delta tables.

Key processing activities include:

- Reading CSV and compressed TSV files.
- Applying dataset-specific ingestion configurations.
- Adding ingestion timestamp metadata.
- Capturing source file information.
- Writing data into Bronze Delta tables.
- Reconciling source and target record counts.

The ingestion code is maintained in:

`src/bronze/ingestion.py`

### Stage 4 — Silver Layer Transformation

The Silver layer processes Bronze datasets into cleaned and standardised datasets.

The transformations are separated into reusable Python modules:

| Module              | Responsibility                                                            |
| ------------------- | ------------------------------------------------------------------------- |
| `cases_deaths.py`   | Standardises COVID cases and deaths data                                  |
| `hospital.py`       | Processes daily and weekly hospital indicators                            |
| `population.py`     | Converts population data from wide to long format                         |
| `country_lookup.py` | Standardises country reference data and adds additional reference records |

The Silver layer performs data type standardisation, business-key duplicate checks, geography classification and data quality validation.

Processed datasets are published as Silver Delta tables.

### Stage 5 — Gold Layer Transformation

The Gold layer builds the analytical dimensional model.

It produces three dimension tables and four fact tables.

| Table                  | Description                          |
| ---------------------- | ------------------------------------ |
| `dim_country`          | Country reference dimension          |
| `dim_date`             | Date dimension                       |
| `dim_age_group`        | Age-group dimension                  |
| `fact_covid_daily`     | Daily COVID cases and deaths         |
| `fact_hospital_daily`  | Daily hospital metrics               |
| `fact_hospital_weekly` | Weekly hospital metrics              |
| `fact_population_age`  | Population distribution by age group |

The Gold layer maintains the required relationships between facts and dimensions, while preserving valid regional and special-geography records.

### Stage 6 — Export to Azure SQL

The Gold Delta tables are exported as Parquet files for downstream loading.

Export location:

```text
covid/
└── exports/
    └── azure_sql/
        ├── dim_country/
        ├── dim_date/
        ├── dim_age_group/
        ├── fact_covid_daily/
        ├── fact_hospital_daily/
        ├── fact_hospital_weekly/
        └── fact_population_age/
```

The exported datasets are loaded into the corresponding Azure SQL staging tables.

### Stage 7 — Azure SQL Warehouse Loading

Azure SQL stored procedures transfer validated staging data into the analytical `analytics` schema.

The loading process follows this sequence:

1. Validate incoming staging data.
2. Check duplicate records and referential integrity.
3. Load dimension tables.
4. Load fact tables.
5. Record execution details in audit tables.
6. Capture failures through error-handling procedures.

The project uses a full-refresh loading strategy.

### Stage 8 — Power BI Reporting

Power BI uses the analytical tables in Azure SQL Database as its reporting source.

The dimensional model supports analysis of:

- COVID cases and deaths over time.
- Daily and weekly hospital admissions.
- Population distribution by age group.
- Geographical comparisons across countries and regions.

## 4. ADLS Gen2 Folder Organisation

The project separates storage into logical processing and export areas.

```text
covid/
├── landing/
│   ├── cases_deaths/
│   ├── hospital_admissions/
│   ├── population/
│   └── country_lookup/
│
├── processed/
│   ├── cases_deaths/
│   ├── hospital_admissions/
│   ├── population/
│   └── country_lookup/
│
├── curated/
│   ├── cases_deaths/
│   ├── hospital_admissions/
│   ├── population/
│   └── country_lookup/
│
└── exports/
    └── azure_sql/
        ├── dim_country/
        ├── dim_date/
        ├── dim_age_group/
        ├── fact_covid_daily/
        ├── fact_hospital_daily/
        ├── fact_hospital_weekly/
        └── fact_population_age/
```

## 5. Processing Strategy

The platform uses batch processing rather than real-time streaming.

The Azure SQL warehouse loading process uses full refreshes. The project does not implement CDC or incremental loading.

This approach is suitable for the static source datasets used in this project.

## 6. Pipeline Design Principles

- Separate ingestion from transformation.
- Organise datasets by processing layer.
- Use reusable Python modules.
- Maintain source-to-target record reconciliation.
- Apply data quality checks before analytical loading.
- Keep analytical transformations separate from reporting logic.
- Maintain SQL scripts and Python code under version control.
