# Bronze Layer — Data Ingestion

## 1. Overview

The Bronze layer is the initial data processing layer of the Azure COVID Data Engineering Platform.

It ingests source datasets from the ADLS Gen2 landing zone and stores them as Delta tables in the Bronze schema.

The primary objective is to preserve the ingested source data while adding ingestion metadata and establishing a reliable foundation for downstream transformations.

## 2. Source Datasets

The Bronze ingestion process handles four datasets:

| Dataset | Source Format | Bronze Table |
|---|---|---|
| COVID cases and deaths | CSV | cases_deaths |
| Hospital admissions | CSV | hospital_admissions |
| Country lookup | CSV | country_lookup |
| Population by age | GZIP TSV | population_by_age |

## 3. Ingestion Architecture

The ingestion process follows this sequence:

1. Read source files from the ADLS Gen2 landing volume.
2. Apply dataset-specific file-reading configurations.
3. Add ingestion metadata.
4. Write the DataFrames into Bronze Delta tables.
5. Reconcile source and target record counts.
6. Log ingestion results.

## 4. Ingestion Metadata

The ingestion process adds metadata columns to support data lineage and traceability.

| Column | Purpose |
|---|---|
| `ingested_at` | Records the ingestion timestamp |
| `_source_file` | Identifies the originating source file |

These attributes help identify when a dataset was ingested and which source file contributed the records.

## 5. Storage Format

The Bronze layer uses Delta Lake as its storage format.

Delta tables provide a structured foundation for downstream Spark processing and support reliable table operations.

The Bronze tables are stored under the configured Unity Catalog Bronze schema.

## 6. Reusable Python Implementation

The ingestion logic is maintained in:

`src/bronze/ingestion.py`

The module separates ingestion responsibilities into reusable functions.

The implementation includes:

- Dataset-specific ingestion configuration.
- File reading for CSV and compressed TSV sources.
- Metadata enrichment.
- Bronze Delta table writing.
- Source-to-target record count reconciliation.
- Dataset name validation.
- Logging of ingestion activities.

The Spark session is passed explicitly to the relevant functions rather than creating separate Spark sessions inside the ingestion module.

This makes the code easier to test, maintain and reuse.

## 7. Data Quality and Reconciliation

After writing each Bronze table, the ingestion process compares source and target record counts.

This helps identify discrepancies during ingestion.

Dataset name validation also helps prevent unexpected dataset references from being processed.

The Bronze layer focuses primarily on ingestion integrity. Business-level cleansing and validation are handled in the Silver layer.

## 8. Error Handling and Logging

The project uses a centralised logging utility maintained in:

`src/common/logging_utils.py`

The logging utility provides consistent log formatting and severity levels across the Python modules.

The Bronze ingestion process records operational information, including ingestion activities and record counts.

Exceptions should be logged with their traceback and re-raised so that failed processing is not silently treated as successful.

## 9. Design Considerations

- Preserve source data before applying business transformations.
- Maintain ingestion metadata for traceability.
- Use Delta tables as the Bronze storage format.
- Keep ingestion logic reusable and modular.
- Reconcile source and target record counts.
- Separate ingestion validation from business-level data quality checks.
- Use Unity Catalog-managed table references for governed access.

## 10. Processing Strategy

The current implementation uses full-refresh batch ingestion.

It does not implement streaming ingestion or incremental CDC processing.

The Bronze layer provides the foundation for the Silver transformation modules.
