# Deployment Guide — Azure COVID Data Engineering Platform

## 1. Overview

This document describes the deployment sequence and configuration requirements for the Azure COVID Data Engineering Platform.

The platform consists of Azure Data Factory, Azure Data Lake Storage Gen2, Azure Databricks, Unity Catalog, Azure SQL Database and Power BI.

The deployment process is organised into infrastructure setup, data ingestion, transformation, warehouse loading and reporting.

## 2. Prerequisites

Before deployment, ensure the following resources and tools are available:

- Azure subscription with the required resource permissions.
- Azure Data Lake Storage Gen2 storage account.
- Azure Data Factory instance.
- Azure Databricks workspace with Unity Catalog enabled.
- Databricks Access Connector with managed identity.
- Azure SQL Database.
- SQL client such as SQL Server Management Studio.
- GitHub repository containing the project source code and SQL scripts.
- Power BI Desktop for reporting.

## 3. Azure Storage Setup

1. Create or identify the ADLS Gen2 storage account.
2. Enable hierarchical namespace.
3. Create the required filesystem or container.
4. Create the project folder structure.

```text
covid/
├── landing/
│   ├── cases_deaths/
│   ├── hospital_admissions/
│   ├── population/
│   └── country_lookup/
├── processed/
├── curated/
└── exports/
    └── azure_sql/
```

5. Upload or configure access to the source datasets.
6. Assign the required storage permissions to the Databricks managed identity.

## 4. Azure Databricks and Unity Catalog Setup

1. Configure the Databricks workspace.
2. Enable Unity Catalog.
3. Configure the Access Connector managed identity.
4. Create the required storage credential.
5. Create an external location for the approved storage path.
6. Create the project catalog:

`dbw_covid_de_dev`

7. Create the `bronze`, `silver` and `gold` schemas.
8. Configure the required volume for landing data.
9. Grant appropriate permissions to the identities and users.

The project uses Unity Catalog volumes and managed identity-based storage access rather than legacy DBFS mounts.

## 5. Azure Data Factory Setup

1. Create or configure the Azure Data Factory instance.
2. Configure the required linked services.
3. Configure access to the source datasets and ADLS Gen2.
4. Create the required datasets.
5. Configure Copy activities for source ingestion.
6. Configure the required Mapping Data Flows.
7. Configure the pipeline dependencies and execution sequence.
8. Validate that the source files are available in the landing directories.

## 6. Databricks Code Deployment

The Python code is maintained in the GitHub repository under `src/`.

The modules are organised into:

- `src/bronze/`
- `src/silver/`
- `src/gold/`
- `src/common/`

Deployment steps:

1. Retrieve the project source code from GitHub.
2. Configure the required Python package and workspace dependencies.
3. Configure the catalog, schemas and storage paths in the project configuration.
4. Make the source modules available to the Databricks execution environment.
5. Configure the appropriate Databricks job or notebook entry points.
6. Pass the active Spark session to the processing functions.
7. Execute the ingestion and transformation workflows in the required order.

The GitHub refactoring should be tested in the target Databricks environment before being treated as a production deployment.

## 7. Azure SQL Database Setup

Connect directly to the target Azure SQL Database using an authorised SQL client.

Execute the SQL scripts in the following order:

1. `01_schema_setup`
2. `02_staging_tables`
3. `03_analytics_tables`
4. `04_audit`
5. `05_stored_procedures_load`
6. `06_stored_procedure_log`
7. `07_validation`

Review the security script and grant the required permissions to the Azure Data Factory service principal.

Verify that the staging tables, analytics tables, audit objects and stored procedures are created successfully.

## 8. Data Loading and Warehouse Execution

The execution sequence is:

1. Execute the Azure Data Factory ingestion pipeline.
2. Verify that the landing datasets are available.
3. Execute Bronze ingestion.
4. Execute Silver transformations.
5. Execute Gold dimension and fact transformations.
6. Run Gold audit checks.
7. Export the seven Gold datasets as Parquet files.
8. Load the exported datasets into Azure SQL staging tables.
9. Execute the full-load stored procedure.
10. Review the SQL audit and validation results.

The full-load procedure uses a full-refresh strategy.

## 9. Power BI Configuration

1. Open Power BI Desktop.
2. Connect to the target Azure SQL Database.
3. Select the required analytics tables.
4. Configure the dimensional relationships.
5. Verify the model and measures.
6. Validate report totals against the warehouse data.
7. Publish the report to the appropriate Power BI workspace if required.

## 10. Post-Deployment Validation

After deployment, verify:

- Source datasets are available in the landing layer.
- Bronze Delta tables are populated.
- Silver transformations complete successfully.
- Gold dimensions and facts are created.
- Expected Gold record counts are reconciled.
- Parquet exports are available.
- Azure SQL staging tables are populated.
- Analytics tables contain the expected records.
- SQL audit and failure logging operate correctly.
- Power BI reports return expected results.

## 11. Deployment Considerations

- Use environment-specific configuration rather than hardcoded resource names.
- Avoid storing credentials in source code.
- Use managed identities wherever supported.
- Validate permissions before pipeline execution.
- Deploy database objects in dependency order.
- Test stored procedures before running full warehouse loads.
- Keep source code and SQL scripts under version control.
- Validate the refactored code in Azure before promoting it to a production environment.

## 12. Deployment Status

The original Azure project and its Power BI model were validated.

The GitHub repository contains a modular refactoring of the project code and the SQL deployment scripts.

The GitHub-refactored code has not yet been independently redeployed and executed against Azure. This deployment guide describes the intended setup and execution sequence, not a claim that a fresh deployment has already been completed.
