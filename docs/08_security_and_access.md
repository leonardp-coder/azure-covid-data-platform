# Security and Access Management — Azure COVID Data Platform

## 1. Overview

Security and access management are important considerations in the Azure COVID Data Engineering Platform.

The solution uses Azure identity and access management, Azure Databricks Unity Catalog and SQL database permissions to control access to data and platform resources.

The objective is to avoid unnecessary credential exposure and provide controlled access across the data engineering workflow.

## 2. Azure Data Lake Storage Gen2 Access

Azure Data Lake Storage Gen2 is used as the central storage layer for the project.

Access to storage is configured through Azure identity-based authentication.

The project uses an Azure Databricks Access Connector with a managed identity for Unity Catalog storage access.

This avoids relying on hardcoded storage account keys or SAS tokens in the Databricks application code.

## 3. Azure Databricks Unity Catalog

Unity Catalog provides centralised governance for the Databricks data environment.

The project uses Unity Catalog to organise data into catalogs, schemas and tables.

The configured catalog is:

`dbw_covid_de_dev`

The project separates data into the following schemas:

- `bronze`
- `silver`
- `gold`

Unity Catalog volumes provide governed access to the configured ADLS Gen2 landing location.

The landing volume is referenced in the project configuration.

## 4. Managed Identity and Storage Credentials

The Databricks Access Connector provides a managed identity that can be granted the required permissions on the Azure storage account.

The Unity Catalog storage credential uses this identity to authenticate with the storage resource.

An external location associates the storage credential with an approved storage path.

This provides a controlled method of accessing cloud storage through Unity Catalog.

## 5. Unity Catalog Access Control

Unity Catalog provides permissions at different levels of the data hierarchy.

Access can be controlled for:

- Catalogs.
- Schemas.
- Tables.
- Volumes.
- External locations.

Permissions should be granted according to the responsibilities of the user or service principal.

The principle of least privilege should be followed when granting access.

## 6. Azure SQL Database Security

The project includes a SQL security script for the Azure Data Factory service principal.

The service principal is intended to receive the permissions required to interact with the staging, analytics and audit objects.

These permissions include access to relevant database objects and execution rights for the loading and failure-logging procedures.

The SQL deployment should use a dedicated service identity with only the permissions required by the pipeline.

## 7. Credential Management

Sensitive credentials must not be hardcoded into Python modules or committed to GitHub.

Recommended practices include:

- Use managed identities where supported.
- Store application secrets in Azure Key Vault when secret-based authentication is required.
- Avoid committing passwords, client secrets, connection strings and SAS tokens.
- Restrict access to secrets and storage resources.
- Rotate credentials according to organisational security policies.

## 8. Network and Resource Access

Access to Azure resources should be restricted through the relevant Azure resource permissions and network configuration.

Storage and database access should be granted only to approved identities and services.

Additional network controls, such as private endpoints and restricted firewall rules, can be introduced where required by the deployment environment.

## 9. Security Design Principles

- Prefer managed identity over long-lived credentials.
- Apply least-privilege access.
- Use Unity Catalog for governed data access.
- Keep secrets outside source control.
- Separate storage authentication from application code.
- Restrict database permissions to required operations.
- Review identity assignments and access permissions periodically.

## 10. Implementation Scope

The project includes managed-identity-based Unity Catalog storage access and SQL service-principal permission scripts.

Additional enterprise controls such as centralised secret rotation, private endpoints, network isolation and Azure Monitor security monitoring should be treated as deployment enhancements unless separately configured and verified.
