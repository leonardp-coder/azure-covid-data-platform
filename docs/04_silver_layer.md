# Silver Layer — Data Cleansing and Transformation

## 1. Overview

The Silver layer is responsible for transforming raw Bronze datasets into cleaned, standardised and structured datasets suitable for analytical modelling.

The transformation logic is implemented using PySpark and organised into reusable Python modules.

The Silver layer processes four datasets:
- COVID cases and deaths
- Hospital admissions
- Population by age group
- Country reference data

## 2. Transformation Modules

The Silver transformation logic is maintained under:

`src/silver/`

| Module | Responsibility |
|---|---|
| `cases_deaths.py` | Cleans and standardises COVID cases and deaths data |
| `hospital.py` | Processes daily and weekly hospital indicators |
| `population.py` | Converts population data from wide to long format |
| `country_lookup.py` | Standardises country reference data and adds additional reference records |

## 3. COVID Cases and Deaths Transformation

The `cases_deaths.py` module processes the Bronze COVID dataset.

Key transformation activities include:

- Standardising country names and country codes.
- Standardising continent and indicator values.
- Converting date fields into the required date format.
- Classifying country-level and regional geographical records.
- Removing the unnecessary source column.
- Validating required fields and business keys.

The module checks for null values, negative daily counts, unexpected indicators and duplicate business keys.

The business key used for duplicate validation is:

`(country, date, indicator)`

## 4. Hospital Admissions Transformation

The `hospital.py` module processes hospital admission datasets containing daily and weekly indicators.

Key activities include:

- Separating daily and weekly hospital indicators.
- Standardising relevant data fields.
- Validating daily business keys.
- Validating weekly business keys.
- Profiling null values and invalid period formats.

The business keys are:

**Daily data:**

`(country, date, indicator)`

**Weekly data:**

`(country, year_week, indicator)`

The module maintains separate daily and weekly indicator configurations to support downstream fact table creation.

## 5. Population Transformation

The `population.py` module processes the population-by-age dataset.

The source dataset is provided in a wide format, with separate columns representing population age groups.

The transformation converts this structure into a long format suitable for analytical processing.

Key activities include:

- Reading population data from the Bronze layer.
- Unpivoting age-group columns into individual records.
- Standardising geography codes and year values.
- Classifying geographical records.
- Converting population values into numeric types.
- Preserving source-related flags and ingestion metadata.
- Performing data quality checks.

The resulting dataset supports population analysis by geography, year and age group.

## 6. Country Reference Transformation

The `country_lookup.py` module prepares the country reference dataset.

Key activities include:

- Standardising country names.
- Standardising two-digit and three-digit country codes.
- Standardising continent values.
- Converting population values into the required numeric type.
- Adding additional reference records.
- Validating duplicate country codes and required fields.

Additional reference records include:

- Montserrat
- Kosovo
- Wallis and Futuna

These records are included to support geographical reference coverage across the datasets.

Population values are allowed to remain null where the additional reference records do not have population information.

## 7. Data Quality Validation

The Silver layer includes checks designed to identify data quality issues before analytical modelling.

| Validation | Purpose |
|---|---|
| Null checks | Identify missing required values |
| Duplicate checks | Identify duplicate business-key combinations |
| Negative value checks | Identify invalid negative measures where applicable |
| Indicator validation | Identify unexpected indicator values |
| Geography validation | Check geographical classification |
| Date validation | Check date and period fields |
| Source metadata checks | Maintain ingestion metadata compatibility |

The validation approach differs by dataset. Some checks are fail-fast validations, while others provide warning-based profiling.

## 8. Delta Table Publishing

After transformation and validation, the cleaned DataFrames are published into the configured Silver schema in Unity Catalog.

The modules separate extraction, transformation, validation and publishing responsibilities.

The Spark session is passed explicitly to functions that require Spark operations, improving reusability and testability.

## 9. Design Considerations

- Keep transformations modular and dataset-specific.
- Standardise source attributes before analytical modelling.
- Preserve source metadata for traceability.
- Validate business keys before publishing.
- Separate mandatory validation from warning-based quality profiling.
- Use reusable PySpark functions.
- Publish standardised datasets through Unity Catalog.

## 10. Output

The Silver layer provides cleaned and standardised Delta datasets for the Gold dimensional modelling process.

It acts as the controlled transformation boundary between raw ingestion and analytical data modelling.
