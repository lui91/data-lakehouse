# Databricks data Lakehouse.

## Project Diagram

![General process](/diagram/data_lakehouse.drawio.png "General process")

## Steps

1. [Schema creation](schema_creation.py).
2. Different data types found in files so [manual integrity check](check_data_integrity.py) performed.
3. [Data load](data_load.py) using copy into.
4. [Data transformation](transformations.py).
5. [Machine learning dataset](dataset_creation.py) creation.

# Configuration

1. Create service principal in Azure active directory.
2. Configure databricks CLI.
3. Create databricks secret scope.
4. Add secret.
5. Grant databricks access to GitHub.

## Useful links

[Kaggle dataset](https://www.kaggle.com/datasets/robikscube/flight-delay-dataset-20182022?select=Combined_Flights_2022.csv)

[Data columns](/md_files/data_structure.md)

---
