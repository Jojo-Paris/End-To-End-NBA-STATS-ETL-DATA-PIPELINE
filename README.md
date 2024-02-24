# NBA Stats ETL Project

## Overview

This project focuses on extracting, transforming, and loading (ETL) NBA statistics data into an AWS RDS PostgreSQL database. The ETL pipeline is implemented using Python, PySpark, AWS S3, Apache Airflow, and pandas.

## Project Structure

- **`data_extraction`**: Contains scripts for extracting NBA statistics data.
- **`data_transformation`**: Includes the ETL pipeline for transforming the raw data.
- **`data_loading`**: Manages the loading of transformed data into AWS RDS PostgreSQL.
- **`airflow_dags`**: Airflow DAGs for scheduling and orchestrating the ETL pipeline.

## Technologies Used

- Python
- PySpark
- AWS RDS PostgreSQL
- AWS S3
- Apache Airflow
- pandas
- Git

## Getting Started

### Prerequisites

- Python 3.x
- PySpark
- AWS account with S3 and RDS PostgreSQL access
- Apache Airflow

### Installation

1. Clone the repository: `git clone <repository_url>`
2. Install required dependencies: `pip install -r requirements.txt`

## Usage

1. Extract raw NBA data using scripts in `data_extraction`.
2. Place the extracted data into a AWS S3 bucket
3. Transform the data using the ETL pipeline in `data_transformation`.
4. Load the transformed data into AWS RDS PostgreSQL with scripts in `data_loading`.
5. Schedule and orchestrate the entire ETL pipeline using Apache Airflow.
