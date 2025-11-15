# Sunglass Store ETL Pipeline

A complete **Extract, Transform, Load (ETL)** data pipeline for a sunglass e-commerce store that ingests data from AWS S3, loads it into PostgreSQL, and performs advanced data transformations using dbt (data build tool).

## 📋 Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Features](#features)
- [Project Structure](#project-structure)
- [Prerequisites](#prerequisites)
- [Data Models](#data-models)
- [Technologies](#technologies)

## 🎯 Overview

This project implements a production-ready ETL pipeline for a sunglass store's business intelligence platform. It handles:

- **User data** - Customer profiles, demographics, and registration information
- **Product catalog** - Sunglass inventory with specifications (brand, lens color, polarization, etc.)
- **Order transactions** - Purchase history and sales data
- **User interactions** - Customer engagement events (views, cart additions, purchases)

The pipeline employs modern data engineering best practices including:

- **Slowly Changing Dimensions (SCD Type 2)** for tracking historical changes
- **Incremental loading** for efficient data updates
- **Data validation** and quality checks
- **Dimensional modeling** for analytical queries

## 🏗 Architecture

```
┌─────────────────┐
│   AWS S3        │  Source: Parquet files
│   (Raw Data)    │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  dlt Pipeline   │  Extract & Load
│  (Python)       │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  PostgreSQL     │  Raw Data Layer
│  (Staging)      │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  dbt Models     │  Transform
│  (SQL)          │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Data Warehouse │  Analytics Layer
│  (PostgreSQL)   │  • Dimensions
│                 │  • Facts
│                 │  • Metrics
└─────────────────┘
```

## ✨ Features

### ETL Pipeline (`etl_sunglass_store/postgres_pipeline.py`)

- **Smart Loading Strategies:**

  - SCD Type 2 for dimension tables (users, products) with validity tracking
  - Incremental append for fact tables (orders, interactions)
  - Upsert for reference data (interaction types)

- **Modular Design:**

  - Configurable source creation
  - Environment-based configuration
  - Reusable components

- **Error Handling & Logging:**
  - Comprehensive logging
  - Pipeline state management
  - Automatic retry mechanisms (via dlt)

### dbt Transformations (`dbt_sunglass_store/`)

#### Staging Models

Clean and standardize raw data:

- `stg_users` - User profiles
- `stg_products` - Product catalog
- `stg_orders` - Order transactions
- `stg_interactions` - User engagement events
- `stg_interaction_types` - Interaction reference data

#### Dimensional Models

Business entities for analytics:

- `dim_users` - Customer dimension
- `dim_products` - Product dimension
- `dim_dates` - Date dimension for time-based analysis
- `dim_interactions` - Interaction type dimension

#### Fact Tables

Measurable business events:

- `fct_orders` - Sales transactions with price information
- `fct_users_summary` - User-level aggregations
- `fct_products_summary` - Product-level aggregations

#### Metrics & Analytics

Pre-calculated business metrics:

- `monthly_sales` - Revenue trends over time
- `monthly_active_users` - User engagement metrics
- `platform_sales` - Cross-platform performance
- `age_distribution` - Customer demographic analysis
- `total_revenue` - Overall revenue calculations

## 📁 Project Structure

```
Sunglass Store ETL/
├── etl_sunglass_store/          # ETL Pipeline
│   ├── postgres_pipeline.py     # Main pipeline orchestration
│   └── config.py                # Configuration management
│
├── dbt_sunglass_store/          # dbt Transformations
│   ├── models/
│   │   ├── staging/             # Cleaned raw data
│   │   ├── marts/
│   │   │   ├── dimensions/      # Business dimensions
│   │   │   ├── facts/           # Fact tables
│   │   │   └── metrics/         # Pre-calculated metrics
│   │   └── sources.yml          # Source definitions
│   ├── dbt_project.yml          # dbt configuration
│   └── packages.yml             # dbt dependencies
│
├── .env.example                 # Environment template
├── .gitignore                   # Git exclusions
├── SECURITY.md                  # Security guidelines
├── pyproject.toml               # Python dependencies
└── README.md                    # This file
```

## 🔧 Prerequisites

- **Python 3.10+**
- **PostgreSQL 13+**
- **AWS S3 Access** (with appropriate credentials)
- **dbt Core**
- **Git**

## 📊 Data Models

### Source Tables (Raw Layer)

| Table              | Description            | Load Strategy        |
| ------------------ | ---------------------- | -------------------- |
| `users`            | Customer profiles      | SCD Type 2           |
| `products`         | Product catalog        | SCD Type 2           |
| `orders`           | Purchase transactions  | Incremental (append) |
| `interaction`      | User engagement events | Incremental (append) |
| `interactionTypes` | Interaction reference  | Upsert               |

### Dimension Tables

| Table              | Description        | Key Columns                          |
| ------------------ | ------------------ | ------------------------------------ |
| `dim_users`        | Customer dimension | user_id, email, age, gender, country |
| `dim_products`     | Product dimension  | item_id, product_name, brand, price  |
| `dim_dates`        | Date dimension     | date_actual, year, month, quarter    |
| `dim_interactions` | Interaction types  | id, interaction_name                 |

### Fact Tables

| Table                  | Description          | Grain               |
| ---------------------- | -------------------- | ------------------- |
| `fct_orders`           | Sales transactions   | One row per order   |
| `fct_users_summary`    | User aggregations    | One row per user    |
| `fct_products_summary` | Product aggregations | One row per product |

### Metrics

| Metric                 | Description           | Use Case             |
| ---------------------- | --------------------- | -------------------- |
| `monthly_sales`        | Revenue by month      | Trend analysis       |
| `monthly_active_users` | Active users by month | Engagement tracking  |
| `platform_sales`       | Sales by platform     | Channel performance  |
| `age_distribution`     | Customer age groups   | Demographic insights |
| `total_revenue`        | Overall revenue       | Executive dashboard  |

## 🛠 Technologies

| Technology                | Purpose                           |
| ------------------------- | --------------------------------- |
| **Python 3.13**           | Core programming language         |
| **dlt (data load tool)**  | ETL orchestration & data loading  |
| **dbt (data build tool)** | SQL-based transformations         |
| **PostgreSQL**            | Data warehouse                    |
| **AWS S3**                | Data lake storage (Parquet files) |
| **Parquet**               | Columnar data format              |
| **UV**                    | Fast Python package manager       |

## 📝 License

This project is part of a data engineering portfolio.

## 👤 Author

**Ayush Acharya**

- GitHub: [@ayushacharya007](https://github.com/ayushacharya007)

## 🙏 Acknowledgments

- Built with [dlt](https://dlthub.com/)
- Transformations powered by [dbt](https://www.getdbt.com/)
- Inspired by modern data engineering best practices
