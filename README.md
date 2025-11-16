# Sunglass Store ETL Pipeline

A complete ETL (Extract, Transform, Load) pipeline for sunglass store data, extracting from AWS S3, loading into PostgreSQL, and transforming with dbt.

## 🏗️ Architecture

```
AWS S3 (Parquet) → dlt → PostgreSQL (Raw) → dbt → PostgreSQL (Marts)
```

**Data Flow:**

1. **Extract**: Read Parquet files from S3 using dlt
2. **Load**: Load raw data into PostgreSQL (`sunglass_store_raw` schema)
3. **Transform**: Run dbt models to create staging and marts layers

## 📊 Data Models

### Staging Layer (`staging` schema)

- `stg_users` - Cleaned user demographics
- `stg_products` - Product catalog
- `stg_orders` - Order transactions
- `stg_interactions` - User-product interactions
- `stg_interaction_types` - Interaction type lookup

### Marts Layer (`marts` schema)

**Dimensions:**

- `dim_users` - User dimension
- `dim_products` - Product dimension
- `dim_interactions` - Enriched interactions
- `dim_dates` - Date dimension (2019-2025)

**Facts:**

- `fct_orders` - Order transactions with sales
- `fct_users_summary` - User behavior aggregates
- `fct_products_summary` - Product performance metrics

**Metrics:**

- `monthly_sales` - Revenue by month
- `total_revenue` - Lifetime revenue
- `monthly_active_users` - MAU tracking
- `platform_sales` - Revenue by acquisition channel
- `age_distribution` - Customer demographics

## 📁 Project Structure

```
.
├── etl_sunglass_store/       # ETL pipeline code
│   ├── postgres_pipeline.py  # Main pipeline script
│   ├── models.py              # Pydantic data models
│   └── config.py              # Configuration management
├── dbt_sunglass_store/        # dbt project
│   ├── models/
│   │   ├── staging/           # Staging models
│   │   └── marts/             # Dimensional models
│   ├── macros/                # Custom macros
│   └── dbt_project.yml        # dbt configuration
├── .env.example               # Environment template
├── SECURITY.md                # Security guidelines
└── README.md                  # This file
``` 

## 🛠️ Tech Stack

- **Python** - Core language
- **dlt** - Data loading tool
- **dbt-core** - Data transformation
- **dbt-postgres** - PostgreSQL adapter
- **PostgreSQL** - Data warehouse
- **Pydantic v2** - Data validation
- **AWS S3** - Data lake storage

## 👤 Author

**Ayush Acharya**

- GitHub: [@ayushacharya007](https://github.com/ayushacharya007)
