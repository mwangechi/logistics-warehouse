# Star Schema Data Warehouse for Logistics

A dimensional data warehouse built with [dbt](https://www.getdbt.com/) that transforms raw logistics data into a clean star schema optimized for analytics. Designed to run against BigQuery in production and DuckDB locally.

## Architecture

```
┌───────────────┐     ┌────────────┐     ┌──────────────┐     ┌──────────────┐
│  Raw Sources  │────▶│  Staging   │────▶│ Intermediate │────▶│    Marts     │
│  (Seeds/EL)   │     │  stg_*     │     │   int_*      │     │  dim_ / fct_ │
└───────────────┘     └────────────┘     └──────────────┘     └──────────────┘

Marts (Star Schema):
  ┌──────────────┐
  │ fct_shipments│──┐
  └──────────────┘  ├── dim_suppliers
                    ├── dim_products
                    └── dim_warehouses
```

## Features

- **Star Schema**: Fact table (`fct_shipments`) with three dimension tables
- **Layered Architecture**: Staging → Intermediate → Marts following dbt best practices
- **Data Quality**: Built-in tests for uniqueness, not-null, referential integrity, and business rules
- **Seed Data**: Realistic sample datasets for local development
- **Documentation**: Column-level YAML docs for all models
- **DuckDB Compatible**: Run the entire warehouse locally without cloud credentials

## Tech Stack

| Component | Technology |
|---|---|
| Transformation | dbt-core 1.7 |
| Production DB | Google BigQuery |
| Local DB | DuckDB |
| Data Modeling | Star Schema (Kimball) |

## Quick Start

### Prerequisites

- Python 3.11+
- pip

### 1. Install dbt

```bash
# For local development with DuckDB
pip install dbt-duckdb==1.7.0

# For production with BigQuery
pip install dbt-bigquery==1.7.0
```

### 2. Configure Profile

```bash
cp profiles.yml.example profiles.yml
# Edit profiles.yml with your connection details
```

### 3. Install Packages & Load Seeds

```bash
dbt deps
dbt seed
```

### 4. Run Models

```bash
# Run all models
dbt run

# Run only marts
dbt run --select marts.*

# Run with full refresh
dbt run --full-refresh
```

### 5. Test

```bash
dbt test
```

### 6. Generate Documentation

```bash
dbt docs generate
dbt docs serve
```

## Project Structure

```
logistics-warehouse/
├── dbt_project.yml
├── profiles.yml.example
├── packages.yml
├── seeds/                     # Sample CSV data
│   ├── raw_shipments.csv
│   ├── raw_inventory.csv
│   └── raw_suppliers.csv
├── models/
│   ├── staging/               # 1:1 source cleaning
│   │   ├── _staging.yml
│   │   ├── stg_shipments.sql
│   │   ├── stg_inventory.sql
│   │   └── stg_suppliers.sql
│   ├── intermediate/          # Business logic joins
│   │   └── int_shipment_enriched.sql
│   └── marts/                 # Star schema output
│       ├── _marts.yml
│       ├── dim_suppliers.sql
│       ├── dim_products.sql
│       ├── dim_warehouses.sql
│       └── fct_shipments.sql
├── macros/
│   └── generate_surrogate_key.sql
├── tests/
│   └── assert_shipment_positive_qty.sql
└── analyses/
    └── shipment_trends.sql
```

## Star Schema

| Table | Type | Description |
|---|---|---|
| `fct_shipments` | Fact | Shipment transactions with metrics |
| `dim_suppliers` | Dimension | Supplier master data |
| `dim_products` | Dimension | Product catalog with categories |
| `dim_warehouses` | Dimension | Warehouse locations and capacity |

## License

MIT
