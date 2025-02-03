# FELFEL Work Task - Minh Hoang Nguyen

## Overview
This repository contains a **dbt-based ELT pipeline** for processing FELFEL fridge inventory and order data. It includes **staging, dimensional, and fact models** to facilitate data exploration, reporting, and analytics.

## Project Structure
```
├── models/
│   ├── staging/               # Staging models (stg_*)
│   ├── dimensions/            # Dimension tables (dim_*)
│   ├── facts/                 # Fact tables (fct_*)
├── snapshots/                 # Snapshot tables (if enabled)
├── dbt_project.yml            # dbt configurations
├── tests/                     # Data quality tests
├── README.md                  # Project documentation
```

## Data Model

### Staging Layer (`stg_*`)
**Purpose:** Standardizes raw data (renaming, cleaning, type-casting) before further transformation.
- `stg_orders`
- `stg_product_item_supplier_batches`
- `stg_products`
- `stg_inventory_transitions`
- `stg_inventory_counts`
- `stg_inventory_stages`
- `stg_locations`

### Dimensional Models (`dim_*`)
**Purpose:** Enriches and denormalizes data for analytical queries.
- `dim_products` → Product details
- `dim_locations` → Fridge location details.
- `dim_inventory_stages` → Inventory lifecycle tracking.
- `dim_product_batches` → Product batch metadata.

### Fact Models (`fct_*`)
**Purpose:** Stores transactional data for aggregations & reporting.
- `fct_orders` → Sales transactions.
- `fct_inventory_transitions` → Inventory movements.
- `fct_inventory_counts` → Inventory stock corrections.

## Data Exploration (Task 2)
### 1. Trace ProductItemSupplierBatch (`F1672AB1-F568-42F7-AF05-2FA9117C1966`)
- Query `fct_inventory_transitions` to track movements.
- Validate `fct_inventory_counts` for stock discrepancies.
- Identify any missing transitions or unexpected events.

### 2. Calculate Inventory at Lausanne Office (2024-12-01 20:00:00.00)
- Filter `fct_inventory_transitions` for all movements **before** this timestamp.
- Subtract outgoing amounts from incoming.
- Adjust based on `fct_inventory_counts` for driver corrections.

## Data Architecture (Task 3)
### Goal: Enable querying fridge inventory at a specific time (`t`).

### Proposed Solution
1. **Materialized Inventory Snapshot (`inventory_snapshot`)**
   - Stores stock levels per **product, location, and timestamp**.
   - Updated incrementally.
   - Partitioned by date & clustered by location/product.

2. **Materialized Views for Live Queries**
   - Uses **window functions** to aggregate inventory up to time `t`.
   - Reduces query complexity for end users.

## How to Run dbt Models
### 1. Install dbt Dependencies
```sh
dbt deps
```

### 2. Run All Models with tests
```sh
dbt build
```

### 3. Run Tests
```sh
dbt test
```

### 4. Generate Documentation
```sh
dbt docs generate && dbt docs serve
```

## Next Steps
- Optimize snapshot refresh rates.
- Evaluate partitioning strategies for query cost optimization.

### Resources:
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [dbt community](https://getdbt.com/community) to learn from other analytics engineers
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices

