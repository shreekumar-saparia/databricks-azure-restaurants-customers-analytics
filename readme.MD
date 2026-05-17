# 🍽️ Databricks Restaurant Analytics

A comprehensive analytics platform for restaurant performance, customer interactions, and operational efficiencies — built on Azure Databricks with a medallion architecture (Bronze → Silver → Gold).

---

## 🏗️ Architecture

![Architecture Diagram](/diagrams/Azure_Databricks_Restaurant_Analytics_Architecture.png)

---

## 🛠️ Tools & Technologies

| Category | Tools |
|---|---|
| **Ingestion** | Lakeflow Connect (batch & streaming), Lakeflow CDC |
| **Transformation** | Spark Declarative Pipeline |
| **Data Quality** | Databricks Data Quality |
| **Governance** | Unity Catalog |
| **Visualisation** | AI/BI Dashboard |
| **Azure Services** | Azure Event Hubs, Azure SQL Server, Azure Databricks |

---

## 📊 Data Model

![Dimensional Data Model](/diagrams/restaurant_analytics_data_model.png)

---

## 📁 Catalog Structure

![Catalog Schemas/Tables](/diagrams/catalog.png)

### `restaurant_analytics` Catalog

> Organizes data related to restaurant operations and analytics across four processing stages.

| Schema | Purpose |
|---|---|
| `00_landing` | Raw ingestion staging area |
| `01_bronze` | Foundational raw data — reviews, orders, historical orders |
| `02_silver` | Cleaned dimensional & fact tables for analytical queries |
| `03_gold` | Aggregated, report-ready datasets |

#### `01_bronze` Schema
Contains foundational data for restaurant operations — customer reviews, current orders, and historical orders. Use for analyzing customer feedback, tracking order trends, and examining sales history.

#### `02_silver` Schema
Contains fact tables (orders, order items, reviews) and dimension tables (customers, restaurants, menu items). Use for analyzing restaurant performance, customer behaviour, and order patterns.

#### `03_gold` Schema
Contains aggregated, finalized datasets:
- **`daily_sales_summary`** — date-based sales aggregates
- **`daily_customer_360`** — comprehensive daily customer profiles
- **`daily_restaurant_reviews`** — daily collected review data

---

## 🚀 Setup Guide

### Step 1 — Generate CSV Data Files

```bash
python run.py
```

Generates all CSV data files into the `data/` directory.

---

### Step 2 — Setup Streaming Data (Azure Event Hubs)

1. Create namespace `shree-eh-ns-dbxproject` in Azure Event Hubs with **Standard** pricing tier
2. Create an `orders` event hub inside the namespace
3. Create the following policies on the namespace:

| Policy Name | Type | Usage |
|---|---|---|
| `producer-send-policy` | Send | Push events to Event Hub |
| `datacricks-read-policy` | Listen | Read events from Event Hub |

4. Add the `producer-send-policy` primary connection string to your `.env` file
5. Run the producer script and verify events in Data Explorer:

```bash
python eventhub_orders.py
```

---

### Step 3 — Setup Azure SQL Database

Create an Azure SQL Server database:

| Setting | Value |
|---|---|
| Database name | `shree-restaurantops` |
| Server name | `shree-sqlserverops` |

---

### Step 4 — Load Data into Azure SQL

1. Create the following tables in `shree-restaurantops`:
   - `customers`
   - `historical_orders`
   - `menu_items`
   - `restaurants`
   - `reviews`

2. Use DataGrip or another SQL tool to load CSV data into the tables

3. Install Change Tracking & CDC utilities:
   - Run statements from **lines 55–106** of `azure_sql_database_setup.sql`

---

### Step 5 — Setup Databricks Environment

#### 5a. Create Workspace & Schemas

1. Create Azure Databricks workspace: `shree-databricks-workspace`
2. Create the following schemas in the Databricks catalog:
   - `00_landing`
   - `01_bronze`
   - `02_silver`
   - `03_gold`

#### 5b. Create Bronze Ingestion Pipeline

In **Jobs & Pipelines**, create an ingestion pipeline with **SQL Server** as source using **Change Data Capture**:

| Item | Name |
|---|---|
| Pipeline configuration | `shree-pipeline-ingestion-bronze` |
| Ingestion gateway | `shree-gw-ingestion-bronze` |
| Target schema | `01_bronze` |
| Tables to ingest | `historical_orders`, `reviews` |

#### 5c. Create Minimal Compute Policy

Create a custom policy (e.g. `minimal compute policy`) to minimise cost:

| Parameter | Type | Value |
|---|---|---|
| `num_workers` | Fixed | `1` |
| `driver_node_type_id` | Fixed | `Standard_E4d_v4` |
| `node_type_id` | Fixed | `Standard_F4s` *(or `Standard_D4s_v3` in West US)* |

#### 5d. Install & Configure Databricks CLI

```bash
# Install
brew tap databricks/tap
brew install databricks

# Configure
databricks configure
# Enter your workspace URL and personal access token
```

#### 5e. Attach Policy to Pipelines

```bash
# List all pipelines
databricks pipelines list-pipelines --output json

# Update gateway ingestion pipeline with compute policy
databricks pipelines update 3940495c-24eb-40bb-8463-f07fb7322d3f --json '{
  "name": "gw_ingestion",
  "catalog": "azure_databricks_analytics",
  "schema": "00_landing",
  "gateway_definition": {
    "connection_name": "conn_restaurantops",
    "gateway_storage_catalog": "azure_databricks_analytics",
    "gateway_storage_schema": "00_landing",
    "gateway_storage_name": "gw_ingestion"
  },
  "clusters": [
    {
      "label": "default",
      "policy_id": "0001861BBBC810D7",
      "apply_policy_default_values": true
    }
  ],
  "continuous": true
}'
```

---

### Step 6 — Setup Silver Ingestion Pipeline

In **Jobs & Pipelines**, create a second ingestion pipeline with **SQL Server** as source using **Change Data Capture**:

| Item | Name |
|---|---|
| Pipeline configuration | `shree-pipeline-ingestion-silver` |
| Ingestion gateway | `shree-gw-ingestion-silver` |
| Target schema | `02_silver` |

Table mappings:

| Source (SQL Server) | Target (Silver) |
|---|---|
| `customers` | `dim_customers` |
| `restaurants` | `dim_restaurants` |
| `menu_items` | `dim_menu_items` |

---

## 🔁 Pipelines

| Pipeline | Description | Diagram |
|---|---|---|
| Streaming (Event Hub → Bronze) | Consumes orders from Azure Event Hub into bronze layer | ![](/diagrams/spark_declarative_pipeline_eventhub.png) |
| Ingestion Bronze | SQL Server → Bronze layer | ![](/diagrams/pipeline_ingestion_bronze.png) |
| Ingestion Silver | SQL Server → Silver dimension tables | ![](/diagrams/pipeline_ingestion_silver.png) |
| Transformation Silver | Generates fact tables in silver layer | ![](/diagrams/pipeline_transformation_silver.png) |
| Transformation Gold | Generates aggregated tables in gold layer | ![](/diagrams/pipeline_transformation_gold.png) |

---

## ⚙️ Orchestration

**Workflow:** Streaming (Event Hub) → Silver transformation → Gold transformation
![](/diagrams/workflow.png)

---

---

## 🖥️ AI/BI DashboardRestaurant Chain Performance Dashboard

![](/diagrams/dashboard.png)

---

## 🐛 Debugging

### Event Log Query

Use this query to inspect pipeline execution details:

```sql
SELECT
    timestamp,
    details:flow_definition.output_dataset,
    details:flow_progress.status,
    details:planning_information
FROM event_log(TABLE(`03_gold`.`daily_customer_360`))
WHERE details:planning_information IS NOT NULL
ORDER BY timestamp DESC
LIMIT 10;
```

---

## 📄 License

This project is for educational and analytical purposes.
