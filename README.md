# 🍽️ Restaruant Customer Lifetime Value & Restaurant Analytics

**AWS-only, PySpark-only customer analytics platform extracting directly from SQL Server — built around a daily-evolving CLV model, not a static aggregate.**


---

## 📋 Overview

A multi-location restaurant business needs to know which customers are worth the most over time, which marketing channels actually convert, and where discounting helps vs. hurts margin. This project builds that analytics layer under real enterprise constraints: **the source is SQL Server** (not flat files), **the architecture is AWS-only** with Snowflake and dbt explicitly forbidden, and **every transformation must be implemented in PySpark**. The centerpiece is a Customer Lifetime Value model that updates *daily per customer* — a meaningfully different data modeling problem than a single static "total spend" column.

**The numbers:**
- **203,519** order-item records
- **193,017** order-option records (the discount/customization signal)
- **8** PySpark Glue jobs orchestrated by Step Functions — CLV builds first, then six Gold metric jobs fan out in parallel
- **6** Streamlit dashboards mapped one-to-one to named business questions — all 6 pages fully implemented
- **100%** of transformation logic implemented in PySpark — zero dbt, zero Snowflake

---

## 🏗️ Architecture

```
┌─────────────┐     ┌───────────────────┐     ┌──────────────────────────┐     ┌──────────────┐
│ SQL Server  │ ──▶│   AWS Glue (JDBC)  │ ──▶│   AWS S3                 │ ──▶│  Redshift /  │
│ (source)    │     │   PySpark ETL     │     │  Bronze → Silver → Gold  │     │  Athena      │
└─────────────┘     └───────────────────┘     └──────────────────────────┘     └──────────────┘
                              │                                                         │
                     Step Functions retry +                                    Streamlit (6 dashboards)
                     SQS dead-letter queue                                      + GitHub Actions CI/CD
```

| Layer | Technology | Purpose |
|---|---|---|
| **Extraction** | AWS Glue JDBC connection (SQL Server) | Real database extraction, not a flat-file drop |
| **Transformation** | AWS Glue ETL — **100% PySpark** | Every join, aggregation, and scoring rule as Spark DataFrame ops — no dbt, no SQL-only logic |
| **Storage** | AWS S3, SSE-KMS encrypted | Bronze/Silver/Gold, explicit encryption requirement |
| **Serving Layer** | Amazon Redshift / Athena | No Snowflake permitted — Gold Parquet loaded via idempotent COPY (delete-then-copy on the daily CLV partition) |
| **Orchestration** | Step Functions, SQS DLQ, EventBridge | Explicit failure/reload requirement — not assumed, built |
| **Dashboard** | Streamlit | 6 pages mapped to the 6 named business metrics |
| **CI/CD** | GitHub Actions | Lint/test on push, automated Glue script deployment on merge |

---

## 🗂️ Data Model

```
order_items ──┐
              ├──▶ JOIN on (order_id, lineitem_id) ──▶ Silver (cleaned, typed, deduped)
order_item_   │
  options ────┘
                                    │
                                    ▼
                    ┌───────────────────────────────┐
                    │  customer_clv_daily (GOLD)     │
                    │ grain: (user_id, snapshot_date)│
                    │ — cumulative spend window fn   │
                    └───────────────────────────────┘
```

**The CLV table is the primary deliverable.** Grain is `(user_id, snapshot_date)`, not `(user_id)` — every day, a new snapshot partition is appended, with `cumulative_spend` computed via a Spark window function over all historical orders. This makes "how did this customer's value evolve" a simple time-series query instead of a recomputation.

---

## 📊 Metrics & Dashboards

| # | Dashboard | Goal |
|---|---|---|
| 1 | Customer Segmentation (RFM) | Recency/Frequency/Monetary scoring → VIP / Regular / Churn Risk / New |
| 2 | Churn Risk Indicators | Days since last order, inter-order gap trends |
| 3 | Sales Trends & Seasonality | Daily/weekly revenue by location and category |
| 4 | Loyalty Program Impact | `is_loyalty=True` vs `False` — spend, repeat rate, CLV comparison |
| 5 | Location Performance | Revenue, AOV, and order volume ranked by restaurant |
| 6 | Pricing & Discount Effectiveness | `option_price < 0` flag — discounted vs. full-price order economics |

All 6 pages are fully implemented in `dashboard/app.py` (no skeletons). Each page opens with the KPIs or visual that directly answers its named business question, and the Churn Risk page includes a visible threshold-based alert — a dashed 45-day line on the inactivity histogram plus a red-highlighted table of at-risk customers. Every page loads exclusively from the Gold layer.

---


## 🧩 Key Design Decisions

- **CLV as a snapshot fact, not a rolled-up column** — the brief requires CLV to evolve daily per customer. A single "total lifetime spend" field can't answer "how is this customer trending," so the Gold table is keyed on `(user_id, snapshot_date)` with a window-function-computed running total.
- **Incremental extraction with a per-table watermark** — `order_items` carries its own timestamp column (`creation_time_utc`) so the SQL Server extraction only pulls new rows each run; `order_item_options` and `date_dim` are pulled in full each time since they lack an independent timestamp or are small enough that a full refresh is cheaper than tracking state.
- **100% PySpark, including RFM scoring** — even simple lookups and percentile-based tier tagging are implemented as Spark DataFrame operations (`approxQuantile`, `Window`) rather than collected to a pandas DataFrame, per the brief's explicit constraint.
- **Discount detection at the option level, not the item level** — `option_price < 0` on `order_item_options` is the actual discount signal; `order_items.item_price` is never negative. Getting this join grain wrong silently undercounts discounts.
- **Step Functions + SQS DLQ as a named deliverable** — the brief explicitly calls out fault tolerance and reload handling, so retry policies and dead-letter queues are built and diagrammed, not just assumed as "good architecture."
- **Unit tests target the window-function logic specifically** — `test_clv_logic.py` verifies that cumulative spend never decreases day-over-day and that tier tagging correctly separates top/bottom spenders, since a silent regression in the window function would be the hardest bug to catch visually on a dashboard.
- **Orchestration encodes the job dependency, not just the schedule** — the Step Functions state machine runs the CLV job *before* fanning the other six Gold jobs out in parallel, because the loyalty comparison reads the CLV Gold table. Retries use exponential backoff and exhausted failures route to an SNS alert.
- **The Redshift load is idempotent by design** — the daily CLV partition is loaded via delete-then-COPY on the current `snapshot_date`, and small full-refresh tables are truncate-and-reload, so re-running the state machine after a failure never double-counts.

---

## 📁 Repo Structure

```
restaruant-clv-project/
├── README.md
├── .github/workflows/{ci.yml, deploy.yml}
├── architecture/
│   ├── architecture_diagram.png
│   ├── solution_design_document.docx
│   └── sme_approval.pdf
├── extraction/
│   ├── glue_jdbc_connection.json
│   └── extract_to_s3.py
├── glue_jobs/
│   ├── bronze_to_silver_orders.py      # order_items + order_item_options
│   ├── silver_to_gold_clv.py           # primary deliverable
│   ├── silver_to_gold_rfm.py
│   ├── silver_to_gold_churn.py
│   ├── silver_to_gold_sales.py
│   ├── silver_to_gold_loyalty.py
│   ├── silver_to_gold_location.py
│   └── silver_to_gold_discount.py
├── orchestration/step_function_definition.json
├── sql/
│   ├── 01_create_redshift_tables.sql
│   └── 02_load_gold_to_redshift.sql
├── dashboard/app.py                    # final version — all 6 pages
└── tests/test_clv_logic.py
```
