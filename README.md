# NYC-taxi-pipeline-medallion
# NYC Yellow Taxi - Medallion Architecture (Bronze / Silver / Gold)

End-to-end Data Engineering pipeline built on Databricks Delta Lake.
Dataset: NYC Yellow Taxi Trip Data 2023-2024 (1.5M trips)
Location: Toronto, ON | Stack: PySpark, Delta Lake, Databricks

## Architecture


## Layers

### 1. Bronze - `NY_trip_bronze`
- Ingest raw CSV with `try_cast(timestamp)` to handle dirty date strings
- No transformation, immutable landing zone
- Saved as Delta with `overwriteSchema`

### 2. Silver - `NY_silver` (Core Engineering)
This is where 80% of data quality work happens.

**Cleaning logic & bug fixes:**
- Valid timestamps: `dropoff > pickup` and `isNotNull`
- Passenger count: `1-6` (taxi capacity business rule)
- Trip distance: `0.5-100 miles` (removes GPS glitches)
- Fare/Total: `>0` and `<1000` (removes free/refund rides)
- GPS: `lat/long != 0` - **Fixed bug:** Original code filtered `==0` then `!=0` which deleted ALL data (0 rows). Fixed to single `!=0` filter
- Deduplication: `dropDuplicates([pickup, dropoff, VendorID])` - **Fixed bug:** Was `pickup, pickup, VendorID`
- Trip duration: `(dropoff_long - pickup_long)/60` - cast to long BEFORE subtraction
- Delta fix: `DELTA_METADATA_MISMATCH` resolved with `.option("overwriteSchema","true")`

**Data Quality Tracking:**

### 3. Gold - Business KPIs
Ready for Power BI / Tableau.

- `gold_hourly_demand` - Trips, avg distance, avg duration by hour → Peak hours analysis
- `gold_payment_type` - Revenue by payment type → Cash vs Credit trend
- `gold_vendor_performance` - Vendor comparison (Creative Mobile vs VeriFone)

All tables built with `groupBy + agg(avg, sum, count)`

## Files
- `01_bronze_NY_trip_bronze.py` - Ingestion
- `02_silver_NY_silver.py` - Cleaning (most important)
- `03_gold_KPIs.py` - Aggregations

## How to Run
1. Run Bronze → creates `NY_trip_bronze`
2. Run Silver → creates `NY_silver` with `trip_duration`
3. Run Gold → reads `NY_silver` → creates 3 gold tables

## Key Learnings
- Always `count()` after each filter to avoid silent data loss
- Delta Lake needs `overwriteSchema` when column changes
- Filtering `==0` then `!=0` = 0 rows - logic matters

## Author
muhammed | Toronto
GitHub: github.com/muhammaddjwore8-blip/nyc-taxi-medallion
