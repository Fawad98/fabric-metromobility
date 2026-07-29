# MetroMobility — Operations Runbook

## Rerunning failed loads
- Batch: re-run `pl_ingest_batch` from the Fabric portal or via CI (`fab job run`).
  The acquire notebook skips already-downloaded files, so re-runs are cheap.
- If a single layer failed, run its notebook directly against the target lakehouse.
  Ensure the notebook's default lakehouse is attached to the correct workspace stage.

## Handling TLC schema drift across years
- NYC TLC changes the Parquet schema between years (column additions/type changes).
- Bronze ingest reads with explicit handling; to tolerate drift across years use
  `.option("mergeSchema", "true")` on the Delta write, and pin known columns with
  explicit casts (e.g. LocationID -> long) to avoid int/bigint merge failures.

## Delta maintenance (VACUUM / OPTIMIZE cadence)
- Run `OPTIMIZE <table> VORDER` after large writes (silver, gold) to keep files
  compacted and V-Ordered for Direct Lake performance.
- Run `VACUUM` periodically (e.g. weekly) with the default 7-day retention to
  reclaim storage from old file versions. Do not lower retention below 7 days
  unless you are certain no readers/time-travel queries depend on older versions.

## Capacity management
- Spark compute on trial capacity is limited; concurrent jobs can hit HTTP 430
  (TooManyRequestsForCapacity). Cancel idle Spark sessions in the Monitoring hub
  and stop notebook sessions when not in use.
- Monitor CU consumption via the Fabric Capacity Metrics app.

## Cross-stage promotion notes
- Deployment pipelines promote item definitions, not lakehouse data.
- Warehouse cross-database views require the target lakehouse to be populated first;
  promotion order: lakehouse -> run ingestion pipeline -> warehouse.
- Notebook default-lakehouse bindings may need manual rebinding after promotion.
- Schemas are bootstrapped via `CREATE SCHEMA IF NOT EXISTS` in the ingest notebook.