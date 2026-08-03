# MetroMobility — Operations Runbook

Operational procedures for running, maintaining, and recovering the MetroMobility platform.

---

## Rerunning failed loads

**Full batch pipeline:**
- Re-run `pl_ingest_batch` from the Fabric portal (Monitoring hub → the run → Rerun) or via CI/CD (`fab job run "ws-metromobility-dev.Workspace/pl_ingest_batch.DataPipeline"`).
- The acquire notebook skips files already present in `Files/raw/`, so re-runs are cheap and idempotent.

**Single layer:**
- Run the specific notebook directly against the target lakehouse (`nb_bronze_ingest`, `nb_silver_clean_trips`, `nb_gold_dimensional_model`).
- **Before running in a promoted stage (test/prod):** confirm the notebook's default lakehouse is attached to *that stage's* `lh_mobility`, not dev's. Binding does not always survive promotion.

**SCD2 dimension:**
- `nb_gold_scd2_dim_zone` is idempotent (create-guarded + MERGE-based insert). Safe to re-run without duplicating history.

---

## Handling TLC schema drift across years

NYC TLC changes the Yellow Taxi Parquet schema between years (added columns, changed types). If extending beyond 2024:

- On the bronze Delta write, use `.option("mergeSchema", "true")` to tolerate additive column changes.
- Pin known integer columns with explicit casts (e.g. `LocationID` → `long`) to avoid the int/bigint merge failures documented in challenges-and-solutions.md (§1).
- Do **not** rely on `inferSchema` for columns used in downstream joins/merges — pin them explicitly.

---

## Delta maintenance (OPTIMIZE / VACUUM cadence)

**OPTIMIZE + V-Order:**
- Run `OPTIMIZE <table> VORDER` after large writes to silver and gold. This compacts small files and V-Orders the data, which is required for good Direct Lake performance.
- Suggested cadence: after every full pipeline run, or nightly for incremental loads.

**VACUUM:**
- Run `VACUUM <table>` periodically (e.g. weekly) with the default 7-day retention to reclaim storage from superseded file versions.
- **Do not** lower retention below 7 days unless certain no time-travel queries or concurrent readers depend on older versions — doing so can break in-flight reads.

---

## Capacity management

- Trial/shared capacity has a Spark compute ceiling. Concurrent jobs can hit `HTTP 430 TooManyRequestsForCapacity`.
- **Prevention:** stop idle notebook sessions when not in use (each holds a pool slot until timeout); sequence heavy runs rather than overlapping them.
- **Recovery:** Monitoring hub → cancel active/queued Spark jobs you don't need → wait 1–2 minutes for compute to release → retry.
- **Visibility:** use the Fabric Capacity Metrics app to see CU consumption per workload. Batch Spark transforms on the ~40M-row layers are the dominant consumers; streaming is a low steady baseline.

---

## Cross-stage promotion (deployment pipeline)

Deployment pipelines promote **item definitions, not lakehouse data**. Correct promotion order:

1. Deploy lakehouse + notebooks + pipeline definitions to the target stage.
2. Run `pl_ingest_batch` **in that stage** to populate bronze/silver/gold.
3. Deploy the **warehouse** last — its cross-database views require the gold tables to physically exist, or the import fails (`DmsImportDatabaseException`).

Additional notes:
- Schemas are bootstrapped by `CREATE SCHEMA IF NOT EXISTS` at the top of the ingest notebook, so a freshly promoted lakehouse gets its bronze/silver/gold schemas automatically.
- Rebind notebook default lakehouses to the target stage after promotion.
- Rebind the `sm_mobility` semantic model's data source to the target-stage lakehouse via a deployment rule.
- Eventstream custom-endpoint keys are workspace-specific; the promoted eventstream source may need reconnection.

---

## Real-time stream operations

**Starting the simulator (from the Codespace):**
```bash
cd simulator
python ride_simulator.py
```
Requires `FABRIC_ES_CONN` set (Codespaces Secret or session export). The connection string's `EntityPath` drives the entity name — do not override it.

**If ingestion stops:**
- Confirm the simulator is running (tiles/KQL use `ago()` windows and go empty without recent data).
- Confirm the Eventstream is published and all destinations are connected to the stream node.
- Check the Eventhouse `ride_events` table exists and the direct-ingestion mapping completed.

**Secret rotation:** if the connection string is ever exposed, regenerate the primary key in the Eventstream custom-endpoint Keys panel immediately — this invalidates the leaked key.

---

## CI/CD operations

- Workflow: `.github/workflows/deploy.yml`, triggered on push to `main` (paths `fabric/**`) or manually via `workflow_dispatch`.
- Auth: Entra service principal via fabric-cli. Requires `encryption_fallback_enabled true` set before `fab auth login` (CI runners have no keyring).
- Pipeline trigger uses `fab job run` (not `fab run` — verify against the installed CLI version).
- Secrets (`AZURE_CLIENT_ID`, `AZURE_TENANT_ID`, `AZURE_CLIENT_SECRET`) live only in GitHub Actions secrets. Rotate in the Entra app registration if exposed.

---

## Git discipline (Fabric + Codespace)

Fabric Git integration and Codespace both commit to `main`. To avoid divergence:
- **Pull before starting work** and **pull before pushing** (Fabric may have committed in between).
- Merge strategy is set to merge (`git config pull.rebase false`).
