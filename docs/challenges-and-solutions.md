# Challenges & Solutions

A record of the real problems encountered building MetroMobility and how each was resolved. These are actual errors hit during live implementation, not hypotheticals — the debugging is where most of the learning happened.

---

## 1. Delta type-merge failure on SCD2 append

**Error:** `AnalysisException [DELTA_FAILED_TO_MERGE_FIELDS] Failed to merge fields 'zone_id' and 'zone_id'`

**Context:** The SCD Type 2 notebook builds `gold.dim_zone_scd2` from `gold.dim_zone`, then appends a simulated "changed" row for zone 132. The append failed on the `zone_id` column.

**Root cause:** `gold.dim_zone` derives `zone_id` from the taxi-zone lookup CSV, which was read with `inferSchema=true`. Spark inferred `LocationID` as **bigint (long)**. The hand-built update DataFrame used the Python literal `132`, which Spark types as **int**. Delta enforces strict type equality on append, so int-into-bigint failed. (The preceding `MERGE` succeeded because MERGE is more permissive about int/bigint comparison — only the append surfaced the mismatch.)

**Fix:** Cast the update columns to match the target schema explicitly:
```python
updates = spark.createDataFrame(...).withColumn("zone_id", F.col("zone_id").cast(T.LongType()))
```
Also hardened the notebook with an idempotent create guard (`if not spark.catalog.tableExists(...)`) so re-runs don't wipe SCD2 history, and switched the final insert to a `MERGE` so re-running can't create duplicate current rows.

**Lesson:** `inferSchema` produces unpredictable integer widths. Pin types explicitly at the bronze layer for anything used downstream in joins or merges.

---

## 2. Cross-stage warehouse deployment failure

**Error:** `DmsImportDatabaseException ... Invalid object name 'lh_mobility.gold.fact_trips'` when deploying `wh_mobility` through the deployment pipeline.

**Root cause:** The warehouse's views (`vw_hourly_demand`, `sp_top_routes`) use cross-database references into the lakehouse gold tables. Deployment pipelines promote **item definitions, not lakehouse data** — so in a freshly promoted stage the gold tables don't exist yet, and the view's `CREATE` fails because the referenced object is missing, which fails the entire warehouse import.

**Fix / correct promotion order:**
1. Deploy the lakehouse definition to the target stage.
2. Run the ingestion pipeline **in that stage** to populate `bronze/silver/gold`.
3. Only then deploy the warehouse, so its cross-database views can resolve.

**Lesson:** In Fabric, data-plane dependencies dictate deployment order. Cross-database T-SQL objects require their referenced lakehouse tables to physically exist in the target stage before the object can be created.

---

## 3. Notebook schema/binding drift across stages

**Error:** `[SCHEMA_NOT_FOUND] The schema 'default.ws-metromobility-test.lh_mobility.bronze' cannot be found.`

**Root cause:** Two gaps after cross-stage promotion. First, the promoted notebook's **default lakehouse binding** did not automatically rebind to the target-stage lakehouse. Second, the `bronze/silver/gold` **schemas didn't pre-exist** in the freshly promoted (empty) lakehouse — in dev they'd been created implicitly during the first write.

**Fix:**
- Manually reattached the notebook's default lakehouse to the target-stage `lh_mobility`.
- Added defensive schema bootstrapping at the top of the ingest notebook so it's portable across any stage:
```python
for schema in ["bronze", "silver", "gold"]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
```

**Lesson:** Notebook-to-lakehouse bindings are workspace-relative and don't survive promotion cleanly. Make notebooks self-bootstrapping (create their own schemas) so promotion is robust.

---

## 4. Spark capacity throttling

**Error:** `HTTP 430 [TooManyRequestsForCapacity] This Spark job can't be run because you've hit spark overall capacity compute limit.`

**Root cause:** Trial capacity (F64-equivalent) has a ceiling on concurrent Spark compute. Idle notebook sessions across dev/test/prod each hold a pool slot until timeout, and running the full medallion pipeline in multiple stages exhausted available compute.

**Fix:** Cancelled idle/queued Spark sessions in the **Monitoring hub**, stopped notebook sessions when not in use, and sequenced heavy runs so they didn't overlap. Analyzed CU consumption in the **Capacity Metrics app** to confirm the batch Spark transforms (silver/gold on ~40M rows) were the dominant consumers versus the low steady baseline of streaming.

**Lesson:** Capacity units are the real currency of Fabric. Concurrency management (session cleanup, run sequencing) matters as much as code efficiency on constrained capacity. This directly informed the scope decision to demo primarily on dev rather than triplicate 40M-row loads across all stages.

---

## 5. Custom-endpoint entity-name mismatch (CBS auth failure)

**Error:** `azure.eventhub.exceptions.AuthenticationError: CBS Token authentication failed.`

**Root cause:** Fabric's Eventstream custom endpoint auto-generates the underlying event hub **entity name** (`EntityPath=...`), which differs from the eventstream's display name (`es_ride_telemetry`). The simulator was passing the display name as `eventhub_name`, conflicting with the `EntityPath` embedded in the connection string.

**Fix:** Let the connection string's `EntityPath` drive the entity name — dropped the explicit `eventhub_name` argument so `from_connection_string()` reads it automatically.

**Lesson:** With Event Hub-compatible endpoints, the connection string's `EntityPath` is authoritative. Don't override it with a friendly display name.

---

## 6. Disconnected destination in Eventstream

**Error (authoring):** `This operation is missing an input to work.` (Fatal) on the Eventhouse destination.

**Root cause:** The Eventhouse destination node was added to the canvas but never wired to the stream's output — a destination with no input can't function. A duplicate orphan Activator node compounded the confusion.

**Fix:** Connected the stream node's output to the destination's input, deleted the orphan node, then completed the direct-ingestion table setup (`ride_events`, JSON) which only finalizes at/after publish.

**Lesson:** Eventstream is a visual dataflow — every destination needs an explicit input edge, and direct-ingestion tables are created as a post-publish step, not at node creation.

---

## 7. fabric-cli authentication in GitHub Actions

**Error:** `[UnexpectedError] An error occurred with the encrypted cache. Enable plaintext auth token fallback with 'config set encryption_fallback_enabled true'`

**Root cause:** The GitHub Actions runner is a headless container with no OS keyring, so fabric-cli can't encrypt its token cache.

**Fix:** Added `fab config set encryption_fallback_enabled true` before `fab auth login`. Acceptable in CI because runners are ephemeral and destroyed after the job, and secrets stay masked in logs. This is the documented CI pattern.

**Lesson:** CLI tools that rely on OS credential stores need an explicit fallback in headless CI. Know the difference between "insecure" and "appropriate for an ephemeral runner."

---

## 8. fabric-cli command syntax verification

**Error:** `unknown command for "fab"` on `fab run`.

**Root cause:** Documentation showed both `fab run` and `fab job run` for triggering pipelines, but the installed CLI version only implements `fab job run`.

**Fix:** Verified available commands against the actual installed CLI (`fab` with no args lists them) and used `fab job run`, which also polls for completion.

**Lesson:** Verify CLI syntax against the installed version, not documentation alone — docs and shipped behavior diverge, especially for fast-moving tools.

---

## 9. Git divergence between Fabric and Codespace

**Error:** `fatal: Need to specify how to reconcile divergent branches.`

**Root cause:** Fabric Git integration commits promoted items to `main` on the remote, while the Codespace commits local work (workflow, simulator, scripts) — both pushing to the same branch caused divergence.

**Fix:** Set a merge strategy (`git config pull.rebase false`) and pulled; adopted a **pull-before-push** discipline since Fabric may commit between local pushes.

**Lesson:** Running platform-managed Git (Fabric) and developer Git (Codespace) against one branch requires disciplined syncing. Pull before starting work and before pushing.

---

## Cross-cutting takeaways

- **"Green" is not "done."** Deployment pipelines reporting success still left warehouses and notebooks non-functional until data-plane dependencies were satisfied. Always verify downstream artifacts, not just exit codes.
- **Promotion moves definitions, not data.** Nearly every cross-stage issue traced back to this single fact about how Fabric deployment pipelines work.
- **Constrained capacity forces real engineering judgment.** Throttling drove genuine trade-off decisions about scope, sequencing, and where to invest compute.
