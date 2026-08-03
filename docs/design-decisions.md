# Design Decisions

The reasoning behind the key architectural and implementation choices in MetroMobility. Each entry records what was decided, the alternatives considered, and why.

---

## Naming conventions

Consistent, self-documenting item names across the workspace:

| Item type | Convention | Example |
|---|---|---|
| Lakehouse | `lh_<domain>` | `lh_mobility` |
| Warehouse | `wh_<domain>` | `wh_mobility` |
| Notebook | `nb_<layer>_<purpose>` | `nb_silver_clean_trips` |
| Pipeline | `pl_<purpose>` | `pl_ingest_batch` |
| Eventstream | `es_<source>` | `es_ride_telemetry` |
| Eventhouse | `eh_<domain>` | `eh_mobility_rt` |
| Semantic model | `sm_<domain>` | `sm_mobility` |
| Real-Time Dashboard | `rtd_<domain>_<purpose>` | `rtd_mobility_live` |
| Deployment pipeline | `dp-<domain>` | `dp-metromobility` |

The prefix scheme means any item's type is readable from its name alone, and items sort by type in the workspace list.

---

## Cloud-only build (Fabric + Codespaces)

**Decision:** Build the entire platform in the browser — Fabric portal for all data/BI work, GitHub Codespaces for the streaming simulator, CLI, and git.

**Why:** No local dependencies means the project is fully reproducible by anyone with the accounts, and it demonstrates fluency with modern cloud-dev workflows. Fabric notebooks run on Fabric Spark; even the real-time producer runs in a cloud Codespace rather than a local machine.

**Trade-off:** Codespaces free-tier hours are finite, so the simulator is run in short bursts rather than left running indefinitely.

---

## Medallion architecture (bronze / silver / gold)

**Decision:** Classic three-layer medallion with schema separation.

- **Bronze:** raw ingest with audit columns (`_ingest_ts`, `_source_file`), no transformation.
- **Silver:** cleansed, deduplicated, quality-filtered, with derived columns and a persisted DQ metrics table.
- **Gold:** dimensional star schema optimized for serving.

**Why:** Separates concerns (raw preservation vs. cleansing vs. modeling), makes each stage independently re-runnable, and gives clear lineage. Using lakehouse **schemas** (`bronze.`, `silver.`, `gold.`) rather than table-name prefixes keeps the namespace clean.

---

## Metadata-driven ingestion

**Decision:** A config table (`bronze.ingest_config`) drives a generic loader, rather than one hardcoded pipeline per source.

**Why:** Adding a new source becomes a config row, not new code. This is the pattern real ingestion frameworks use and scales far better than per-table copies. It's a deliberate signal of production thinking over tutorial-style hardcoding.

---

## Explicit typing over inferSchema

**Decision:** Pin data types explicitly for columns used downstream, rather than trusting `inferSchema`.

**Why:** Learned the hard way — `inferSchema` produced a bigint for `LocationID`, which then failed a Delta append against an int-typed literal (see challenges-and-solutions.md §1). Explicit casts at the boundary prevent an entire class of type-merge failures downstream.

---

## SCD Type 2 on the zone dimension

**Decision:** Implement Slowly Changing Dimension Type 2 on `dim_zone`, with `valid_from` / `valid_to` / `is_current` columns, an idempotent create guard, and MERGE-based history handling.

**Why:** Demonstrates dimensional-modeling depth beyond a static star schema — the ability to track historical changes to a dimension over time. The idempotent guard and MERGE insert make the notebook safe to re-run without corrupting or duplicating history, which matters for a scheduled pipeline.

**Note:** The demonstrated change (renaming zone 132) is a scripted demo of the mechanism. In production, SCD2 would be driven by actual incoming changes. For this reason the SCD2 notebook is kept somewhat separate from the straight-through medallion flow.

---

## V-Order + OPTIMIZE for Direct Lake

**Decision:** Run `OPTIMIZE ... VORDER` on silver and gold tables.

**Why:** V-Ordering is what makes **Direct Lake** performant — Power BI reads the optimized Delta files directly from OneLake. This decision in the data layer is what unlocks the no-refresh serving model in the BI layer; the two are directly linked.

---

## Both Lakehouse and Warehouse

**Decision:** Serve from a Lakehouse (Spark/Delta) **and** a Warehouse (T-SQL) over the same OneLake data.

**Why:** Different consumers want different interfaces. Data engineers and PySpark users work in the lakehouse; SQL analysts and T-SQL tooling work in the warehouse. Because both read the same OneLake Delta with no copies, this costs storage nothing and demonstrates the cross-engine, zero-copy model that is core to Fabric's value. The warehouse's cross-database views query lakehouse gold tables directly.

---

## Direct Lake semantic model

**Decision:** Serve Power BI via a Direct Lake semantic model rather than Import or DirectQuery.

**Why:** Direct Lake reads V-Ordered Delta straight from OneLake — no import refresh to schedule, no DirectQuery latency. It's a Fabric-unique capability and the right default when the data is already optimized Delta in OneLake. This is why V-Ordering (above) was non-negotiable.

---

## Role-playing zone dimension

**Decision:** Handle pickup vs. dropoff zones with a single `dim_zone` and an inactive relationship activated via `USERELATIONSHIP`, rather than duplicating the dimension.

**Why:** `fact_trips` has both `pickup_zone_id` and `dropoff_zone_id`, but a model allows only one active relationship to a dimension. `USERELATIONSHIP` in a measure activates the inactive (dropoff) relationship on demand. This is the more elegant, DRY solution and demonstrates understanding of role-playing dimensions — a common real-world modeling pattern.

---

## Unified batch + streaming (lambda-ish)

**Decision:** Route the real-time stream to three destinations — Eventhouse (live KQL), Activator (alerting), and the Lakehouse (`bronze.ride_events_stream`).

**Why:** The stream serves immediate operational needs (live dashboard, alerts) *and* persists for historical batch analysis in the same medallion. Unifying batch and streaming on one storage layer is the platform's central differentiator versus keeping them in separate systems.

---

## Three-stage deployment (dev → test → prod)

**Decision:** A three-stage deployment pipeline with all stages assigned to real workspaces, rather than the simpler two-stage dev→prod.

**Why:** dev → test → prod mirrors how real teams gate promotion, and the test stage is where CI validation belongs before prod. It's a stronger signal of production discipline. All three stages were populated to prove the full mechanism.

**Trade-off:** Populating all stages consumes trial capacity (and triggered the throttling documented in challenges §4). On a paid capacity this would be automated per-stage; here it was done deliberately once to validate the mechanism, with dev as the ongoing demo environment.

---

## CI/CD via service principal

**Decision:** Automate deployment/runs through GitHub Actions authenticating as an Entra **service principal**, not personal credentials.

**Why:** Automated operations should never use a human's credentials — a service principal is the correct, auditable identity for CI/CD. It's scoped to the workspaces it needs and its secrets live only in GitHub Actions secrets. This is the production-grade authentication pattern.

---

## Secrets discipline

**Decision:** No plaintext secrets in code, notebooks, or screenshots. Connection strings and SP credentials live in Codespaces Secrets / GitHub Actions Secrets. Rotate immediately on any exposure.

**Why:** Standing security practice. The one exception — `encryption_fallback_enabled` in CI — is acceptable because the runner is ephemeral and destroyed after each job, and secrets stay masked in logs.

---

## Honesty about scope

**Decision:** Document trial-capacity limitations openly rather than implying a fully production-scaled deployment.

**Why:** dev is the primary demo environment; test/prod hold validated promoted definitions. Stating this plainly reads as engineering maturity — and is far better than being caught overstating scope in an interview. Every trade-off shaped by trial capacity is recorded in the docs rather than hidden.
