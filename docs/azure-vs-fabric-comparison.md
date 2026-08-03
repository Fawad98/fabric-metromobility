# Azure Discrete Services vs. Microsoft Fabric — An Honest Comparison

I've built comparable analytics platforms two ways: once by assembling discrete Azure services (ADF, Databricks/Synapse, Unity Catalog, Logic Apps, Power BI) with infrastructure-as-code, and once on Microsoft Fabric's SaaS-unified platform (this project). This document compares the two honestly — where each wins, where each hurts, and how I'd choose between them.

It exists because the choice between "assemble best-of-breed services" and "adopt a unified platform" is a real decision data teams face, and having built both, I can speak to the trade-offs from experience rather than marketing.

---

## At a glance

| Dimension | Discrete Azure Services | Microsoft Fabric |
|---|---|---|
| Provisioning | Terraform / Bicep, per-service | Workspaces in a portal; no infra to provision |
| Compute model | Separately sized/scaled per service | Shared capacity units (CU) across all workloads |
| Storage | ADLS Gen2, explicit accounts/containers | OneLake, unified, automatic |
| Cost model | Per-service metering, granular | Single capacity SKU, coarse |
| Governance | Purview + per-service RBAC, assembled | Built-in workspace roles, sensitivity labels, lineage |
| CI/CD | Mature (Azure DevOps/GitHub, Terraform state) | Git integration + deployment pipelines (newer, evolving) |
| BI serving | Import / DirectQuery | Direct Lake (unique — reads Delta directly) |
| Real-time | Event Hubs + Stream Analytics + ASA, wired together | Eventstream + Eventhouse + Activator, integrated |
| Lock-in | Lower — swappable components | Higher — platform-coupled |
| Learning curve | Steep per service, but transferable | Faster to a working platform, Fabric-specific |

---

## Where Fabric genuinely wins

**Time to a working platform.** The single biggest difference. In Fabric I went from nothing to an end-to-end batch + streaming + BI platform without provisioning a single resource, wiring a network, or managing storage accounts. On discrete Azure services, that same footprint is meaningful Terraform: storage, compute, orchestration, key vault, networking, identity — before any data moves.

**OneLake unification.** One logical storage layer that every engine reads without copies. The Warehouse queries the same Delta the Lakehouse wrote, which the semantic model reads directly. On discrete services, moving data between ADLS, a warehouse, and a BI import model means copies, pipelines, and refresh schedules.

**Direct Lake.** This has no equivalent in the discrete stack. Power BI reads V-Ordered Delta files straight from OneLake — no import refresh, no DirectQuery round-trip latency. It collapses a whole category of refresh-orchestration and performance-tuning work.

**Integrated real-time.** Eventstream → Eventhouse → Activator is one connected experience. The discrete equivalent (Event Hubs + Stream Analytics + a KQL cluster + an alerting function) is more powerful and flexible, but it's several services to provision, secure, and wire together.

---

## Where discrete Azure services genuinely win

**Cost granularity and control.** Per-service metering means you see and control exactly what each component costs, and scale each independently. Fabric's shared-capacity model is simpler but coarser — and, as I experienced, a single capacity ceiling means one heavy Spark workload can throttle everything else (`HTTP 430`). On discrete services I'd have scaled that Spark cluster independently without starving the rest of the platform.

**Mature, battle-tested CI/CD.** Terraform state, plan/apply, per-environment variable files, and years of tooling make discrete-service deployment predictable. Fabric's deployment pipelines are capable but newer, and I hit real friction: promotion moves item definitions but not data, cross-database warehouse views fail until the target lakehouse is populated, and notebook lakehouse bindings don't always rebind cleanly across stages. These are solvable, but they're the rough edges of a younger deployment model.

**Component swappability / lower lock-in.** With discrete services I can replace Databricks with Synapse, or swap the alerting layer, without re-platforming. Fabric is more coupled by design — the integration that makes it fast to build also makes individual pieces harder to replace.

**Flexibility at the edges.** When a workload needs something specific — a particular Spark runtime, a custom streaming topology, fine-grained network isolation — discrete services give you the control to do it. Fabric optimizes for the common path.

---

## How I'd actually choose

**Choose Fabric when:** the team wants one platform with minimal ops overhead, the workloads fit the common analytics path (medallion batch, standard streaming, Power BI serving), and speed-to-value matters more than granular cost control or component flexibility. It's especially strong for teams already in the Microsoft/Power BI ecosystem.

**Choose discrete Azure services when:** you need independent scaling and cost attribution per workload, mature IaC-driven multi-environment CI/CD, lower lock-in, or specific capabilities Fabric doesn't expose. Larger platform teams with dedicated infra engineers often benefit here.

**The honest middle:** many organizations will run both — Fabric for fast-moving analytics and self-service BI, discrete services for cost-sensitive or specialized production workloads — and use OneLake shortcuts to bridge them.

---

## What building both taught me

The deepest lesson wasn't which platform is "better" — it's that the **abstraction Fabric provides is real and has a cost on both sides of the ledger.** It removes an enormous amount of provisioning and integration work, and in exchange it takes away granular control and couples you to the platform. Every advantage (unified capacity, unified storage, integrated services) is the same coin as a corresponding constraint (shared throttling, higher lock-in, less flexibility).

Understanding *why* that trade-off exists — and being able to reason about which side of it a given team should land on — is more valuable than fluency in either stack alone.
