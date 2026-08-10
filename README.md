# 🏗️ Data Platform Architecture - Lab #01
**Focus:** Idempotency, Data Contracts, and Blast Radius Control
**Environment:** On-prem (Docker Compose)

## 1. Mindset & Rules
- **Concept First:** Focus on architecture, trade-offs, and data flow.
- **Internal Product Mindset:** Platform serves Data Engineers. Self-serve DevEx is key.
- **SRE Lens:** Assume failure. Design for Observability, Data Freshness, and Blast Radius control.

## 2. Core DataOps Concepts
- **Idempotency (Tính lặp lại an toàn):** A pipeline can be retried multiple times without duplicating or corrupting data.
- **Atomic Swap (Hoán đổi nguyên tử):** Instead of truncating live data (risk of Blackout), we load data into a `Staging` table and swap it with `Production` instantly. This is the Data equivalent of Blue/Green Deployment.
- **Silent vs. Loud Failure:** 
  - *Loud:* Process crashes, exit code != 0.
  - *Silent:* Process succeeds, but data is corrupted or missing (e.g., Schema drift). DataOps must turn Silent failures into Loud failures via Validation.
- **Data Contracts:** Agreements between Data Producers (Source) and Consumers (Platform) regarding schema and data quality.

## 3. High-Level Architecture (4 Layers)
1. **Source Layer:** MySQL (Simulating transactional DB).
2. **Control Plane (Orchestration):** The "Conductor" managing task dependencies, retries, and UI for DevEx.
3. **Processing Layer:** 
   - *Contract Validator:* Pre-flight schema checks (Fail-fast).
   - *ETL Engine:* Bulk extract and load into Staging.
4. **Storage Layer:** PostgreSQL (Target DB holding Staging and Production tables).

## 4. Data Flow (Happy Path)
1. Orchestrator wakes up.
2. Calls `Contract Validator` -> Source schema matches Data Contract.
3. Calls `ETL Engine` -> Extracts data, loads into `orders_staging`.
4. Calls `Swap Task` -> Renames `orders_staging` to `orders_production`.
5. Sends Success Alert to Slack/Email.

## 5. Trade-off Analysis (Component Separation)
- **Pros:** Clear Separation of Concerns. Small Blast Radius. High Observability. Data Engineers can debug specific tasks via Orchestrator UI.
- **Cons:** Higher operational complexity, higher baseline RAM/CPU footprint on On-prem servers.
- **Decision:** We accept the resource overhead to guarantee Production-grade Data Safety and Debuggability.