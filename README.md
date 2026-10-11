# Bangkok Traffic Project: Namtang GTFS data pipeline

This module collects Bangkok public-transport data from the **Namtang static GTFS feed**, published by the Office of Transport and Traffic Policy and Planning (OTP). It keeps the original download untouched, cleans and validates it, builds analysis-ready datasets, and loads everything into PostgreSQL for the analysis and ML team.

- **Source:** the static ZIP only, `https://namtang-api.otp.go.th/download/namtang-gtfs.zip`. No other endpoint, API or realtime feed is used.
- **Out of scope:** frontend, backend, ML models, accident data (owned by another team member), realtime data, service-instance expansion.
- **Rules:** [.ai/PROJECT_RULES.md](.ai/PROJECT_RULES.md). Correctness comes before row counts, raw data is never changed, and records are never invented.

## Status

| Phase | What | Status |
|---|---|---|
| 1 | Scrape and store the raw ZIP (SHA-256 identity) | Complete |
| 2 | Clean GTFS into Parquet | Complete |
| 3 | Validate the clean feed | Complete |
| 3.5 | Data quality policy DQ-001 to DQ-010 | Complete |
| 4 | Analysis-ready datasets | Complete |
| 4.5 | Derived-data policy DQ-011 and DQ-012 | Complete |
| 5 | PostgreSQL migrations and loader | **Complete.** Runtime checked 2026-10-02 with read-only queries |
| Daily | Python daily pipeline runner (`run_pipeline.py`) | Implemented and tested |
| 6 | Airflow orchestration (`namtang_gtfs_pipeline` DAG) | Complete. One real unchanged-feed run verified (6.5); manual trigger only (`schedule=None`) |
| 7 | New-feed lifecycle / policy-gate verification | Complete. Isolated fixtures and test database; production untouched |
| 8 | Scheduling and operational readiness | Implemented. Daily schedule configured but **off** (manual until activated) |
| 9 | BKK Traffic (Longdo) pipeline in PostgreSQL schema `bkk_traffic` | **In production since 2026-10-03.** `*/15` schedule active; see [section 12](#12-bkk-traffic-pipeline-production) |

## Project structure

```
.ai/PROJECT_RULES.md            shared project rules (read first)
data/
  scraper/
    namtang_scraper.py          Phase 1: download, verify, store raw feed, log
    processing/
      clean_gtfs.py             Phase 2: raw ZIP -> clean Parquet
      validate_gtfs.py          Phase 3: read-only validation report
      build_analysis.py         Phase 4: clean Parquet -> analysis Parquet
    run_pipeline.py             daily runner: scrape -> clean -> validate -> policy gate -> analysis -> load
  raw/namtang/                  original downloads (immutable, git-ignored)
  clean/namtang/                clean Parquet (git-ignored)
  analysis/namtang/             derived Parquet (git-ignored)
  metadata/                     scrape_log.csv, validation reports, DQ policies (committed)
database/
  migrations/namtang/001..005   PostgreSQL schema (005 = load_history)
  load/load_namtang.py          Phase 5: Parquet -> PostgreSQL (COPY)
  README.md                     database details
docker-compose.yml              local PostgreSQL for this module (namtang_db)
airflow/
  dags/namtang_gtfs_pipeline.py Airflow DAG: one task per pipeline step (orchestration only)
  docker-compose.yml            local Airflow 3.3.2 dev stack (separate Compose project)
  Dockerfile                    Airflow image + requirements.txt
  .env.example                  template for airflow/.env (secrets, git-ignored)
  run_dag_tests.sh              full test suite inside the Airflow image
tests/                          pytest suite (synthetic fixtures, no network)

BKK Traffic (section 12):
data/scraper/bkk_traffic/       Longdo pipeline (scrape, history, travel times, clean, publish, quality)
database/migrations/bkk_traffic/001..006, database/load/load_bkk_traffic.py
airflow/dags/bkk_traffic_pipeline.py, bkk_traffic_quality.py, bkk_traffic_maintenance.py
docs/integration/               BKK design + verification reports; docs/architecture/ overview
handoff/bkk_traffic/            teammate data handoff (README, manifest, checksums; dump kept outside Git)
```

## 1. Python environment

Python 3.11 or newer. These commands are for PowerShell, run from the project root.

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
.\.venv\Scripts\python.exe -m pytest tests -q        # 280 pass, 148 skip: Airflow, Phase 7 DB and bkk_traffic DB tests skip (see sections 8-9)
```

Dependencies: `requests`, `pandas`, `pyarrow`, `psycopg[binary]`, `pytest`.

## 2. Run the pipeline

Each step can be re-run safely. A step skips work it has already completed, and the next step only reads output that has a completion marker.

```powershell
$py = ".\.venv\Scripts\python.exe"

# Phase 1: download the static ZIP once. An unchanged feed is detected by SHA-256 and not stored again.
& $py data/scraper/namtang_scraper.py

# Phase 2: clean every raw feed that has no completed clean output yet (or pass a raw feed dir).
& $py data/scraper/processing/clean_gtfs.py data/raw/namtang/2026-10-01

# Phase 3: validate (read-only). Writes data/metadata/validation_<feed>.json and .md.
& $py data/scraper/processing/validate_gtfs.py data/clean/namtang/2026-10-01

# Phase 3.5: needs a reviewed data/metadata/data_quality_policy_<feed>.json (see section 5).

# Phase 4: build the analysis layer. The DQ policy for the feed is required.
& $py data/scraper/processing/build_analysis.py data/clean/namtang/2026-10-01
```

- `--force` on `clean_gtfs.py` or `build_analysis.py` rebuilds that step's output from its input. Raw data is never touched.
- `validate_gtfs.py` exits with code 1 when the overall status is `ERROR`. That is the expected result for feed 20260930 because of DQ-001 and DQ-002 (see section 6). The daily runner uses the policy gate instead of this exit code.

### Daily pipeline (one command)

```powershell
$env:DATABASE_URL = "postgresql://postgres:postgres@localhost:5433/namtang_db"
& $py data/scraper/run_pipeline.py            # or --no-load to stop after the analysis step
```

It runs scrape → clean → validate → **DQ policy gate** → analysis → PostgreSQL load, using the existing step implementations.

- **Target feed:** always the `raw_path` the scraper returns, not the newest folder.
- **Unchanged feed (same SHA-256):** no new raw, clean, analysis or validation output, and no database write. The validation report is reused. The run is logged as `unchanged`.
- **New feed:** cleaned and validated, then the gate requires a reviewed `data/metadata/data_quality_policy_<feed>.json` written for that exact SHA-256. Without one the run stops as `awaiting_policy`; a policy is never generated. Once a reviewer adds the policy, the next run continues from the existing outputs and replaces the database snapshot in one transaction.
- **Gate rule:** every validation ERROR must belong to a table with policy ERROR issues (`validation_severity` / `affected_table`), and the per-table counts must equal the policy's `affected_rows`. A new table, a changed count or a feed-level error stops the run. Warnings never block.
- **Failures:** the run stops at the failing step and earlier outputs and the database are left as they were. The next run resumes at the first incomplete step.
- **Overlapping runs:** prevented by `data/metadata/namtang_pipeline.lock`. A lock left by a dead process on the same machine is replaced. Leftover `.tmp_*` / `.old_*` folders under the clean and analysis roots that are older than 24 h are removed; raw data is never touched.
- **Exit codes:** `0` success/unchanged, `1` failed, `2` awaiting_policy, `3` already running.
- **Logging:** each run writes `stage=pipeline` (started / final status) and `stage=policy_gate` rows, and the loader writes `stage=load` rows.

## 3. Data layout

### Raw (immutable)
```
data/raw/namtang/YYYY-MM-DD/            first distinct feed of a day (Asia/Bangkok date)
data/raw/namtang/YYYY-MM-DD/HHMMSS/     further distinct feeds on the same day
    gtfs.zip                            original bytes, set read-only, never overwritten
    manifest.json                       URL, HTTP headers, SHA-256, size, feed_info, per-file columns/row counts/encoding
data/raw/namtang/_quarantine/           failed downloads, kept for debugging
```

### Clean
```
data/clean/namtang/<feed>/              <feed> = raw path joined with "_" (e.g. 2026-10-01)
    <table>.parquet                     12 GTFS tables, typed, trimmed, empty -> null
    rejected/<table>.parquet            rows that failed parsing, with reject_reason (none for 20260930)
    metadata.json                       per-table raw/clean/rejected counts, schema, warnings
    _SUCCESS                            written last; downstream steps require it
```
- Rows are read in 100,000-row chunks straight from the ZIP; no extracted copy is made.
- GTFS times keep their original string and also get `*_secs` (times of 24:00:00 or later are supported).
- `frequencies` has `is_single_departure` (DQ-004).

### Analysis (derived)
```
data/analysis/namtang/<feed>/
    agencies_analysis, routes_analysis, stops_analysis, trips_analysis,
    stop_times_analysis, frequencies_analysis, fare_analysis, shapes_analysis (.parquet)
    metadata.json                       transformations, flag counts, source vs derived counts, validation
    _SUCCESS
```
- No rows are excluded. Data-quality issues appear as flag columns tied to DQ IDs.
- `shapes_analysis` has one summary row per shape; the 3.87M points are not copied.

### Metadata (committed)
- `data/metadata/scrape_log.csv`: one lightweight row per run and per table. The `stage` column says which step wrote it: `scrape`, `clean`, `validate`, `policy`, `analysis`, `analysis_policy` or `database_verify`. It never contains GTFS records.
- `data/metadata/validation_<feed>.json` and `.md`: validation reports.
- `data/metadata/data_quality_policy_<feed>.json` and `.md`: DQ policy, which is authoritative for the analysis step.

## 4. Lineage and immutability

- **Feed identity:** the SHA-256 of `gtfs.zip`, plus `feed_version` from `feed_info.txt`. Every clean, analysis and database row carries `feed_sha256` and `feed_version`.
- **`source_row`** is the 1-based data-row number in the original GTFS file. Validation proves every source row appears exactly once (clean + rejected = raw).
- **Analysis rows** also keep `source_table`. `fare_analysis` keeps both `fare_rules_source_row` and `fare_attributes_source_row`; `shapes_analysis` keeps the first and last source row of each shape.
- **The raw ZIP and manifest are read-only.** Cleaning re-checks the ZIP hash against the manifest before it starts.
- **Source data vs derived data:**
  - *Source* = raw and clean tables: the published records, typed but not changed in meaning.
  - *Derived* = analysis tables: counts, segments, speeds and estimates.
  - Never present a derived or "active network" count as an original GTFS count.
  - Estimated departures are a `DERIVED_ESTIMATE`, not observed departures.
  - Service instances are not generated.

## 5. Validation and the data quality policy

`validate_gtfs.py` checks:
- exact duplicates and logical keys;
- foreign keys (optional nulls are allowed);
- stop_times ordering and times, shapes, frequencies, calendar and fares;
- lineage and row counts.

Each check is `PASS`, `WARNING` (unusual but possibly valid) or `ERROR` (structurally broken). Warnings alone never make the feed `ERROR`.

The DQ policy (`data/metadata/data_quality_policy_2026-10-01.*`) decides how each finding is handled. The classifications are `PRESERVE`, `FLAG`, `EXCLUDE_DOWNSTREAM_ONLY`, `DO_NOT_REMAP` and `DO_NOT_FILL_TIME`. **Source data is never repaired or deleted**, and any downstream exclusion must report its count. `build_analysis.py` checks every flag count against the policy and refuses to publish if they don't match.

## 6. Known DQ findings (feed 20260930)

| ID | Finding | Count | Handling |
|---|---|---:|---|
| DQ-001 | `agency_id` CCT and SBT each used by 2 operators (validation ERROR) | 4 agency rows → 70 routes, 123 trips, 775 fares | Keep, flag, do not remap. Joining to `agency` on `agency_id` duplicates rows |
| DQ-002 | Fare rules referencing zone `221`, which no stop defines (validation ERROR) | 120 fare_rules | Keep, flag; fare analysis may exclude them and report it |
| DQ-003 | Duplicate frequency window, trip 6160 | 2 rows (1 extra) | Keep both; count once in derived calculations |
| DQ-004 | `single_departure` (headway 0, start = end) | 555 | Accepted; exactly 1 departure; never divide by headway |
| DQ-005 | Same stop twice in a row | 3 rows (2 of these hops take 0 s) | Keep; allow zero-length segments |
| DQ-006 | Repeated consecutive shape points | 815,348 | Keep; may be collapsed only in derived geometry |
| DQ-007/008/009 | Unused routes / stops / shapes | 4 / 219 / 7 | Keep; may be left out of active-network metrics |
| DQ-010 | `calendar_dates` rows that change nothing | 33 | Apply normally; not counted as service changes |
| DQ-011 | Derived segment speed > 150 km/h (an analysis threshold, not proof of bad source data) | 564 | Keep, flag `unrealistic_segment_speed`, do not cap |
| DQ-012 | Zero-second segments between different stops | 1,092 (of 1,094 zero-second segments; the other 2 are DQ-005) | Keep, flag `zero_second_segment`, do not fill time; speed stays NULL |

Two more interpretation rules:
- **Frequency-based trips (4,429 of 4,656):** their `stop_times` are relative templates (`stop_times_are_relative_template`), so `first_departure_time`, `last_arrival_time` and `duration_seconds` are not clock times. Use the frequency windows for clock-time analysis.
- **Estimated departures:** `ceil((end - start) / headway_secs)` for normal windows (departures at `start + k*headway` while before `end`), and 1 for single departures. The end-boundary convention is an assumption that can be changed later.

## 7. PostgreSQL (Phase 5)

This module has its **own** container. The IT project's database (`itproject_db`, port 5432) is separate and must not be touched.

| Setting | Value |
|---|---|
| Container / compose service | `namtang_db` / `namtang-db` (image `postgres:18`) |
| Host port | **5433** (5432 is `itproject_db`) |
| Database / user | `namtang_db` / `postgres` (password set in `docker-compose.yml`; local development only) |
| Schemas | `namtang` (12 source tables + `migration_history` + `load_history`), `namtang_analysis` (8 derived tables) |

```powershell
docker compose up -d namtang-db

# Migrations, in order 001 -> 005. Each can be re-run and is recorded in namtang.migration_history.
# (All five, including 005_create_load_history, are applied to the running namtang_db.)
foreach ($m in "001_create_schema","002_create_tables","003_create_indexes","004_create_constraints","005_create_load_history") {
  Get-Content "database/migrations/namtang/$m.sql" -Raw |
    docker exec -i namtang_db psql -U postgres -d namtang_db -v ON_ERROR_STOP=1
}

# Load one feed (explicit directories). Same SHA-256 already loaded -> "unchanged", nothing written.
# A new SHA-256 replaces the snapshot in one transaction (counts checked before commit).
$env:DATABASE_URL = "postgresql://postgres:postgres@localhost:5433/namtang_db"
.\.venv\Scripts\python.exe database/load/load_namtang.py `
  --clean-dir data/clean/namtang/2026-10-01 --analysis-dir data/analysis/namtang/2026-10-01
# add --dry-run to only report, or --replace to force a reload of the same feed
```

Expected row counts (verified in PostgreSQL on 2026-10-02):

| `namtang` table | Rows | `namtang_analysis` table | Rows |
|---|---:|---|---:|
| agency | 126 | agencies_analysis | 126 |
| calendar | 7 | fare_analysis | 946,175 |
| calendar_dates | 105 | frequencies_analysis | 10,072 |
| fare_attributes | 946,175 | routes_analysis | 2,069 |
| fare_rules | 946,175 | shapes_analysis | 3,544 |
| feed_info | 1 | stop_times_analysis | 159,358 |
| frequencies | 10,072 | stops_analysis | 17,081 |
| routes | 2,069 | trips_analysis | 4,656 |
| shapes | 3,873,114 | | |
| stop_times | 159,358 | | |
| stops | 17,081 | | |
| trips | 4,656 | | |
| **total** | **5,958,939** (0 rejected) | | |

The total adds rows from different GTFS tables; it is not a count of trips. More detail is in [database/README.md](database/README.md).

## 8. Airflow orchestration (Phase 6)

**Purpose:** show each pipeline step as its own task in the Airflow UI, with per-task status, logs and retries.

**Airflow only orchestrates.** The DAG holds no data logic: each task calls one step function in `data/scraper/run_pipeline.py` (`step_scrape` … `step_load`). Those steps call the existing modules (`namtang_scraper.py`, `clean_gtfs.py`, `validate_gtfs.py`, the policy gate, `build_analysis.py`, `load_namtang.py`). `run_pipeline.py` itself is still available for manual or local runs, and both paths use the same steps.

DAG `namtang_gtfs_pipeline` (`airflow/dags/namtang_gtfs_pipeline.py`):

```
scrape
  ↓
clean
  ↓
validate
  ↓
dq_policy_gate
  ↓
analysis
  ↓
load_postgresql
```

- **Feed identity:** `scrape` pins the feed to the `raw_path` the scraper returns. Each task passes a small JSON context through XCom (`run_id`, `feed_sha256`, `feed_version`, `raw_path`/`raw_dir`, `clean_dir`, `clean_run_id`, `validation_report` path, policy decision, `analysis_dir`, per-step status). Datasets stay on disk; no rows go into XCom.
- **Failures:** a failed task marks all later tasks `upstream_failed`.
  - `dq_policy_gate` fails **without retries** as `awaiting_policy` when the policy is missing or mismatched, or an unexpected ERROR appears. Known errors covered by the reviewed policy pass.
  - `scrape` retries 2× (10 min apart) and `load_postgresql` 1× (5 min); other tasks don't retry.
  - Every outcome is also written to `scrape_log.csv` (`stage=pipeline`, `policy_gate` and `load` rows).
- **Database safety:** `load_postgresql` uses the existing loader. A feed already in the database is `unchanged` and writes nothing; a new feed replaces the snapshot in one transaction that rolls back on any failure, and is recorded in `load_history`.
- **Concurrency:** `max_active_runs=1`, plus a DAG-run **lease** on `data/metadata/namtang_pipeline.lock` from `scrape` to the end of the run, so a manual `run_pipeline.py` can't start between tasks (see section 10).

### Environment

A separate Compose project, `namtang-airflow` (Airflow **3.3.2**, Python 3.13, LocalExecutor). It does not manage the `namtang_db` container and never touches `itproject_db`; its only writes to `namtang_db` are made by the `load_postgresql` task through the existing loader.

| Service | Role |
|---|---|
| `airflow-apiserver` | UI and API, published on **127.0.0.1:8080** only |
| `airflow-scheduler` | Runs the tasks. It is the only service on the `bangkok-traffic-project_default` network, where it reaches `namtang_db:5432` |
| `airflow-dag-processor` | Parses `airflow/dags` |
| `airflow-metadata-db` | Airflow's own Postgres 16. Not exposed, and not `namtang_db` |
| `airflow-cli` | Profile `cli`: one-off commands and the test suite |

The repository is mounted at `/opt/namtang`, so tasks read and write the same `data/` folders as local runs.

### Start, open, trigger, inspect, stop

```powershell
# one-time setup: create airflow/.env from the template and fill in real values
Copy-Item airflow/.env.example airflow/.env

docker compose -f airflow/docker-compose.yml up -d --build      # start (namtang_db must be running)

# UI: http://localhost:8080   user: admin   password:
docker compose -f airflow/docker-compose.yml exec airflow-apiserver cat /opt/airflow/simple_auth_manager_passwords.json.generated

# trigger manually: in the UI, unpause namtang_gtfs_pipeline and click "Trigger", or:
docker compose -f airflow/docker-compose.yml exec airflow-scheduler airflow dags unpause namtang_gtfs_pipeline
docker compose -f airflow/docker-compose.yml exec airflow-scheduler airflow dags trigger namtang_gtfs_pipeline

# task logs: click a task box in the Grid/Graph view, or read airflow/logs/ on disk
docker compose -f airflow/docker-compose.yml exec airflow-scheduler airflow dags list-runs namtang_gtfs_pipeline

# tests inside the Airflow image (throwaway SQLite metadata DB; real DB connection removed)
docker compose -f airflow/docker-compose.yml run --rm airflow-cli

docker compose -f airflow/docker-compose.yml down       # stop (add -v to also delete Airflow's metadata DB)
```

The DAG is created **paused** and has `schedule=None`, so nothing runs until someone triggers it. A real trigger downloads the static ZIP once; an unchanged feed then flows through without writing anything.

### PostgreSQL connection

The Airflow connection `namtang_postgres` is defined by `AIRFLOW_CONN_NAMTANG_POSTGRES` in `airflow/.env`:

```
postgres://postgres:<password>@namtang_db:5432/namtang_db
```

- Inside Docker the host is the container name `namtang_db` and the port is **5432**; 5433 is only the host-side port.
- The URI path is the **database** (`namtang_db`). The tables are in the PostgreSQL schemas `namtang` and `namtang_analysis`.
- No credentials are in the DAG code. `airflow/.env` is git-ignored.

### Schedule

The daily 06:00 Asia/Bangkok schedule is already in the DAG and is switched on with `NAMTANG_SCHEDULE_ENABLED`, so no code edit is needed. See **section 10, "Activating the daily schedule"**.

## 9. New-feed lifecycle tests (Phase 7)

These tests prove what happens when a **new** feed SHA appears, without touching production data, the real feed or `namtang_db`. They run the real scraper, clean, validate, policy-gate, analysis and loader code, as plain Python and through the real DAG (`dag.test()`).

**Fixtures (TEST-ONLY, not production data):** `tests/fixtures/phase7/`. They're built by `build_fixture.py` from real rows of the local feed 20260930 (routes 3393/CCT, 66/SRT, 2547/PPCT), with only `feed_version` replaced. The ZIPs are byte-deterministic; SHAs and row counts are in `fixture_manifest.json`.

| Variant | feed_version | SHA-256 (prefix) | Purpose |
|---|---|---|---|
| A | `PHASE7-TEST-A` | `890ccf51…` | Real DQ-001 (CCT) and DQ-002 (zone 221) errors, covered by a test policy |
| B | `PHASE7-TEST-B` | `b92bf214…` | Same rows, different SHA: policy-SHA mismatch |
| C | `PHASE7-TEST-C` | `a41ad7e2…` | A minus one stop that `stop_times` uses: an unexpected ERROR the policy doesn't cover |

`data_quality_policy_phase7_A.json` is a reviewed **test** policy for SHA-A only. The real policy is never used or changed.

**Behavior proven:**
- **New feed, no policy:** scrape (new SHA) → clean → validate → `dq_policy_gate` fails as `awaiting_policy`. Analysis and load never run; in Airflow they're `upstream_failed`. Nothing is loaded.
- **Approved policy for that exact SHA:** the gate passes (DQ-001/DQ-002 covered), analysis passes its policy reconciliation, and the loader writes the fixture **into the test database only**. Row counts and feed identity are verified, and `load_history` gets one `success` row.
- **Same SHA again:** `unchanged`. No new files and no database write.
- **Policy written for another SHA:** rejected ("different feed SHA-256"), with no analysis and no load. A correct policy for SHA-B then replaces the snapshot; `load_history` records the previous SHA.
- **Unexpected ERROR** (orphan `stop_times.stop_id`, variant C): not covered, so `awaiting_policy`. Nothing is loaded.

**Isolation:**
1. **Separate database.** The throwaway service `namtang-test-db` (`namtang_test_db`, data in tmpfs, `127.0.0.1:5434`) sits only on the Airflow network, never on `namtang_db`'s network.
2. **Guarded target.** Tests take the target only from `NAMTANG_TEST_DATABASE_URL` and refuse any DSN naming `namtang_db`, host `namtang_db` or port 5433. Before any write they confirm `current_database()` = `namtang_test_db`.
3. **No real URL.** The real scraper gets a fake HTTP session that serves the fixture and raises on any other URL.
4. **Temporary storage.** All pipeline folders and logs are pytest temporary directories.
5. **No production connection.** Container runs unset `AIRFLOW_CONN_NAMTANG_POSTGRES` and use a throwaway SQLite Airflow metadata database.
6. **Checked every test.** Each test asserts that the real `data/` tree is unchanged.

**Commands:**

```powershell
# inside the Airflow image (starts namtang-test-db automatically)
docker compose -f airflow/docker-compose.yml run --rm airflow-cli

# on Windows
docker compose -f airflow/docker-compose.yml --profile testdb up -d namtang-test-db
$env:NAMTANG_TEST_DATABASE_URL = "postgresql://namtang_test:namtang_test@127.0.0.1:5434/namtang_test_db"
.\.venv\Scripts\python.exe -m pytest tests -q
docker compose -f airflow/docker-compose.yml stop namtang-test-db     # data is discarded
```

Use `127.0.0.1`, not `localhost`: on Windows `localhost` first tries IPv6 and adds about 5 s per connection. Without `NAMTANG_TEST_DATABASE_URL`, the database-backed Phase 7 tests are skipped.

## 10. Operations (Phase 8)

The Namtang pipeline is **one independent source pipeline**: `source = namtang`, DAG `namtang_gtfs_pipeline`, PostgreSQL schemas `namtang` and `namtang_analysis`.

### Daily schedule and timezone

| Setting | Value |
|---|---|
| Timetable | `CronTriggerTimetable("0 6 * * *", timezone="Asia/Bangkok")`: once a day at **06:00 Bangkok** (23:00 UTC the previous day) |
| Switch | `NAMTANG_SCHEDULE_ENABLED` in `airflow/.env`. `false` (default) means `schedule=None`, manual trigger only |
| Safety | `catchup=False` (no backfill), `max_active_runs=1`. The DAG also stays **paused** until someone unpauses it |
| Timezone | Set explicitly on the timetable and `start_date`; it doesn't depend on the machine or Airflow's default timezone (which is also `Asia/Bangkok` in `airflow/docker-compose.yml`) |

06:00 is a **polling time chosen by us, not the publisher's update time.** The only evidence is one feed: `20260930`, `Last-Modified` 30 Sep 2026 18:02 UTC (01:02 Bangkok). Polling a feed that hasn't changed costs one download and writes nothing.

**What one run does:**
- **Unchanged feed (same SHA-256):** scrape logs `unchanged`. Clean and analysis reuse their existing output, validation reuses its report, the gate passes, and load is `unchanged`, so nothing is written to PostgreSQL. The run is green, and no new raw, clean or analysis folder is created.
- **New feed:** scrape stores a new raw snapshot, then clean and validate run. Then:
  - **No reviewed policy for that exact SHA:** `dq_policy_gate` fails as **`awaiting_policy`**. Analysis and load show `upstream_failed`, and PostgreSQL keeps the previous feed.
  - **Approved policy:** analysis runs, then load replaces the snapshot in **one transaction**. Row counts and feed identity are checked before commit, and a `load_history` row records the new SHA, version, previous SHA, clean and analysis run IDs, status and row counts. Any failure rolls back completely.

### Handling `awaiting_policy`

1. Find the run in the Airflow UI (red `dq_policy_gate`), or the `stage=policy_gate` row in `data/metadata/scrape_log.csv`. Both name the feed SHA and the **exact policy file path** expected, `data/metadata/data_quality_policy_<feed-folder>.json`.
2. Review that feed's validation report, `data/metadata/validation_<feed-folder>.json/.md`.
3. Write and review the policy for **that SHA**. Every validation ERROR must belong to a table with policy ERROR issues, and the counts must match. Never generate or auto-approve a policy.
4. Trigger the DAG again (or wait for the next scheduled run). Finished steps are reused, so the run continues at analysis.

### Inspecting failures

- **Airflow UI (`http://localhost:8080`):** the Grid view shows each run, the state of every task, try numbers (retries) and durations. Click a task for its log, including the full error message.
- **`scrape_log.csv`:** one short row per event.
  - `stage=pipeline` (`started`, then `success` / `unchanged` / `failed` / `awaiting_policy` with "failed at <step>: ..." or a one-line summary including `db previous=... current=...`);
  - `stage=policy_gate`;
  - `stage=load` (`previous=... current=...`);
  - per-step rows from scrape, clean, validate and analysis.
- **Notifications:** no external service is configured. A failed DAG run calls `on_dag_run_failure` in the DAG file, which releases the run's lock and logs a summary. Add an e-mail or chat notifier there once the team has one; never commit its credentials.

### Lock and concurrency

`data/metadata/namtang_pipeline.lock` lets only one pipeline run work at a time, whether manual or Airflow.

| Owner | How it holds the lock | When it's stale |
|---|---|---|
| Manual `run_pipeline.py` (`kind=process`) | For the whole run, then releases it | Only if it was taken on **this** host and that process no longer exists |
| Airflow DAG run (`kind=airflow`, owner `airflow-<run_id>`) | A **lease**: `scrape` takes it, later tasks of the same run re-enter it, and it's released after `load_postgresql`, on any task failure, or by the DAG failure callback | Only after its `expires_at_utc` (6 h, refreshed by every task) |

- A lock file that can't be read is **never** treated as stale. Check that no run is active, then remove it by hand.
- Only the owner can release a lock. Stale takeover and release rename the file and check its owner first, so a fresh lock is never deleted.
- A manual run during an Airflow run exits with code 3 ("held by airflow owner ..."). An Airflow task that finds a manual run's lock fails; `scrape` retries later, other tasks fail visibly.
- If a crashed Airflow run blocks a manual run, wait for the lease expiry shown in the message, or, after confirming in the UI that the run is finished, delete the lock file.

### Retries

| Task | Retries | Why |
|---|---|---|
| `scrape` | 2 (10 min apart) | Network errors are transient. An already-stored feed is detected by SHA, so a retry never stores a second snapshot |
| `clean`, `validate`, `analysis` | 0 | Deterministic: a retry would fail the same way |
| `dq_policy_gate` | 0 | A policy decision must never be retried away |
| `load_postgresql` | 1 (5 min) | Safe because the load is one transaction: a failed attempt leaves nothing behind, and a retry after a commit sees the same SHA and does nothing |

### Manual run procedure

```powershell
# Only when no Airflow run is active (the lock refuses otherwise). Uses the same steps as the DAG.
$env:DATABASE_URL = "postgresql://postgres:postgres@localhost:5433/namtang_db"
.\.venv\Scripts\python.exe data/scraper/run_pipeline.py      # exit 0 ok/unchanged, 1 failed, 2 awaiting_policy, 3 locked
```

### Activating the daily schedule (explicit decision)

1. Run the checks: `docker compose -f airflow/docker-compose.yml run --rm airflow-cli` must pass completely.
2. Set `NAMTANG_SCHEDULE_ENABLED=true` in `airflow/.env`.
3. Recreate the Airflow services so they pick it up: `docker compose -f airflow/docker-compose.yml up -d`. `namtang_db` isn't part of this project and isn't touched.
4. Confirm that `airflow dags details namtang_gtfs_pipeline` shows the 06:00 Asia/Bangkok timetable and `airflow dags list-import-errors` is empty.
5. Unpause: `docker compose -f airflow/docker-compose.yml exec airflow-scheduler airflow dags unpause namtang_gtfs_pipeline`.
6. After the first scheduled run, check it in the Grid view and `scrape_log.csv`.

To stop scheduled runs: pause the DAG, or set the switch back to `false` and recreate the services.

### Don't do this in production

- Don't use `load_namtang.py --replace` as a normal workflow.
- Don't edit or delete raw snapshots.
- Don't hand-edit clean or analysis outputs, or write to `namtang_db` by hand.
- Don't auto-generate DQ policies.
- Don't delete a lock file while a run may be active.
- Don't point tests at `namtang_db`.
- Don't touch `itproject_db` (port 5432).
- Don't commit `.env` files or credentials.

### Integrating another source later

Each source gets its own pipeline and keeps its source-specific logic to itself:

```
source-specific ingestion -> source-specific cleaning -> source-specific analysis
    -> its own database schema -> shared backend / analysis layer
```

- **New source:** its own folders (`data/raw/<source>/` and so on), its own DAG (for example `<source>_pipeline`), its own schema(s) and migrations, and its own DQ policies.
- **Shared code:** only generic infrastructure (Airflow stack, logging conventions, lock pattern) may be shared.
- **Keep Namtang separate:** don't merge another scraper's business logic into the Namtang modules, and don't change the Namtang schemas to anticipate another source's data.

## 11. Git and data rules

- **Excluded by `.gitignore`:**
  - data: `data/raw/**`, `data/clean/**`, `data/analysis/**` (except the `.gitkeep` placeholders), `data/bkk_traffic/**` and the pipeline lock file;
  - logs and caches: `airflow/logs/` and `*.log`, `.venv/`, `__pycache__/`, `.pytest_cache/`;
  - secrets: every `.env` (including `airflow/.env`) and `.claude/settings.local.json`;
  - large files: database dumps (`*.dump`, `handoff/**/payload/`) and the friend's 237 MB source archive `scrapping--main.zip`.

  Datasets are rebuilt from the scrapers or handed off separately ([handoff/bkk_traffic/README.md](handoff/bkk_traffic/README.md)), never committed.
- **Committed:** `data/metadata/` (logs, validation reports, DQ policies), code, migrations, tests and documentation.
- **Never commit API keys or real credentials.** The Longdo API key exists only as the encrypted Airflow Variable `longdo_api_key`.

## 12. BKK Traffic pipeline (production)

Longdo traffic data (incidents, the Bangkok traffic index, travel times on 8 fixed routes) is collected into the PostgreSQL schema **`bkk_traffic`** in `namtang_db`. It is ported from a teammate's Longdo pipeline and fully separate from the Namtang schemas.

- **PostgreSQL is the only store and the source of truth.** There is no SQLite dependency. The teammate's original SQLite warehouse is not used.
- **Code:**
  - pipeline: `data/scraper/bkk_traffic/`;
  - migrations: `database/migrations/bkk_traffic/001..006`;
  - initial Parquet loader: `database/load/load_bkk_traffic.py`;
  - DAGs: `airflow/dags/bkk_traffic_*.py`.
- **Design and verification reports:** `docs/integration/bkk_traffic_*.md`.
- **Timestamps** are Bangkok wall-clock time (UTC+7, `timestamp without time zone`).

| DAG | Schedule (Asia/Bangkok) | Production state |
|---|---|---|
| `bkk_traffic_pipeline` | `*/15 * * * *`: scrape → update_history → collect_travel_times → clean → validate → publish → verify_postgresql | **Active** (unpaused 2026-10-03) |
| `bkk_traffic_maintenance` | `30 3 * * *`: deletes BKK task logs older than 7 days (never `airflow db clean`) | **Active** |
| `bkk_traffic_quality` | `0 1 * * *`: event-type refresh, missing-incident detection and recovery (≤ 30 eids / 360 s), quality report, gate | **Paused on purpose during bootstrap** |

**Configuration:**
- **Schedule switch:** `BKK_TRAFFIC_SCHEDULE_ENABLED=true` in `airflow/.env` turns on all three timetables. Each DAG runs only once it is unpaused.
- **Pool:** all Longdo requests go through the Airflow pool `longdo_api`, which has **1 slot**.
- **Database connection:** `bkk_traffic_postgres` is an Airflow connection with a least-privilege login.
- **API key:** an external secret, stored only as the encrypted Airflow Variable `longdo_api_key`. It is never in code, `.env`, logs or Git.

**Quality bootstrap.** The quality gate checks full-day 15-minute coverage of the *previous* Bangkok day, so it FAILs until one complete production day exists. Thresholds are not lowered to get around this.
- Collection started 2026-10-03, so the first full day is 2026-10-04.
- Unpause `bkk_traffic_quality` shortly after 2026-10-05 01:00. Airflow (`catchup=False`) starts one run immediately for the latest past 01:00 when a DAG is unpaused. Unpausing earlier therefore produces an expected partial-day FAIL.

**Data for teammates.** A restorable dump of the complete `bkk_traffic` schema is shared outside Git. See [handoff/bkk_traffic/README.md](handoff/bkk_traffic/README.md).
