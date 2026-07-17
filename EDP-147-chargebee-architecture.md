# EDP-147 — Chargebee Modelling: Architecture & Design Walkthrough

**Branch:** `EDP-147-Chargebee-Modelling`  
**Scope:** New monthly close infrastructure, Finance correction layer, and analytics refactor for all `cb_*` Chargebee models.

---

## 1. Why We Rebuilt This

The original Chargebee stack had three structural problems that would have compounded over time:

| # | Problem | Impact |
|---|---|---|
| **Critical 1** | Customer attributes (billing_country, pod, roller_payments, etc.) stored in `snp_cb_subscription` | Any customer rename triggers false subscription SCD2 rows, inflating history |
| **Critical 2** | `chargebee_mrr_lc` (a measure) included in `check_cols` of the subscription snapshot | Every price change creates an unnecessary new SCD2 version even when nothing else changed |
| **Critical 3** | `chargebee_new_customers.sql` (330 lines, 6 heavy CTEs) doing all SCD2 joins, MRR pivots, and discount calculations inline — nothing reusable | Logic cannot be shared across reports; Finance corrections require manual re-runs in every model |

On top of the structural issues, there was no durable monthly close mechanism. The Finance team relied on a manual Google Sheet for new customer reporting. If the end-of-month job failed, there was no fallback, and Finance corrections had no pathway into the data pipeline.

---

## 2. What Was Fixed (Critical 1 & 2)

### Critical 1 — Customer attributes removed from subscription snapshot

**Files changed:** `models/silver/conformed/cb_subscription.sql`, `snapshots/snp_cb_subscription.sql`

Removed 8 customer columns (`account_guid_bridge`, `billing_country`, `pod`, `customer_segment`, `managed_account`, `roller_payments`, `roller_payments_customer_type`, `book_now_migrated_customer`) from the subscription conformed model and its snapshot.

These now live exclusively in `snp_cb_customer`. Point-in-time customer attributes are fetched via `fact_cb_customer_month` (SCD2 join at month-end).

### Critical 2 — MRR measure excluded from SCD2 check_cols

**File changed:** `snapshots/snp_cb_subscription.sql`

Changed `check_cols='all'` to an explicit 20-column list that excludes `chargebee_mrr_lc`. The MRR field is still SELECT-ed (kept for backward compatibility of existing as-of queries) but price-only changes no longer trigger a new SCD2 row.

```
check_cols = [customer_id, plan_name, status, subscription_start_date,
              billing_cycle, billing_cycle_months, currency_code,
              venue_name, venue_id, venue_unique_identifier,
              tier_roller_payments, tier_api, tier_3rd_party_processing,
              auto_suspend, next_billing_at, activated_at, current_term_start,
              pause_date, resume_date, cancelled_at]
```

---

## 3. New Architecture — Full Picture

```mermaid
flowchart TD
    subgraph BRONZE["Bronze (raw)"]
        B1[chargebee_subscriptions]
        B2[chargebee_customers]
        B3[chargebee_subscription_items]
    end

    subgraph SILVER["Silver"]
        direction TB
        S1["stg_cb_subscription\n(dedup + cast)"]
        S2["stg_cb_customer\n(dedup + cast)"]
        S3["stg_cb_subscription_item\n(dedup + mrr_category)"]
        C1["cb_subscription\n(conformed table)"]
        C2["cb_customer\n(conformed table)"]
        C3["cb_subscription_item\n(conformed table)"]
    end

    subgraph SNAPSHOTS["SCD2 Snapshots (roller_gold_snapshots)"]
        SN1["snp_cb_subscription\n(check_cols — no MRR, no customer attrs)"]
        SN2["snp_cb_customer\n(check_cols=all)"]
        SN3["snp_cb_subscription_item\n(check_cols=all)"]
    end

    subgraph GOLD_FACTS["Gold Facts (roller_gold_finance)"]
        direction TB
        F0["fact_cb_subscription_item_snapshot\n(incremental — item line MRR)"]
        F1["fact_cb_subscription_daily_snapshot\n(incremental by day — today's active subs)"]
        F2["fact_cb_active_subscription_month\n(incremental by month — IMMUTABLE close)"]

        subgraph SEED_LAYER["Seeds"]
            SD1["cb_may_2026_active_seed\n(May baseline — Finance master sheet)"]
            SD2["cb_subscription_correction\n(Finance corrections — add/remove)"]
        end

        F3["fact_cb_active_subscription_month_official\n(TABLE — corrections applied — ALL reports read here)"]
        F4["fact_cb_subscription_month\n(TABLE — subscription dim attrs point-in-time)"]
        F5["fact_cb_customer_month\n(TABLE — customer dim attrs point-in-time)"]
        F6["fact_cb_subscription_mrr_month\n(TABLE — MRR by category point-in-time)"]
    end

    subgraph DIMS["Gold Dimensions"]
        D1["dim_account\n(SFDC — vertical / sfdc_account_id)"]
        D2["dim_location\n(country code → name)"]
    end

    subgraph ANALYTICS["Gold Analytics (roller_gold_finance)"]
        A1["chargebee_new_customers\n(thin — anti-join + equality joins only)"]
    end

    B1 --> S1 --> C1 --> SN1
    B2 --> S2 --> C2 --> SN2
    B3 --> S3 --> C3 --> SN3

    C1 --> F1
    C2 --> F1
    SN3 --> F0

    F1 --> F2
    SD1 --> F2
    SD2 --> F3

    F2 --> F3
    SN1 --> F4
    SN2 --> F5
    F0 --> F6

    F3 --> F4
    F3 --> F5
    F3 --> F6
    F3 --> A1
    F4 --> A1
    F5 --> A1
    F6 --> A1
    D1 --> A1
    D2 --> A1
```

---

## 4. Layer-by-Layer Explanation

### 4.1 Silver — Staging & Conformed

| Model | Purpose |
|---|---|
| `stg_cb_*` | Dedup from bronze (ROW_NUMBER on updated_at DESC, resource_version DESC), SAFE_CAST, basic filtering |
| `cb_subscription` | Conformed subscription facts — pure subscription attrs only (no customer cols) |
| `cb_customer` | Conformed customer attrs — company, billing_country, pod, roller_payments flags, SFDC bridge key |
| `cb_subscription_item` | Conformed line items — mrr_category CASE logic, excludes item_type='charge' (one-time) |

### 4.2 SCD2 Snapshots

| Snapshot | Tracks | Notes |
|---|---|---|
| `snp_cb_subscription` | Subscription dim attributes | check_cols explicit list, excludes chargebee_mrr_lc |
| `snp_cb_customer` | Customer attributes | check_cols='all' |
| `snp_cb_subscription_item` | Line item amounts by mrr_category | check_cols='all' |

All snapshots use `hard_deletes='invalidate'` — deleted records get a `dbt_valid_to` set rather than being physically removed.

### 4.3 Daily Snapshot — Resilience Layer

```
fact_cb_subscription_daily_snapshot
  Grain: (subscription_id, source_instance, snapshot_date)
  Partition: snapshot_date (day)
  Materialization: incremental (insert_overwrite)
  Run cadence: daily
```

Captures every billing-active subscription (status IN active/non_renewing/paused) as of `CURRENT_DATE()`. Filters out HQ/DNU/DUMMY company names.

**Why this exists:** If the month-end job fails on the 30th/31st, the monthly close can fall back to the 29th snapshot automatically — no manual intervention required.

### 4.4 Monthly Close — Immutable Base

```
fact_cb_active_subscription_month
  Grain: (subscription_id, source_instance, month_end_date)
  Partition: month_end_date (month)
  Materialization: incremental (insert_overwrite, full_refresh=false)
  Run cadence: 3rd of each month
```

Close logic:
1. `period` CTE computes `DATE_SUB(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 1 DAY)` = last day of prior month.
2. `close_date` CTE finds `MAX(snapshot_date)` from the daily snapshot within that month (fallback if last-day job failed).
3. `computed` CTE reads the daily snapshot at that close date.
4. `seed` CTE re-emits the May 2026 seed partition (Finance master sheet baseline — idempotent on every run).
5. `UNION ALL` of computed + seed = final output.

`full_refresh=false` prevents accidental `dbt run --full-refresh` from wiping immutable historical partitions.

`close_snapshot_date` records which daily snapshot was actually used — Finance can always audit which day's data closed a given month.

### 4.5 Finance Correction Layer — Official

```
fact_cb_active_subscription_month_official
  Grain: (subscription_id, source_instance, month_end_date)
  Materialization: TABLE (full rebuild on every run)
  Reads from: fact_cb_active_subscription_month + cb_subscription_correction seed
```

This is the **authoritative active-subscription record**. Every downstream model reads from this — never directly from the base.

Two correction types:
- `action='remove'` — subscription is excluded from the closed month (e.g. Finance reversed a charge)
- `action='add'` — subscription is added to the closed month (e.g. missed in the auto capture)

`is_active_correction=FALSE` revokes a correction without deleting the audit row.

`record_source` column: `'auto'` for system rows, `'finance_correction'` for seed-added rows.

### 4.6 Monthly Dimension Locks

These three TABLE models read from `fact_cb_active_subscription_month_official` and perform point-in-time SCD2 joins. They lock dimension attributes at month-end so downstream analytics don't have to.

| Model | SCD2 Source | What It Locks |
|---|---|---|
| `fact_cb_subscription_month` | `snp_cb_subscription` | plan_name, billing_cycle_months, currency_code, venue, tiers, dates |
| `fact_cb_customer_month` | `snp_cb_customer` | account_guid_bridge, billing_country, pod, roller_payments flags |
| `fact_cb_subscription_mrr_month` | `fact_cb_subscription_item_snapshot` + `snp_cb_subscription` | All 8 MRR categories, discount, net MRR, discount % |

**Point-in-time join formula used in all three:**

```sql
month_end_ts = TIMESTAMP_ADD(
    TIMESTAMP(DATE_ADD(month_end_date, INTERVAL 1 DAY)),
    INTERVAL -1 SECOND
)
-- i.e. 2026-06-30T23:59:59Z

JOIN snp_cb_subscription AS s
  ON  s.dbt_valid_from <= month_end_ts
  AND (s.dbt_valid_to IS NULL OR s.dbt_valid_to > month_end_ts)
```

### 4.7 MRR Calculation — fact_cb_subscription_mrr_month

MRR categories (from `stg_cb_subscription_item.mrr_category`):

| Category | Counts toward gross MRR? |
|---|---|
| platform_plan | Yes |
| gxs | Yes |
| api | Yes |
| waivers | Yes |
| hq | Yes |
| volare | Yes |
| other | Yes |
| rp_sim_fees | No — non-MRR addon |
| rp_terminal_rental | No — non-MRR addon |
| implementation_instalment | No — non-MRR addon |

Key formulas:

```
platform_plan_mrr_lc   = ROUND(platform_plan_lc_raw  / billing_cycle_months, 2)
total_gross_mrr_lc     = SUM of all 7 MRR categories ÷ billing_cycle_months
recurring_discount_lc  = ABS(chargebee_mrr_lc − all_items_sum_raw / billing_cycle_months)
total_net_mrr_lc       = total_gross_mrr_lc − recurring_discount_lc
discount_pct_of_gross  = recurring_discount_lc / total_gross_mrr_lc × 100
```

`chargebee_mrr_lc` is Chargebee's own pre-calculated MRR (locked from the official layer) and serves as the **single authoritative net MRR baseline**. It is never derived from item amounts.

### 4.8 Analytics — chargebee_new_customers (Refactored)

**Before:** 330 lines, 6 heavy CTEs performing SCD2 joins and MRR pivot inline.

**After:** ~160 lines — anti-join only, then simple equality joins to the three monthly fact tables.

```sql
-- Anti-join: billing-active in current month, not in prior month
FROM fact_cb_active_subscription_month_official AS curr
LEFT JOIN fact_cb_active_subscription_month_official AS prev
    ON curr.subscription_id = prev.subscription_id
   AND curr.source_instance = prev.source_instance
   AND prev.month_end_date = DATE_SUB(DATE_TRUNC(curr.month_end_date, MONTH), INTERVAL 1 DAY)
   AND prev.is_active_billing = TRUE
WHERE curr.is_active_billing = TRUE
  AND prev.subscription_id IS NULL
  AND curr.month_end_date > DATE '2026-05-31'   -- baseline month is reference, not a reporting month
```

Then joins to:
- `fact_cb_subscription_month` — plan name, billing cycle, venue, dates
- `fact_cb_customer_month` — SFDC bridge key, billing country, pod
- `fact_cb_subscription_mrr_month` — all MRR columns pre-computed
- `dim_account` — Finance vertical (experience_category + sub_vertical)
- `dim_location` — country code → full name

---

## 5. Monthly Close Flow

```mermaid
sequenceDiagram
    participant Orchestrator
    participant DailyJob as Daily Job (every day)
    participant MonthlyJob as Monthly Job (3rd of month)
    participant BQ as BigQuery

    loop Every day (incl. month-end)
        Orchestrator ->> DailyJob: trigger
        DailyJob ->> BQ: INSERT INTO fact_cb_subscription_daily_snapshot<br/>PARTITION BY CURRENT_DATE()<br/>WHERE status IN (active, non_renewing, paused)
    end

    Note over Orchestrator,BQ: Month-end arrives (e.g. June 30).<br/>Job may succeed or fail.

    Orchestrator ->> MonthlyJob: trigger on 3rd of next month
    MonthlyJob ->> BQ: SELECT MAX(snapshot_date) FROM daily_snapshot<br/>WHERE snapshot_date BETWEEN June 1 AND June 30
    BQ -->> MonthlyJob: close_snapshot_date = 2026-06-30 (or 29th if 30th failed)
    MonthlyJob ->> BQ: INSERT OVERWRITE month_end_date=2026-06-30 partition<br/>INTO fact_cb_active_subscription_month
    MonthlyJob ->> BQ: REBUILD fact_cb_active_subscription_month_official<br/>(corrections applied on top)
    MonthlyJob ->> BQ: REBUILD fact_cb_subscription_month<br/>fact_cb_customer_month<br/>fact_cb_subscription_mrr_month
    MonthlyJob ->> BQ: REBUILD chargebee_new_customers
```

---

## 6. Finance Correction Cascade

```mermaid
sequenceDiagram
    participant Finance
    participant DataTeam as Data Team
    participant Seed as cb_subscription_correction.csv
    participant dbt as dbt run
    participant Official as fact_cb_active_subscription_month_official
    participant Downstream as Downstream Facts + Analytics

    Finance ->> DataTeam: "Sub X shouldn't be in June — they cancelled before month-end"
    DataTeam ->> Seed: Add row:<br/>correction_id=CORR-2026-06-001<br/>report_month=2026-06-30<br/>action=remove<br/>reason="cancelled pre-month-end"<br/>is_active_correction=TRUE

    DataTeam ->> dbt: dbt run (or nightly pipeline runs automatically)

    dbt ->> Official: Full rebuild (TABLE materialization)<br/>Base rows WHERE subscription not in active removals<br/>UNION ALL added rows from seed
    Official -->> Downstream: fact_cb_subscription_month rebuilt
    Official -->> Downstream: fact_cb_customer_month rebuilt
    Official -->> Downstream: fact_cb_subscription_mrr_month rebuilt
    Official -->> Downstream: chargebee_new_customers rebuilt

    Note over Finance,Downstream: All downstream facts now reflect<br/>the correction. Zero manual re-runs.<br/>Audit trail preserved in seed file.
```

**Revoking a correction** (without deleting): set `is_active_correction=FALSE` in the seed row. The next dbt run will restore the original auto-generated row.

---

## 7. New Customer Detection — Business Logic

A subscription is **new** in month M if:
1. It is billing-active (`is_active_billing = TRUE`) in month M
2. It was NOT billing-active in month M−1

`is_active_billing = TRUE` when `status IN ('active', 'non_renewing')`.
`is_active_billing = FALSE` when `status = 'paused'` — so **Paused → Active** counts as new (the prior month join returns NULL).

**Baseline month:** May 2026 (`cb_may_2026_active_seed`) is the reference set for the June anti-join. It is NOT a reporting month — the model filters `month_end_date > DATE '2026-05-31'` to exclude it from the output.

**Deferred start flag:** `is_deferred_start = TRUE` when the billing start date (`subscription_start_date` / `cf_billing_start_date`) and `current_term_start` are both in a **later** calendar month than `activated_at`. US subscriptions use the raw activation date; non-US adds 1 day for UTC offset.

```
-- US logic
is_deferred_start = (
    DATE_TRUNC(subscription_start_date, MONTH) > DATE_TRUNC(DATE(activated_at), MONTH)
    AND
    DATE_TRUNC(DATE(current_term_start), MONTH) > DATE_TRUNC(DATE(activated_at), MONTH)
)

-- Non-US (UTC+1 offset)
is_deferred_start = (
    DATE_TRUNC(subscription_start_date, MONTH) > DATE_TRUNC(DATE_ADD(DATE(activated_at), INTERVAL 1 DAY), MONTH)
    AND
    DATE_TRUNC(DATE(current_term_start), MONTH) > DATE_TRUNC(DATE_ADD(DATE(activated_at), INTERVAL 1 DAY), MONTH)
)
```

---

## 8. Files Changed / Created

### Modified

| File | Change |
|---|---|
| `models/silver/conformed/cb_subscription.sql` | Removed 8 customer columns (Critical 1) |
| `snapshots/snp_cb_subscription.sql` | Removed customer columns; changed check_cols to explicit list excluding chargebee_mrr_lc (Critical 2) |
| `models/gold/facts/finance/fact_cb_active_subscription_month.sql` | Full rewrite — new close logic with daily snapshot fallback + seed union |
| `models/gold/analytics/finance/chargebee_new_customers.sql` | Refactored — removed 6 heavy CTEs; now uses 3 equality joins to new monthly facts |
| `models/gold/facts/finance/schema.yml` | Added close_snapshot_date column; documented all new models |
| `seeds/finance/schema.yml` | Added full documentation for cb_subscription_correction seed |
| `dbt_project.yml` | Added cb_subscription_correction seed column types |

### New

| File | Purpose |
|---|---|
| `models/gold/facts/finance/fact_cb_subscription_daily_snapshot.sql` | Daily active-sub snapshot (resilience layer) |
| `models/gold/facts/finance/fact_cb_active_subscription_month_official.sql` | Authoritative monthly fact with correction overlay |
| `models/gold/facts/finance/fact_cb_subscription_month.sql` | Subscription dim attrs locked per month |
| `models/gold/facts/finance/fact_cb_customer_month.sql` | Customer dim attrs locked per month |
| `models/gold/facts/finance/fact_cb_subscription_mrr_month.sql` | MRR by category pre-computed per month |
| `seeds/finance/cb_subscription_correction.csv` | Finance correction rows (headers-only — data team adds corrections here) |

---

## 9. Materialization Strategy Summary

| Model | Strategy | Why |
|---|---|---|
| `fact_cb_subscription_daily_snapshot` | Incremental, insert_overwrite by day | Accumulates daily history; idempotent re-run |
| `fact_cb_active_subscription_month` | Incremental, insert_overwrite by month, full_refresh=false | Immutable once written; seed always re-emits May |
| `fact_cb_active_subscription_month_official` | TABLE | Full rebuild guarantees corrections always cascade |
| `fact_cb_subscription_month` | TABLE | Must reflect latest corrections on every run |
| `fact_cb_customer_month` | TABLE | Must reflect latest corrections on every run |
| `fact_cb_subscription_mrr_month` | TABLE | Must reflect latest corrections on every run |
| `chargebee_new_customers` | TABLE | Must reflect latest corrections on every run |

**Key rule:** Everything downstream of the official layer is a TABLE. Tables fully rebuild on every run, so a correction in the seed file propagates to `chargebee_new_customers` in a single `dbt run` with no additional flags or manual steps.

---

## 10. Run Order (DAG)

```
Bronze
  └─ Silver Staging (views)
       └─ Silver Conformed (tables)
            ├─ SCD2 Snapshots (daily)
            │    ├─ snp_cb_subscription
            │    ├─ snp_cb_customer
            │    └─ snp_cb_subscription_item
            │         └─ fact_cb_subscription_item_snapshot (incremental)
            │
            ├─ fact_cb_subscription_daily_snapshot (incremental — daily)
            │    └─ fact_cb_active_subscription_month (incremental — monthly close)
            │         └─ fact_cb_active_subscription_month_official (table — corrections)
            │              ├─ fact_cb_subscription_month (table)
            │              ├─ fact_cb_customer_month (table)
            │              └─ fact_cb_subscription_mrr_month (table)
            │                   └─ chargebee_new_customers (table)
            │
            └─ [seeds: cb_may_2026_active_seed, cb_subscription_correction]
```

---

## 11. What's Deferred

| Item | Status | Reason |
|---|---|---|
| AUD/USD conversion columns (`*_aud`) | NULL placeholders in chargebee_new_customers | FX rate table approach not confirmed |
| `sales_pipeline` column | NULL placeholder | Requires SFDC opportunity join via account_guid_bridge (pending Abhishek confirmation) |
| `customer_segment`, `managed_account` attrs | Not in snp_cb_customer | Can be added to the snapshot when needed |
| EMEA/APAC historical monthly closes pre-June 2026 | Not modelled | No daily snapshots exist for prior months; requires seed files similar to cb_may_2026_active_seed |

---

## 12. How to Apply a Finance Correction (Runbook)

1. Open `seeds/finance/cb_subscription_correction.csv`
2. Add a new row following the format:

```
correction_id,report_month,subscription_id,source_instance,action,is_active_correction,reason,applied_by,...
CORR-2026-06-001,2026-06-30,SUB_XYZ,us,remove,TRUE,"Cancelled before month-end — Finance confirmed",sridhar.iyer@rollerdigital.com,,,,,,
```

3. For `action='add'` also fill in: `customer_id`, `status`, `chargebee_mrr_lc`, `company`, `is_active_billing`, `is_deferred_start`
4. Commit the seed file
5. Run `dbt run` (or let the nightly pipeline run)
6. All downstream facts and analytics rebuild automatically

**To revoke a correction:** set `is_active_correction=FALSE` — the audit row is preserved, the correction is unapplied on the next run.
