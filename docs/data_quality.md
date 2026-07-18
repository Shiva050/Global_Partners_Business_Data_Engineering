# Data Quality — Findings & Handling

Findings from the first end-to-end run on the real data (203,519 order items /
131,328 orders), and how the pipeline handles each. These surfaced only against
real data — unit tests use curated inputs.

| # | Finding | Handling | Where |
|---|---|---|---|
| A | **`date_dim` covers only 2023** (365 rows) but orders span **2020-04 → 2024-02** | **Generate** a full-range daily calendar over the order range; enrich with source holiday flags (accurate for 2023, `is_holiday=false` elsewhere) | `jobs/silver/dim_date.py` |
| B | **No discounts** — `option_price` ∈ [0, 8], zero negatives | No code change; metric correctly reports only "Full-price". Documented so the empty discount view isn't mistaken for a bug | `jobs/gold/discount_effectiveness.py` |
| C | **Revenue outliers** — top order $2.5M (one $5,000 item × 500), next $2.3M, $1.07M; p99.9 = $855 | **Flag, don't drop**: `is_outlier` on orders (revenue > $10,000 threshold). Dashboards filter `is_outlier=false` for clean views; rows retained + auditable | `jobs/silver/order_facts.py` |
| D | **Null `user_id`** on 12,669 orders (9.6%) — anonymous/guest | **Excluded from customer-grain metrics** (CLV, RFM, tiers, churn, loyalty) so they don't collapse into one phantom customer and distort quintiles. Retained in order/location/sales metrics | `jobs/gold/customer_*`, `loyalty_impact.py` |

## Notes
- **A (date dimension):** the provided `date_dim` is treated as a *source of
  holiday annotations*, not the authoritative calendar. Generating the spine
  from the data is what makes CLV (dense per-customer daily) and the sales-trends
  calendar join correct across the full order history. Holiday coverage outside
  2023 is unavailable in the source, so those days are non-holiday by default.
- **C (outliers):** the threshold is a parameter (`outlier_threshold`, default
  $10,000) — well above the legitimate p99.9 ($855) and below the clearly-bad
  $1M+ artifacts, so only genuine anomalies are flagged.
- **D (null customers):** these are still real revenue, so they stay in
  location/sales/discount aggregates; they're only removed where a *customer*
  identity is required.

## Reconciliation (bronze/silver, first run)
- `order_items` 203,519 · `order_item_options` 193,017 · `date_dim` (source) 365
- `silver/facts/order_line_items` 203,519 · `silver/facts/orders` 131,328
- `order_date` parsed from `creation_time_utc` with **0 nulls**
