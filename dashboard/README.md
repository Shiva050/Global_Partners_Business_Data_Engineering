# Streamlit Dashboard

Interactive dashboard for the six BRD deliverables, reading the **gold** tables
from S3. All heavy computation is in the PySpark gold jobs; this app only reads
small pre-aggregated tables and draws them — so it loads fast and stays in sync
with the pipeline.

## Pages
| Page | Reads (gold) | Answers |
|---|---|---|
| Overview — CLV | `clv_trends`, `customer_clv_tiers` | how CLV evolves daily; High/Med/Low tiers |
| Customer Segmentation | `customer_rfm` | RFM segments (VIP / Churn Risk / …), F-vs-M scatter |
| Churn Risk | `churn_indicators` | Active/At Risk/Dormant, days-since-last, at-risk list |
| Sales Trends | `sales_daily` | monthly revenue, top categories, holiday effect |
| Loyalty Impact | `loyalty_impact` | loyalty vs non — CLV, AOV, repeat rate |
| Location Performance | `location_performance` | revenue rank, AOV, orders per store |
| Discount Effectiveness | `discount_effectiveness` | discounted vs full-price (none in this data) |

## Run

```bash
cd dashboard
pip install -r requirements.txt

# AWS credentials must be available (env vars, profile, or role) with read access
# to the bronze bucket. Same creds you used for the Glue runs.
streamlit run app.py
```

Opens at http://localhost:8501. Use the sidebar to switch dashboards; **Refresh
data** clears the 10-minute cache.

Override the bucket if needed:
```bash
export GPB_BRONZE_BUCKET=dms-global-partne-brusiness-bronze
```

## Design notes
- **Data access:** lists each table's parquet parts with boto3 and reads them
  (avoids the `s3fs`/pyarrow `HeadObject`-on-prefix 403 on directory-style keys).
  Cached via `st.cache_data`.
- **Charts:** built with a **CVD-validated** categorical palette (worst adjacent
  ΔE 11.0); status colours (Active/At Risk/Dormant) are reserved green/amber/red;
  every chart has a legend or direct labels plus a table view. The app commits to
  a **light theme** (the validated palette is a light-surface palette).
- **No dual axes:** metrics on different scales (loyalty CLV vs repeat rate) are
  drawn as separate small charts, never two y-axes.
- **CLV at scale:** the dense 11M-row `customer_clv_daily` is never loaded; the
  daily portfolio view reads the pre-aggregated `clv_trends` (1,402 rows).
