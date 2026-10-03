# app.py

import pandas as pd
import streamlit as st
from google.cloud import bigquery

PROJECT_ID = "btibert-ba882-fall26"
DATASET = "dev_autoelite_staging"

client = bigquery.Client(project=PROJECT_ID)


# ------------------------------------------------------------------------------
# Page setup
# ------------------------------------------------------------------------------
st.set_page_config(page_title="AutoElite Dashboard", layout="wide")
st.title("BA882 — AutoElite Motors")
st.caption("Demo app reading from **BigQuery** staging layer")


# ------------------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------------------
@st.cache_data(show_spinner=False)
def fetch_df(query: str) -> pd.DataFrame:
    return client.query(query).to_dataframe()


# ------------------------------------------------------------------------------
# KPI metrics
# ------------------------------------------------------------------------------
col1, col2, col3, col4 = st.columns(4)

with col1:
    n = fetch_df(f"SELECT COUNT(*) AS n FROM `{PROJECT_ID}.{DATASET}.stg_order_items`")["n"].iat[0]
    st.metric("Order Line Items", f"{n:,}")

with col2:
    n = fetch_df(f"SELECT COUNT(DISTINCT order_id) AS n FROM `{PROJECT_ID}.{DATASET}.stg_orders`")["n"].iat[0]
    st.metric("Orders", f"{n:,}")

with col3:
    n = fetch_df(f"SELECT COUNT(*) AS n FROM `{PROJECT_ID}.{DATASET}.stg_customers`")["n"].iat[0]
    st.metric("Customers", f"{n:,}")

with col4:
    n = fetch_df(f"SELECT COUNT(*) AS n FROM `{PROJECT_ID}.{DATASET}.stg_reps`")["n"].iat[0]
    st.metric("Sales Reps", f"{n:,}")

st.divider()


# ------------------------------------------------------------------------------
# Sidebar filters
# ------------------------------------------------------------------------------
with st.sidebar:
    st.header("Filters")

    reps_df = fetch_df(f"""
        SELECT rep_id, first_name || ' ' || last_name AS rep_name
        FROM `{PROJECT_ID}.{DATASET}.stg_reps`
        ORDER BY last_name
    """)
    rep_options = ["All"] + reps_df["rep_name"].tolist()
    selected_rep = st.selectbox("Sales Rep", rep_options)


# ------------------------------------------------------------------------------
# Tabs
# ------------------------------------------------------------------------------
tab_orders, tab_products, tab_reps = st.tabs(["Orders", "Products", "Reps"])

# ---------------------------------- Orders ------------------------------------
with tab_orders:
    st.subheader("Order Line Items")

    where = ""
    if selected_rep != "All":
        rep_id = reps_df.loc[reps_df["rep_name"] == selected_rep, "rep_id"].iat[0]
        where = f"AND o.rep_id = '{rep_id}'"

    df = fetch_df(f"""
        SELECT
            oi.order_item_id,
            o.order_date,
            o.order_status,
            r.first_name || ' ' || r.last_name AS rep_name,
            p.product_name,
            oi.quantity,
            oi.unit_price,
            ROUND(oi.quantity * oi.unit_price, 2) AS total_price
        FROM `{PROJECT_ID}.{DATASET}.stg_order_items` oi
        LEFT JOIN `{PROJECT_ID}.{DATASET}.stg_orders` o ON o.order_id = oi.order_id
        LEFT JOIN `{PROJECT_ID}.{DATASET}.stg_reps` r ON r.rep_id = o.rep_id
        LEFT JOIN `{PROJECT_ID}.{DATASET}.stg_products` p ON p.product_id = oi.product_id
        WHERE 1=1 {where}
        ORDER BY o.order_date DESC
        LIMIT 500
    """)
    st.dataframe(df, use_container_width=True)

# ---------------------------------- Products ----------------------------------
with tab_products:
    st.subheader("Revenue by Product")

    df = fetch_df(f"""
        SELECT
            p.product_name,
            COUNT(*) AS line_items,
            ROUND(SUM(oi.quantity * oi.unit_price), 2) AS total_revenue
        FROM `{PROJECT_ID}.{DATASET}.stg_order_items` oi
        LEFT JOIN `{PROJECT_ID}.{DATASET}.stg_products` p ON p.product_id = oi.product_id
        GROUP BY p.product_name
        ORDER BY total_revenue DESC
    """)
    st.bar_chart(df.set_index("product_name")["total_revenue"])
    st.dataframe(df, use_container_width=True)

# ---------------------------------- Reps --------------------------------------
with tab_reps:
    st.subheader("Revenue by Sales Rep")

    df = fetch_df(f"""
        SELECT
            r.first_name || ' ' || r.last_name AS rep_name,
            COUNT(DISTINCT o.order_id) AS orders,
            ROUND(SUM(oi.quantity * oi.unit_price), 2) AS total_revenue
        FROM `{PROJECT_ID}.{DATASET}.stg_order_items` oi
        LEFT JOIN `{PROJECT_ID}.{DATASET}.stg_orders` o ON o.order_id = oi.order_id
        LEFT JOIN `{PROJECT_ID}.{DATASET}.stg_reps` r ON r.rep_id = o.rep_id
        GROUP BY rep_name
        ORDER BY total_revenue DESC
    """)
    st.bar_chart(df.set_index("rep_name")["total_revenue"])
    st.dataframe(df, use_container_width=True)


# ------------------------------------------------------------------------------
# Teaching notes
# ------------------------------------------------------------------------------
with st.expander("What this app demonstrates (teaching notes)"):
    st.markdown("""
- **BigQuery connection**: `bigquery.Client()` uses the Cloud Run service account automatically — no key files
- **Query pattern**: `client.query(sql).to_dataframe()` → Pandas → `st.*` to render
- **Caching**: `@st.cache_data` so repeated interactions don't re-query BigQuery
- **Layout**: columns for KPIs, sidebar for filters, tabs to separate concerns
- **Deploy**: `gcloud builds submit` builds the image in Cloud Build — no local Docker needed
    """)
