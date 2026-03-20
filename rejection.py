from curses.ascii import alt
from datetime import date, datetime, timedelta
from urllib.error import URLError
import pandas as pd
import streamlit as st
from streamlit_option_menu import option_menu
import psycopg2
from functools import wraps
import pandas as pd
import hmac
import json
import stripe
import requests
from urllib.parse import urlparse
import plotly.express as px
import plotly.graph_objects as go
import numpy as np


# Read credentials directly from Streamlit secrets
db = st.secrets["db"]
name = st.secrets["name"]
passw = st.secrets["passw"]
server = st.secrets["server"]
port = st.secrets["port"]
stripe_key = st.secrets["stripe"]


# ─── Page Config ───
st.set_page_config(
    page_title="Ad Rejection Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)


# st.toast('Successfully connected to the database!!', icon='😍')

st.write("Successfully connected to the database!")

def redshift_connection(dbname, user, password, host, port):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            try:

                connection = psycopg2.connect(
                    dbname=dbname,
                    user=user,
                    password=password,
                    host=host,
                    port=port
                )

                cursor = connection.cursor()

                print("Connected to Redshift!")

                result = func(*args, connection=connection, cursor=cursor, **kwargs)

                cursor.close()
                connection.close()

                print("Disconnected from Redshift!")

                return result

            except Exception as e:
                print(f"Error: {e}")
                return None

        return wrapper

    return decorator

query = '''
SELECT buid,a.ad_account_id,a.ad_id,ad_status,effective_status,a.created_at,edited_at as status_change_date,error_type,error_description
-- ,spend
 FROM
(
SELECT a.ad_account_id,ad_id,ad_status,effective_status,edited_at,a.created_at,ad_review_feedback,error_description,error_type
 FROM
(SELECT 
  fad.ad_account_id,
  ad_id,
  CASE 
    WHEN effective_status = 'DISAPPROVED' THEN 'DISAPPROVED' 
    ELSE 'APPROVED' 
  END AS ad_status,
  effective_status,
  DATE(fad.edited_at) AS edited_at,
  DATE(fad.created_date) AS created_at,

  -- Remove curly braces safely
  SPLIT_PART(
    REPLACE(REPLACE(JSON_EXTRACT_PATH_TEXT(ad_review_feedback, 'global'), '{', ''), '}', ''),
    '=',
    1
  ) AS error_type,

  LTRIM(
    SPLIT_PART(
      REPLACE(REPLACE(JSON_EXTRACT_PATH_TEXT(ad_review_feedback, 'global'), '{', ''), '}', ''),
      '=',
      2
    )
  ) AS error_description,

  ROW_NUMBER() OVER (PARTITION BY ad_id ORDER BY DATE(fad.edited_at) DESC) AS rw,
  ad_review_feedback

FROM zocket_global.fb_ads_details_v3 fad
JOIN zocket_global.fb_child_ad_accounts fcaa 
  ON fad.ad_account_id = fcaa.ad_account_id
)a
where rw=1
) a
-- left join
-- ( select ad_id,sum(spend)spend  from zocket_global.fb_ads_age_gender_metrics_v3 
-- group by 1)b on a.ad_id=b.ad_id
left join zocket_global.fb_child_ad_accounts d on a.ad_account_id = d.ad_account_id
left join zocket_global.fb_child_business_managers e on e.id = d.app_business_manager_id
left join 
    (SELECT     id ,name,brand_name,json_extract_path_text(json_extract_array_element_text(business_user_ids, 0), 'role') AS role,
     json_extract_path_text(json_extract_array_element_text(business_user_ids, 0), 'business_user_id') AS buid
 FROM
     zocket_global.business_profile
 WHERE
     json_extract_path_text(json_extract_array_element_text(business_user_ids, 0), 'role') = 'owner' )bp on e.app_business_id=bp.id
'''


# @st.cache_data(ttl=36400)  # 86400 seconds = 24 hours
@redshift_connection(db,name,passw,server,port)
def execute_query(connection, cursor,query):

    cursor.execute(query)
    column_names = [desc[0] for desc in cursor.description]
    result = pd.DataFrame(cursor.fetchall(), columns=column_names)

    return result

# df = execute_query(query=query)


st.title('FB Rejection')


df = execute_query(query=query)
# Redshift DATE → Python date objects; normalize to datetime64 so .dt / .date() work.
if df is not None and not df.empty:
    for _col in ("created_at", "status_change_date"):
        if _col in df.columns:
            df[_col] = pd.to_datetime(df[_col], errors="coerce")

# ─── Custom CSS ───
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=DM+Sans:ital,opsz,wght@0,9..40,300;0,9..40,500;0,9..40,700;1,9..40,400&display=swap');

html, body, [class*="css"] {
    font-family: 'DM Sans', sans-serif;
}

/* Metric cards — readable labels, no ellipsis truncation */
div[data-testid="column"] > div[data-testid="stVerticalBlock"] > div[data-testid="stMetric"] {
    width: 100%;
}
div[data-testid="stMetric"] {
    background: linear-gradient(155deg, #1e293b 0%, #0f172a 55%, #172554 100%);
    border: 1px solid rgba(148, 163, 184, 0.35);
    border-radius: 12px;
    padding: 18px 20px 16px 20px;
    min-height: 108px;
    box-shadow: 0 4px 20px rgba(0, 0, 0, 0.35), inset 0 1px 0 rgba(255, 255, 255, 0.06);
    transition: box-shadow 0.2s, border-color 0.2s;
}
div[data-testid="stMetric"]:hover {
    box-shadow: 0 8px 28px rgba(0, 0, 0, 0.45), inset 0 1px 0 rgba(255, 255, 255, 0.08);
    border-color: rgba(96, 165, 250, 0.45);
}
div[data-testid="stMetric"] label {
    color: #cbd5e1 !important;
    font-size: 0.9rem !important;
    font-weight: 600 !important;
    text-transform: none !important;
    letter-spacing: 0 !important;
    line-height: 1.35 !important;
    width: 100% !important;
    max-width: 100% !important;
}
div[data-testid="stMetric"] label *,
div[data-testid="stMetric"] [data-testid="stMarkdownContainer"] p {
    color: #cbd5e1 !important;
    white-space: normal !important;
    overflow: visible !important;
    text-overflow: unset !important;
    word-break: break-word !important;
    line-height: 1.35 !important;
}
div[data-testid="stMetric"] [data-testid="stMetricValue"] {
    color: #f8fafc !important;
    font-weight: 800 !important;
    font-size: 1.85rem !important;
    letter-spacing: -0.02em;
}
div[data-testid="stMetric"] [data-testid="stMetricDelta"] {
    font-size: 0.8125rem !important;
    font-weight: 500 !important;
    margin-top: 8px;
}
div[data-testid="stMetric"] [data-testid="stMetricDelta"] *,
div[data-testid="stMetric"] [data-testid="stMetricDelta"] svg {
    overflow: visible !important;
}

/* Sidebar */
section[data-testid="stSidebar"] {
    background: linear-gradient(180deg, #1e293b 0%, #0f172a 100%);
}
section[data-testid="stSidebar"] .stMarkdown p,
section[data-testid="stSidebar"] .stMarkdown h1,
section[data-testid="stSidebar"] .stMarkdown h2,
section[data-testid="stSidebar"] .stMarkdown h3,
section[data-testid="stSidebar"] label {
    color: #e2e8f0 !important;
}

/* Header */
.main-header {
    background: linear-gradient(135deg, #1e293b 0%, #334155 100%);
    border-radius: 16px;
    padding: 32px 40px;
    margin-bottom: 24px;
    color: white;
}
.main-header h1 {
    margin: 0;
    font-size: 2rem;
    font-weight: 700;
    letter-spacing: -0.5px;
}
.main-header p {
    margin: 8px 0 0 0;
    opacity: 0.7;
    font-size: 0.95rem;
}

/* Section headers — self-contained bar, readable on any page bg */
.section-header {
    font-size: 1.2rem;
    font-weight: 700;
    color: #f8fafc !important;
    margin: 28px 0 14px 0;
    padding: 12px 18px;
    border-radius: 10px;
    border-left: 4px solid #3b82f6;
    display: block;
    letter-spacing: -0.02em;
    background: linear-gradient(135deg, #1e293b 0%, #334155 100%);
    box-shadow: 0 2px 8px rgba(15, 23, 42, 0.15);
}
.section-header span {
    color: inherit !important;
}

/* Alert cards */
.alert-card {
    background: linear-gradient(135deg, #fef2f2 0%, #fee2e2 100%);
    border-left: 4px solid #ef4444;
    border-radius: 8px;
    padding: 16px 20px;
    margin: 8px 0;
}
.alert-card-warn {
    background: linear-gradient(135deg, #fffbeb 0%, #fef3c7 100%);
    border-left: 4px solid #f59e0b;
    border-radius: 8px;
    padding: 16px 20px;
    margin: 8px 0;
}

/* Tabs */
.stTabs [data-baseweb="tab-list"] {
    gap: 8px;
}
.stTabs [data-baseweb="tab"] {
    border-radius: 8px 8px 0 0;
    padding: 8px 20px;
    font-weight: 500;
}

/* Dataframe */
.stDataFrame {
    border-radius: 12px;
    overflow: hidden;
}

/* Hide streamlit branding */
#MainMenu {visibility: hidden;}
footer {visibility: hidden;}
</style>
""", unsafe_allow_html=True)


today = df["created_at"].max()
yesterday = today - timedelta(days=1)
data_start = df["created_at"].min().date()
data_end = df["created_at"].max().date()

# ─── Sidebar Filters ───
with st.sidebar:
    st.markdown("## 🎛️ Filters")
    st.markdown("---")
    
    # Date range — default spans full dataset so charts/tables start unfiltered
    date_range = st.date_input(
        "📅 Created date range",
        value=(data_start, data_end),
        min_value=data_start,
        max_value=data_end,
        help="Narrows tab content below. KPI cards above always use full data.",
    )
    
    # Ad status filter
    status_filter = st.multiselect(
        "📋 Ad Status",
        options=["APPROVED", "DISAPPROVED"],
        default=["APPROVED", "DISAPPROVED"]
    )
    
    # Effective status
    eff_statuses = df["effective_status"].dropna().unique().tolist()
    eff_filter = st.multiselect(
        "⚡ Effective Status",
        options=sorted(eff_statuses),
        default=sorted(eff_statuses)
    )
    
    # Error type filter
    error_types_list = df[df["error_type"].notna()]["error_type"].unique().tolist()
    error_filter = st.multiselect(
        "🚫 Error Type",
        options=sorted(error_types_list),
        default=[]
    )
    
    # Ad account filter
    st.markdown("---")
    account_search = st.text_input("🔍 Search Ad Account ID", placeholder="e.g. act_1234...")
    
    st.markdown("---")
    st.markdown(
        f"<div style='text-align:center; opacity:0.5; font-size:0.8rem;'>"
        f"Data as of {today.strftime('%B %d, %Y')}<br>"
        f"{len(df):,} total records</div>",
        unsafe_allow_html=True
    )

# ─── Apply Filters ───
filtered = df.copy()
if len(date_range) == 2:
    dr_lo, dr_hi = date_range[0], date_range[1]
elif len(date_range) == 1:
    dr_lo = dr_hi = date_range[0]
else:
    dr_lo, dr_hi = data_start, data_end
filtered = filtered[
    (filtered["created_at"].dt.date >= dr_lo)
    & (filtered["created_at"].dt.date <= dr_hi)
]
if status_filter:
    filtered = filtered[filtered["ad_status"].isin(status_filter)]
if eff_filter:
    filtered = filtered[filtered["effective_status"].isin(eff_filter)]
if error_filter:
    filtered = filtered[filtered["error_type"].isin(error_filter)]
if account_search:
    filtered = filtered[filtered["ad_account_id"].str.contains(account_search, case=False, na=False)]


# ─── Header ───
st.markdown(
    '<div class="main-header">'
    '<h1>📊 Ad Rejection Dashboard</h1>'
    '<p>Real-time monitoring of ad approvals, disapprovals, and rejection patterns</p>'
    '</div>',
    unsafe_allow_html=True
)

# ─── KPI Cards — Total Ads ───
st.markdown('<div class="section-header">📈 Total Ads</div>', unsafe_allow_html=True)

ads_today = len(df[df["created_at"] == today])
ads_yesterday = len(df[df["created_at"] == yesterday])
ads_7d = len(df[df["created_at"] >= today - timedelta(days=6)])
ads_30d = len(df[df["created_at"] >= today - timedelta(days=29)])
ads_current_month = len(df[(df["created_at"].dt.year == today.year) & (df["created_at"].dt.month == today.month)])
ads_prev_month = len(df[
    (df["created_at"].dt.year == (today - timedelta(days=today.day)).year) &
    (df["created_at"].dt.month == (today - timedelta(days=today.day)).month)
])
ads_current_year = len(df[df["created_at"].dt.year == today.year])

day_before_yesterday = today - timedelta(days=2)
ads_day_before = len(df[df["created_at"] == day_before_yesterday])
prev_7d = len(df[(df["created_at"] >= today - timedelta(days=13)) & (df["created_at"] < today - timedelta(days=6))])

a1, a2, a3 = st.columns(3)
with a1:
    st.metric("Ads created · Today", f"{ads_today:,}", delta=f"{ads_today - ads_yesterday:+,} vs yday")
with a2:
    st.metric("Ads created · Yesterday", f"{ads_yesterday:,}", delta=f"{ads_yesterday - ads_day_before:+,} vs prior")
with a3:
    st.metric("Ads created · Last 7 days", f"{ads_7d:,}", delta=f"{ads_7d - prev_7d:+,} vs prev 7d")
a4, a5, a6 = st.columns(3)
with a4:
    st.metric("Ads created · Last 30 days", f"{ads_30d:,}")
with a5:
    st.metric("Ads created · This month", f"{ads_current_month:,}")
with a6:
    st.metric("Ads created · This year", f"{ads_current_year:,}")

# ─── KPI Cards — Disapproved Ads ───
st.markdown('<div class="section-header">🚫 Disapproved Ads</div>', unsafe_allow_html=True)

dis = df[df["ad_status"] == "DISAPPROVED"]
dis_today = len(dis[dis["status_change_date"] == today])
dis_yesterday = len(dis[dis["status_change_date"] == yesterday])
dis_7d = len(dis[dis["status_change_date"] >= today - timedelta(days=6)])
dis_30d = len(dis[dis["status_change_date"] >= today - timedelta(days=29)])
dis_current_month = len(dis[
    (dis["status_change_date"].dt.year == today.year) &
    (dis["status_change_date"].dt.month == today.month)
])
dis_prev_month = len(dis[
    (dis["status_change_date"].dt.year == (today - timedelta(days=today.day)).year) &
    (dis["status_change_date"].dt.month == (today - timedelta(days=today.day)).month)
])
dis_current_year = len(dis[dis["status_change_date"].dt.year == today.year])

dis_day_before = len(dis[dis["status_change_date"] == day_before_yesterday])
dis_prev_7d = len(dis[(dis["status_change_date"] >= today - timedelta(days=13)) & (dis["status_change_date"] < today - timedelta(days=6))])

d1, d2, d3 = st.columns(3)
with d1:
    st.metric("Disapproved · Today", f"{dis_today:,}", delta=f"{dis_today - dis_yesterday:+,} vs yday", delta_color="inverse")
with d2:
    st.metric("Disapproved · Yesterday", f"{dis_yesterday:,}", delta=f"{dis_yesterday - dis_day_before:+,} vs prior", delta_color="inverse")
with d3:
    st.metric("Disapproved · Last 7 days", f"{dis_7d:,}", delta=f"{dis_7d - dis_prev_7d:+,} vs prev 7d", delta_color="inverse")
d4, d5, d6 = st.columns(3)
with d4:
    st.metric("Disapproved · Last 30 days", f"{dis_30d:,}")
with d5:
    st.metric("Disapproved · This month", f"{dis_current_month:,}", delta=f"{dis_current_month - dis_prev_month:+,} vs prev mo", delta_color="inverse")
with d6:
    st.metric("Disapproved · This year", f"{dis_current_year:,}")

# ─── Rates Row ───
st.markdown('<div class="section-header">📊 Rejection Rates</div>', unsafe_allow_html=True)
c1, c2, c3, c4 = st.columns(4)

rate_today = (dis_today / ads_today * 100) if ads_today > 0 else 0
rate_yesterday = (dis_yesterday / ads_yesterday * 100) if ads_yesterday > 0 else 0
rate_7d = (dis_7d / ads_7d * 100) if ads_7d > 0 else 0
rate_30d = (dis_30d / ads_30d * 100) if ads_30d > 0 else 0

c1.metric("Reject rate · Today", f"{rate_today:.1f}%", delta=f"{rate_today - rate_yesterday:+.1f} pp vs yday", delta_color="inverse")
c2.metric("Reject rate · Yesterday", f"{rate_yesterday:.1f}%")
c3.metric("Reject rate · Last 7 days", f"{rate_7d:.1f}%")
c4.metric("Reject rate · Last 30 days", f"{rate_30d:.1f}%")


# ─── Tabs for Visualizations ───
st.markdown("---")
tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "📈 Trends", "🏢 Ad Accounts", "🚫 Error Analysis", "📋 Raw Data", "🔍 Account Drill-Down"
])

# ═══ TAB 1: TRENDS ═══
with tab1:
    # Daily ads created vs disapproved
    daily = df.groupby("created_at").agg(
        total=("ad_id", "count"),
        disapproved=("ad_status", lambda x: (x == "DISAPPROVED").sum())
    ).reset_index()
    daily["approved"] = daily["total"] - daily["disapproved"]
    daily["rejection_rate"] = (daily["disapproved"] / daily["total"] * 100).round(2)
    
    # Show last 30 days by default
    daily_30 = daily[daily["created_at"] >= today - timedelta(days=30)]
    
    col1, col2 = st.columns(2)
    
    with col1:
        fig = go.Figure()
        fig.add_trace(go.Bar(
            x=daily_30["created_at"], y=daily_30["approved"],
            name="Approved", marker_color="#3b82f6",
            hovertemplate="%{x|%b %d}<br>Approved: %{y:,}<extra></extra>"
        ))
        fig.add_trace(go.Bar(
            x=daily_30["created_at"], y=daily_30["disapproved"],
            name="Disapproved", marker_color="#ef4444",
            hovertemplate="%{x|%b %d}<br>Disapproved: %{y:,}<extra></extra>"
        ))
        fig.update_layout(
            title="Daily Ads Created (Last 30 Days)",
            barmode="stack",
            template="plotly_white",
            height=400,
            font=dict(family="DM Sans"),
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
            margin=dict(l=40, r=20, t=60, b=40)
        )
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        fig2 = go.Figure()
        fig2.add_trace(go.Scatter(
            x=daily_30["created_at"], y=daily_30["rejection_rate"],
            mode="lines+markers",
            name="Rejection Rate",
            line=dict(color="#ef4444", width=2.5),
            marker=dict(size=5),
            fill="tozeroy",
            fillcolor="rgba(239,68,68,0.08)",
            hovertemplate="%{x|%b %d}<br>Rate: %{y:.1f}%<extra></extra>"
        ))
        fig2.update_layout(
            title="Daily Rejection Rate (Last 30 Days)",
            yaxis_title="Rejection Rate (%)",
            template="plotly_white",
            height=400,
            font=dict(family="DM Sans"),
            margin=dict(l=40, r=20, t=60, b=40)
        )
        st.plotly_chart(fig2, use_container_width=True)
    
    # Weekly trend
    weekly = df.copy()
    weekly["week"] = weekly["created_at"].dt.isocalendar().week.astype(int)
    weekly["year_week"] = weekly["created_at"].dt.strftime("%Y-W%U")
    weekly_agg = weekly.groupby("year_week").agg(
        total=("ad_id", "count"),
        disapproved=("ad_status", lambda x: (x == "DISAPPROVED").sum())
    ).reset_index()
    weekly_agg["rejection_rate"] = (weekly_agg["disapproved"] / weekly_agg["total"] * 100).round(2)
    weekly_agg = weekly_agg.tail(12)
    
    fig3 = go.Figure()
    fig3.add_trace(go.Bar(x=weekly_agg["year_week"], y=weekly_agg["total"], name="Total Ads", marker_color="#3b82f6"))
    fig3.add_trace(go.Bar(x=weekly_agg["year_week"], y=weekly_agg["disapproved"], name="Disapproved", marker_color="#ef4444"))
    fig3.add_trace(go.Scatter(
        x=weekly_agg["year_week"], y=weekly_agg["rejection_rate"],
        name="Rejection Rate %", yaxis="y2",
        line=dict(color="#f59e0b", width=3), mode="lines+markers"
    ))
    fig3.update_layout(
        title="Weekly Trend (Last 12 Weeks)",
        barmode="group",
        template="plotly_white",
        height=400,
        font=dict(family="DM Sans"),
        yaxis2=dict(title="Rejection Rate %", overlaying="y", side="right", showgrid=False),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        margin=dict(l=40, r=60, t=60, b=40)
    )
    st.plotly_chart(fig3, use_container_width=True)


# ═══ TAB 2: AD ACCOUNTS ═══
with tab2:
    # Date x Ad Status pivot
    st.markdown("**Date × Ad Status**")
    date_status = df.groupby([df["created_at"].dt.date, "ad_status"]).size().unstack(fill_value=0).reset_index()
    date_status.columns = ["Date", "APPROVED", "DISAPPROVED"] if "DISAPPROVED" in date_status.columns else list(date_status.columns)
    date_status = date_status.sort_values("Date", ascending=False).head(15)
    st.dataframe(date_status, use_container_width=True, hide_index=True)
    
    st.markdown("---")
    
    # Top accounts by disapproval
    st.markdown("**Ad Account × Ad Status**")
    acct_status = df.groupby(["ad_account_id", "ad_status"]).size().unstack(fill_value=0).reset_index()
    if "DISAPPROVED" in acct_status.columns:
        acct_status = acct_status.sort_values("DISAPPROVED", ascending=False).head(20)
    st.dataframe(acct_status, use_container_width=True, hide_index=True)
    
    st.markdown("---")
    
    col1, col2 = st.columns(2)
    with col1:
        # Top 10 accounts by disapproval count
        top_dis = dis.groupby("ad_account_id").size().reset_index(name="count").sort_values("count", ascending=True).tail(10)
        fig = px.bar(
            top_dis, x="count", y="ad_account_id", orientation="h",
            title="Top 10 Accounts by Disapproved Ads",
            color_discrete_sequence=["#ef4444"],
            labels={"count": "Disapproved Ads", "ad_account_id": "Ad Account"}
        )
        fig.update_layout(template="plotly_white", height=400, font=dict(family="DM Sans"),
                          margin=dict(l=40, r=20, t=60, b=40))
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        # Accounts by rejection rate (min 10 ads)
        acct_rates = df.groupby("ad_account_id").agg(
            total=("ad_id", "count"),
            disapproved=("ad_status", lambda x: (x == "DISAPPROVED").sum())
        ).reset_index()
        acct_rates["rejection_rate"] = (acct_rates["disapproved"] / acct_rates["total"] * 100).round(2)
        acct_rates = acct_rates[acct_rates["total"] >= 10].sort_values("rejection_rate", ascending=True).tail(10)
        
        fig = px.bar(
            acct_rates, x="rejection_rate", y="ad_account_id", orientation="h",
            title="Top 10 Accounts by Rejection Rate (min 10 ads)",
            color_discrete_sequence=["#f59e0b"],
            labels={"rejection_rate": "Rejection Rate (%)", "ad_account_id": "Ad Account"}
        )
        fig.update_layout(template="plotly_white", height=400, font=dict(family="DM Sans"),
                          margin=dict(l=40, r=20, t=60, b=40))
        st.plotly_chart(fig, use_container_width=True)
    
    # Created Date × Ad Account × Ad Status
    st.markdown("---")
    st.markdown("**Created Date × Ad Account × Ad Status (Filtered)**")
    date_acct = filtered.groupby([filtered["created_at"].dt.date, "ad_account_id", "ad_status"]).size().reset_index(name="Count")
    date_acct.columns = ["created_at", "ad_account_id", "ad_status", "Count"]
    date_acct = date_acct.sort_values(["created_at", "ad_account_id"], ascending=[False, True])
    st.dataframe(date_acct.head(500), use_container_width=True, hide_index=True)


# ═══ TAB 3: ERROR ANALYSIS ═══
with tab3:
    col1, col2 = st.columns(2)
    
    with col1:
        # Error type distribution
        error_dist = dis["error_type"].value_counts().reset_index()
        error_dist.columns = ["Error Type", "Count"]
        fig = px.pie(
            error_dist, values="Count", names="Error Type",
            title="Disapproval Reasons Distribution",
            color_discrete_sequence=px.colors.qualitative.Set2,
            hole=0.4
        )
        fig.update_layout(
            template="plotly_white", height=450, font=dict(family="DM Sans"),
            margin=dict(l=20, r=20, t=60, b=20),
            legend=dict(font=dict(size=10))
        )
        fig.update_traces(textposition="inside", textinfo="percent+label", textfont_size=10)
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        # Error type bar chart
        fig = px.bar(
            error_dist.sort_values("Count", ascending=True),
            x="Count", y="Error Type", orientation="h",
            title="Disapproval Count by Error Type",
            color="Count",
            color_continuous_scale=["#fca5a5", "#ef4444", "#991b1b"]
        )
        fig.update_layout(
            template="plotly_white", height=450, font=dict(family="DM Sans"),
            margin=dict(l=40, r=20, t=60, b=40), showlegend=False,
            coloraxis_showscale=False
        )
        st.plotly_chart(fig, use_container_width=True)
    
    # Error type trend over time
    st.markdown("---")
    error_daily = dis.groupby([dis["status_change_date"].dt.date, "error_type"]).size().reset_index(name="count")
    error_daily.columns = ["date", "error_type", "count"]
    error_daily = error_daily[error_daily["date"] >= (today - timedelta(days=30)).date()]
    
    fig = px.area(
        error_daily, x="date", y="count", color="error_type",
        title="Daily Disapprovals by Error Type (Last 30 Days)",
        color_discrete_sequence=px.colors.qualitative.Set2,
        labels={"count": "Disapproved Ads", "date": "Date", "error_type": "Error Type"}
    )
    fig.update_layout(
        template="plotly_white", height=400, font=dict(family="DM Sans"),
        legend=dict(orientation="h", yanchor="bottom", y=-0.4, font=dict(size=9)),
        margin=dict(l=40, r=20, t=60, b=40)
    )
    st.plotly_chart(fig, use_container_width=True)
    
    # Error type by ad account (heatmap)
    col1, col2 = st.columns(2)
    with col1:
        st.markdown("**Ad Account × Disapproved Reason × Count**")
        acct_error = dis.groupby(["ad_account_id", "error_type"]).size().reset_index(name="Count")
        acct_error = acct_error.sort_values("Count", ascending=False).head(50)
        st.dataframe(acct_error, use_container_width=True, hide_index=True)
    
    with col2:
        st.markdown("**Date × Disapproved Reason × Count**")
        date_error = dis.groupby([dis["status_change_date"].dt.date, "error_type"]).size().reset_index(name="Count")
        date_error.columns = ["Date", "Error Type", "Count"]
        date_error = date_error.sort_values(["Date", "Count"], ascending=[False, False]).head(50)
        st.dataframe(date_error, use_container_width=True, hide_index=True)
    
    # Time to disapproval
    st.markdown("---")
    st.markdown("**⏱️ Time to Disapproval (Days from Creation to Status Change)**")
    dis_copy = dis.copy()
    dis_copy["days_to_disapproval"] = (dis_copy["status_change_date"] - dis_copy["created_at"]).dt.days
    
    fig = px.histogram(
        dis_copy, x="days_to_disapproval", nbins=15,
        title="Distribution of Days Between Ad Creation and Disapproval",
        color_discrete_sequence=["#3b82f6"],
        labels={"days_to_disapproval": "Days to Disapproval"}
    )
    fig.update_layout(
        template="plotly_white", height=350, font=dict(family="DM Sans"),
        margin=dict(l=40, r=20, t=60, b=40)
    )
    st.plotly_chart(fig, use_container_width=True)


# ═══ TAB 4: RAW DATA ═══
with tab4:
    st.markdown(f"**Showing {len(filtered):,} records** (filtered)")
    
    col1, col2, col3 = st.columns(3)
    with col1:
        show_only_disapproved = st.checkbox("Show only disapproved", value=False)
    with col2:
        show_yesterday_accounts = st.checkbox("Yesterday's disapproved accounts (all records)", value=False)
    
    display_df = filtered.copy()
    
    if show_only_disapproved:
        display_df = display_df[display_df["ad_status"] == "DISAPPROVED"]
    
    if show_yesterday_accounts:
        # Find accounts with disapprovals yesterday
        yesterday_dis_accounts = df[
            (df["ad_status"] == "DISAPPROVED") &
            (df["status_change_date"] == yesterday)
        ]["ad_account_id"].unique()
        display_df = df[df["ad_account_id"].isin(yesterday_dis_accounts)]
        st.info(f"Showing all {len(display_df):,} records for {len(yesterday_dis_accounts)} accounts that had disapprovals yesterday")
    
    display_df = display_df.sort_values(["created_at", "ad_account_id"], ascending=[False, True])
    
    st.dataframe(
        display_df.head(2000),
        use_container_width=True,
        hide_index=True,
        column_config={
            "created_at": st.column_config.DateColumn("Created At", format="MMM DD, YYYY"),
            "status_change_date": st.column_config.DateColumn("Status Change Date", format="MMM DD, YYYY"),
            "ad_status": st.column_config.TextColumn("Ad Status"),
        }
    )
    
    # Download button
    csv = display_df.to_csv(index=False)
    st.download_button(
        "⬇️ Download as CSV",
        csv,
        "ad_rejection_data.csv",
        "text/csv",
        key="download-csv"
    )


# ═══ TAB 5: ACCOUNT DRILL-DOWN ═══
with tab5:
    st.markdown("**Select an ad account to see all its records and rejection history**")
    
    # Show accounts with most disapprovals first
    acct_options = dis.groupby("ad_account_id").size().reset_index(name="dis_count").sort_values("dis_count", ascending=False)
    acct_list = acct_options["ad_account_id"].tolist()
    
    selected_account = st.selectbox("Ad Account", acct_list, index=0)
    
    if selected_account:
        acct_data = df[df["ad_account_id"] == selected_account]
        acct_dis = acct_data[acct_data["ad_status"] == "DISAPPROVED"]
        
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Total Ads", f"{len(acct_data):,}")
        c2.metric("Disapproved", f"{len(acct_dis):,}")
        c3.metric("Rejection Rate", f"{len(acct_dis)/len(acct_data)*100:.1f}%")
        c4.metric("BUID", acct_data["buid"].iloc[0])
        
        col1, col2 = st.columns(2)
        with col1:
            # Daily trend for this account
            acct_daily = acct_data.groupby([acct_data["created_at"].dt.date, "ad_status"]).size().unstack(fill_value=0).reset_index()
            acct_daily.columns.name = None
            fig = go.Figure()
            if "APPROVED" in acct_daily.columns:
                fig.add_trace(go.Bar(x=acct_daily["created_at"], y=acct_daily["APPROVED"], name="Approved", marker_color="#3b82f6"))
            if "DISAPPROVED" in acct_daily.columns:
                fig.add_trace(go.Bar(x=acct_daily["created_at"], y=acct_daily["DISAPPROVED"], name="Disapproved", marker_color="#ef4444"))
            fig.update_layout(
                title=f"Ad History: {selected_account[:25]}...",
                barmode="stack", template="plotly_white", height=350,
                font=dict(family="DM Sans"),
                margin=dict(l=40, r=20, t=60, b=40)
            )
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            if len(acct_dis) > 0:
                error_breakdown = acct_dis["error_type"].value_counts().reset_index()
                error_breakdown.columns = ["Error Type", "Count"]
                fig = px.pie(error_breakdown, values="Count", names="Error Type",
                             title="Rejection Reasons for this Account",
                             color_discrete_sequence=px.colors.qualitative.Pastel, hole=0.35)
                fig.update_layout(template="plotly_white", height=350, font=dict(family="DM Sans"),
                                  margin=dict(l=20, r=20, t=60, b=20))
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No disapprovals for this account")
        
        st.markdown("**All Records**")
        st.dataframe(
            acct_data.sort_values("created_at", ascending=False),
            use_container_width=True,
            hide_index=True,
            column_config={
                "created_at": st.column_config.DateColumn("Created At", format="MMM DD, YYYY"),
                "status_change_date": st.column_config.DateColumn("Status Change Date", format="MMM DD, YYYY"),
            }
        )