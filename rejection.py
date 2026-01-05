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
import numpy
import requests
from urllib.parse import urlparse

# Read credentials directly from Streamlit secrets
db = st.secrets["db"]
name = st.secrets["name"]
passw = st.secrets["passw"]
server = st.secrets["server"]
port = st.secrets["port"]
stripe_key = st.secrets["stripe"]


st.set_page_config( page_title = "Spend Stats",
    page_icon=":bar_chart:",
    layout="wide",
    initial_sidebar_state="expanded")

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

# Check if df is None or empty
if df is None or len(df) == 0:
    st.write("No data available.")
    st.stop()

# Convert status_change_date to date if it's datetime
if 'status_change_date' in df.columns:
    df['status_change_date'] = pd.to_datetime(df['status_change_date']).dt.date
    df['created_at'] = pd.to_datetime(df['created_at']).dt.date

df = df[(df['status_change_date'] != date.today()) & (df['created_at'] != date.today())]

# Get yesterday's date
yesterday = (pd.Timestamp.now() - pd.Timedelta(days=1)).date()

# Filter for ads rejected yesterday
yesterday_rejected = df[
    (df['status_change_date'] == yesterday) & 
    (df['ad_status'] == 'DISAPPROVED')
]

# Get unique ad account IDs that got rejected yesterday
ad_accounts_df = yesterday_rejected[['ad_account_id']].drop_duplicates()

# Check if there are any accounts that got rejected yesterday
if len(ad_accounts_df) == 0:
    st.write("No rejected ads found for yesterday.")
    st.stop()

# Filter entire DataFrame using these ad accounts
ystd_rejection_df = df[df['ad_account_id'].isin(ad_accounts_df['ad_account_id'])]

# Calculate date ranges
today = pd.Timestamp.now().date()
last_7_days_start = today - pd.Timedelta(days=7)
last_30_days_start = today - pd.Timedelta(days=30)

# Function to calculate rejection stats for a date range
def calculate_rejection_stats(df_filtered):
    total_published = len(df_filtered)
    total_rejected = len(df_filtered[df_filtered['effective_status'] == 'DISAPPROVED'])
    rejection_pct = (total_rejected / total_published * 100) if total_published > 0 else 0
    return total_published, total_rejected, rejection_pct

# Calculate stats for each ad account
stats_list = []

for ad_account_id in ad_accounts_df['ad_account_id'].unique():
    account_data = ystd_rejection_df[ystd_rejection_df['ad_account_id'] == ad_account_id]
    buid = account_data['buid'].iloc[0] if len(account_data) > 0 else None
    
    # Last 7 days stats
    last_7_days_data = account_data[account_data['created_at'] >= last_7_days_start]
    last_7_published, last_7_rejected, last_7_pct = calculate_rejection_stats(last_7_days_data)
    
    # Last 30 days stats
    last_30_days_data = account_data[account_data['created_at'] >= last_30_days_start]
    last_30_published, last_30_rejected, last_30_pct = calculate_rejection_stats(last_30_days_data)
    
    # Overall stats
    overall_published, overall_rejected, overall_pct = calculate_rejection_stats(account_data)
    
    stats_list.append({
        'BUID': buid,
        'Accounts': ad_account_id,
        'Last 7 days Ads Published': last_7_published,
        'Last 7 days Ads Rejected': last_7_rejected,
        'Last 7 days Ads Rejection %': round(last_7_pct, 0),
        'Last 30 days Ads Published': last_30_published,
        'Last 30 days Ads rejected': last_30_rejected,
        'Last 30 days Ads Rejection %': round(last_30_pct, 0),
        'Total Ads Published': overall_published,
        'Total rejected ads': overall_rejected,
        'Ads Rejection %': round(overall_pct, 0)
    })

# Create stats DataFrame
rejection_df = pd.DataFrame(stats_list)

# Add total row
if len(rejection_df) > 0:
    total_row = {
        'BUID': 'Total',
        'Accounts': 'Total',
        'Last 7 days Ads Published': rejection_df['Last 7 days Ads Published'].sum(),
        'Last 7 days Ads Rejected': rejection_df['Last 7 days Ads Rejected'].sum(),
        'Last 7 days Ads Rejection %': round((rejection_df['Last 7 days Ads Rejected'].sum() / rejection_df['Last 7 days Ads Published'].sum() * 100) if rejection_df['Last 7 days Ads Published'].sum() > 0 else 0, 0),
        'Last 30 days Ads Published': rejection_df['Last 30 days Ads Published'].sum(),
        'Last 30 days Ads rejected': rejection_df['Last 30 days Ads rejected'].sum(),
        'Last 30 days Ads Rejection %': round((rejection_df['Last 30 days Ads rejected'].sum() / rejection_df['Last 30 days Ads Published'].sum() * 100) if rejection_df['Last 30 days Ads Published'].sum() > 0 else 0, 0),
        'Total Ads Published': rejection_df['Total Ads Published'].sum(),
        'Total rejected ads': rejection_df['Total rejected ads'].sum(),
        'Ads Rejection %': round((rejection_df['Total rejected ads'].sum() / rejection_df['Total Ads Published'].sum() * 100) if rejection_df['Total Ads Published'].sum() > 0 else 0, 0)
    }
    rejection_df = pd.concat([rejection_df, pd.DataFrame([total_row])], ignore_index=True)

st.write(today)
st.write(last_7_days_start)
st.write(last_30_days_start)
# Display the dataframe
if len(rejection_df) > 0:
    st.dataframe(rejection_df, use_container_width=True)
else:
    st.write("No rejected ads found for yesterday.")

