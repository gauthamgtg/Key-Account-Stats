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

# Read credentials directly from Streamlit secrets
db = st.secrets["db"]
name = st.secrets["name"]
passw = st.secrets["passw"]
server = st.secrets["server"]
port = st.secrets["port"]
stripe_key = st.secrets["stripe"]



st.set_page_config( page_title = "Spend Stats",
    page_icon=":bar_chart:",
    layout="wide"
    # initial_sidebar_state="expanded"
    )

# st.toast('Successfully connected to the database!!', icon='😍')

# st.write("Successfully connected to the database!")

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

@redshift_connection(db,name,passw,server,port)
def execute_query(connection, cursor,query):

    cursor.execute(query)
    column_names = [desc[0] for desc in cursor.description]
    result = pd.DataFrame(cursor.fetchall(), columns=column_names)

    return result


query = '''
with spends AS
    (SELECT  ad_account_id,date(date_start) as dt,max(spend)spend
    from 
    (
    select ad_account_id,date(date_start) as date_start,spend from ad_account_spends 
    union ALL
    select ad_account_id,date(date_start) as date_start,spend from zocket_global.ad_account_spends
    )aas
	group by 1,2
    ),
    total_payment AS
        (select ad_account,sum(adspend_amount) total_paid 
        from payment_trans_details td
        group by 1)

select coalesce(cast(bp.buid as int),c.app_business_id) as euid,coalesce(eu.business_name,bp.name) as business_name,coalesce(eu.company_name,bp.brand_name) as company_name,dt,coalesce(b.name,d.name) as ad_account_name,
coalesce(c.name,e.name) as business_manager_name,coalesce(c.business_manager_id,e.business_manager_id) as business_manager_id,
case when a.ad_account_id ='act_1090921776002942' then 'INR' else COALESCE(b.currency,d.currency) end as currency_code,a.ad_account_id,spend
from 
    spends a
    left join ( select ad_account_id,name,currency,max(app_business_manager_id)app_business_manager_id
    from fb_ad_accounts 
    group by 1,2,3 )b on a.ad_account_id = b.ad_account_id
    left join fb_business_managers c on c.id = b.app_business_manager_id
    left join enterprise_users eu on eu.euid=c.app_business_id
    left join zocket_global.fb_child_ad_accounts d on a.ad_account_id = d.ad_account_id
    left join zocket_global.fb_child_business_managers e on e.id = d.app_business_manager_id
    left join 
    (SELECT
    id ,name,brand_name,json_extract_path_text(json_extract_array_element_text(business_user_ids, 0), 'role') AS role,
    json_extract_path_text(json_extract_array_element_text(business_user_ids, 0), 'business_user_id') AS buid
FROM
    zocket_global.business_profile
WHERE
    json_extract_path_text(json_extract_array_element_text(business_user_ids, 0), 'role') = 'owner' )bp on e.app_business_id=bp.id
    order by euid,dt desc
    '''

df = execute_query(query=query)


#chaning proper format of date
df['dt'] = pd.to_datetime(df['dt']).dt.date
df['spend'] = pd.to_numeric(df['spend'], errors='coerce')
df['spend'] = df['spend'].map(int)
df['euid'] = pd.to_numeric(df['euid'], errors='coerce').astype('Int64')
# df =  df.fillna("Unknown")
df = df.drop_duplicates(subset=['dt', 'ad_account_id'], keep='first')


grouped_data_adacclevel = None
pivoted_data_adacclevel = None

# Calculate yesterday and day before yesterday's dates
yesterday = (date.today() - timedelta(days=1))
day_before_yst = (date.today() - timedelta(days=2))
last_month = date.today().replace(day=1) - timedelta(days=1)
current_month = datetime.now().month
current_year = datetime.now().year

#Removing today's data
df = df[df['dt'] != date.today()]


st.title("Meta Spend Summary")


non_usd_currencies = df['currency_code'].unique()
non_usd_currencies = [currency for currency in non_usd_currencies if currency != 'USD']

    # Create a dictionary to store the conversion rates entered by the user
conversion_rates = {}

# Predefine default values for specific currencies
default_values = {
                    'EUR': 1.051215,
                    'GBP': 1.27003,
                    'AUD': 0.64307,
                    'INR': 0.012,
                    'THB': 0.029,
                    'KRW': 0.00072,
                    'CAD' : 0.72,
                    'BRL' :0.18,
                    'TRY':0.029,
                    'VND':0.000040,
                    'AED':0.27,
                    'RON': 0.22,
                    'ZAR':0.057,
                    'NOK':0.092,
                    'SAR':0.27,
                    'MXN':0.050
                }

# # Display input boxes for each unique currency code other than 'USD'
# st.write("Enter conversion rates for the following currencies:")

# # Create columns dynamically based on the number of currencies
# cols = st.columns(4)  # Adjust the number of columns (3 in this case)

# # Iterate over non-USD currencies and display them in columns
# for idx, currency in enumerate(non_usd_currencies):
#     default_value = default_values.get(currency, 1.0)  # Use default value if defined, otherwise 1.0
#     with cols[idx % 4]:  # Rotate through the columns
#         conversion_rates[currency] = st.number_input(
#             f"{currency} to USD:", value=default_value, min_value=0.0, step=0.001, format="%.3f"
#         )

def convert_to_usd(row):
    if row['currency_code'] == 'USD':
        return row['spend']
    elif row['currency_code'] in conversion_rates:
        return row['spend'] * conversion_rates[row['currency_code']]
    return row['spend']

# def convert_to_inr(row):
#     if row['currency'] == 'INR':
#         return row['spend']
#     elif row['currency'] in conversion_rates:
#         return row['spend_in_usd'] * conversion_rates[row['currency']]
#     return row['spend']

# Create the 'spend_in_usd' column
df['spend_in_usd'] = df.apply(lambda row: convert_to_usd(row), axis=1)
df['spend_in_usd'] = df['spend_in_usd'].map(int)
df['ad_account_name'] = df['ad_account_name'].str.replace('zocket manager Ad Account', '', regex=True)
df['ad_account_name'] = df['ad_account_name'].str.replace('Zocket manager ad account', '', regex=True)

ind_df = df[df['currency_code'] == 'INR']
us_df = df[df['currency_code'] != 'INR']
yesterday = (date.today() - timedelta(days=1))
print(yesterday)

ind_yesterday = ind_df[ind_df['dt'] == yesterday]['spend'].sum()
us_yesterday = us_df[us_df['dt'] == yesterday]['spend_in_usd'].sum()

ind_current_month = ind_df[pd.to_datetime(ind_df['dt']).dt.to_period('M') == pd.to_datetime('today').to_period('M')]['spend'].sum()
us_current_month = us_df[pd.to_datetime(us_df['dt']).dt.to_period('M') == pd.to_datetime('today').to_period('M')]['spend_in_usd'].sum()

ind_avg_spend = int(ind_current_month / (yesterday.day))
us_avg_spend = int(us_current_month / (yesterday.day))

ind_last_month_spend = ind_df[pd.to_datetime(ind_df['dt']).dt.to_period('M') == (pd.to_datetime('today') - pd.DateOffset(months=1)).to_period('M')]['spend'].sum() 
us_last_month_spend = us_df[pd.to_datetime(us_df['dt']).dt.to_period('M') == (pd.to_datetime('today') - pd.DateOffset(months=1)).to_period('M')]['spend_in_usd'].sum()

ind_yesterday_increase = (ind_yesterday - ind_df[ind_df['dt'] == (yesterday - timedelta(days=1))]['spend'].sum()) / ind_df[ind_df['dt'] == (yesterday - timedelta(days=1))]['spend'].sum() * 100
us_yesterday_increase = (us_yesterday - us_df[us_df['dt'] == (yesterday - timedelta(days=1))]['spend_in_usd'].sum()) / us_df[us_df['dt'] == (yesterday - timedelta(days=1))]['spend_in_usd'].sum() * 100

col1, col2, col3,col4 = st.columns(4)

col1.metric("IND BM Yesterday", f"₹{ind_yesterday:}", f"{ind_yesterday_increase:.2f}%")
col1.metric("US BM Yesterday", f"${us_yesterday:}", f"{us_yesterday_increase:.2f}%")
col2.metric("IND BM This Month", f"₹{ind_current_month:}")
col2.metric("US BM This Month", f"${us_current_month:}")
col3.metric("IND BM Current Month Avg Spend", f"₹{ind_avg_spend:}")
col3.metric("US BM Current Month Avg Spend", f"${us_avg_spend:}")
col4.metric("IND BM Last Month", f"₹{ind_last_month_spend:}")
col4.metric("US BM Last Month", f"${us_last_month_spend:}")
# col4.metric("IND BM MoM Increase", f"{ind_mom_increase:.2f}%")
# col4.metric("US BM MoM Increase", f"{us_mom_increase:.2f}%")

# st.line_chart(df.groupby('dt').sum()['spend_in_usd'])

# st.line_chart(ind_df.groupby('dt').sum()['spend'])
# st.line_chart(us_df.groupby('dt').sum()['spend_in_usd'])

st.subheader("Top 10 Spending Accounts for Yesterday")

col_ind, col_us = st.columns(2)
# Top 10 IND BM accounts for yesterday
top10_ind = ind_df[ind_df['dt'] == yesterday].sort_values('spend', ascending=False).head(10)
col_ind.write("IND BM")
col_ind.dataframe(
    top10_ind[['euid','ad_account_id', 'ad_account_name', 'spend']].rename(columns={'spend': 'Spend (INR)'}),
    use_container_width=True,
    hide_index=True
)

# Top 10 US BM accounts for yesterday
top10_us = us_df[us_df['dt'] == yesterday].sort_values('spend_in_usd', ascending=False).head(10)
col_us.write("US BM")
col_us.dataframe(
    top10_us[['euid','ad_account_id', 'ad_account_name','spend_in_usd']].rename(columns={'spend_in_usd': 'Spend (USD)'}),
    use_container_width=True,
    hide_index=True
)
