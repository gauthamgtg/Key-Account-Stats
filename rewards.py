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
import boto3
import json
import stripe
import numpy
import requests
from urllib.parse import urlparse

client = boto3.client(
    "secretsmanager",
    region_name=st.secrets["AWS_DEFAULT_REGION"],
    aws_access_key_id=st.secrets["AWS_ACCESS_KEY_ID"],
    aws_secret_access_key=st.secrets["AWS_SECRET_ACCESS_KEY"]
)

def get_secret(secret_name):
    # Retrieve the secret value
    response = client.get_secret_value(SecretId=secret_name)
    return json.loads(response["SecretString"])

# Replace 'your-secret-name' with the actual secret name in AWS Secrets Manager
secret = get_secret("G-streamlit-KAT")
db = secret["db"]
name = secret["name"]
passw = secret["passw"]
server = secret["server"]
port = secret["port"]
stripe_key = secret["stripe"]


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


list_query = '''
SELECT distinct b.app_business_id as euid, a.ad_account_id, a.name as ad_account_name, b.name as business_manager_name,b.business_manager_id,eu.business_name,eu.company_name,a.currency,a.created_at as ad_account_created_at
FROM fb_ad_accounts a
	LEFT JOIN fb_business_managers b ON b.id = a.app_business_manager_id
   left join enterprise_users eu on b.app_business_id=eu.euid
union all
SELECT cast(bp.buid as int) as euid, a.ad_account_id, a.name as ad_account_name, b.name as business_manager_name,b.business_manager_id,bp.name,bp.brand_name,a.currency,a.created_at as ad_account_created_at
FROM zocket_global.fb_child_ad_accounts a
	LEFT JOIN zocket_global.fb_child_business_managers b ON b.id = a.app_business_manager_id
   left join 
    (SELECT
    id ,name,brand_name,json_extract_path_text(json_extract_array_element_text(business_user_ids, 0), 'role') AS role,
    json_extract_path_text(json_extract_array_element_text(business_user_ids, 0), 'business_user_id') AS buid
FROM
    zocket_global.business_profile
WHERE
    json_extract_path_text(json_extract_array_element_text(business_user_ids, 0), 'role') = 'owner' )bp on b.app_business_id=bp.id
 '''


#disabled account query
disabled_account_query='''
SELECT euid,ad_account_id,
case when flag = 'Reactivated' then reactivation_date
when flag = 'Disabled' then dt end as disable_date,
case when flag = 'Reactivated' then dt end as reactivation_date
,flag
,currency
,name as ad_account_name,bm_name,
case when disable_reason = 0 then 'NONE'
when disable_reason = 1 then  'ADS_INTEGRITY_POLICY'
when disable_reason = 2 then  'ADS_IP_REVIEW'
when disable_reason = 3 then  'RISK_PAYMENT'
when disable_reason = 4 then  'GRAY_ACCOUNT_SHUT_DOWN'
when disable_reason = 5 then  'ADS_AFC_REVIEW'
when disable_reason = 6 then  'BUSINESS_INTEGRITY_RAR'
when disable_reason = 7 then  'PERMANENT_CLOSE'
when disable_reason = 8 then  'UNUSED_RESELLER_ACCOUNT'
when disable_reason = 9 then  'UNUSED_ACCOUNT'
when disable_reason = 10 then  'UMBRELLA_AD_ACCOUNT'
when disable_reason = 11 then  'BUSINESS_MANAGER_INTEGRITY_POLICY'
when disable_reason = 12 then  'MISREPRESENTED_AD_ACCOUNT'
when disable_reason = 13 then  'AOAB_DESHARE_LEGAL_ENTITY'
when disable_reason = 14 then  'CTX_THREAD_REVIEW'
when disable_reason = 15 then  'COMPROMISED_AD_ACCOUNT' end as disable_reason
FROM
(
SELECT *,case when rw = 1 and prev_status !=1 and account_status = 1 then 'Reactivated' 
            when rw = 1 and account_status != 1 then 'Disabled' else 'Others'
            end as flag, case when rw = 1 and prev_status !=1 and account_status = 1 then prev_dt end as reactivation_date

FROM
(
select coalesce(eu.euid,cast(bp.buid as int)) as euid,COALESCE(b.name,d.name)as name,a.ad_account_id,a.account_status,disable_reason,dateadd('minute',330,a.created_at) as dt,
COALESCE(b.currency,d.currency)as currency,
COALESCE(c.name,e.name)as bm_name,
 row_number() over(partition by a.ad_account_id order by a.created_at desc) as rw,
 lag(a.account_status,1) over(PARTITION by a.ad_account_id order by dateadd('minute',330,a.created_at)) as prev_status,
 lag(dateadd('minute',330,a.created_at),1) over(PARTITION by a.ad_account_id order by dateadd('minute',330,a.created_at)) as prev_dt
-- from "dev"."public"."ad_account_webhook" a
from "dev"."z_b"."ad_account_webhook" a
left join fb_ad_accounts b on a.ad_account_id = b.ad_account_id
left join fb_business_managers c on c.id = b.app_business_manager_id
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
left join enterprise_users eu on c.app_business_id=eu.euid
order by 3
)
)
where flag !='Others'
'''

@redshift_connection(db,name,passw,server,port)
def execute_query(connection, cursor,query):

    cursor.execute(query)
    column_names = [desc[0] for desc in cursor.description]
    result = pd.DataFrame(cursor.fetchall(), columns=column_names)

    return result

# df = execute_query(query=query)
df = execute_query(query=query)
list_df = execute_query(query=list_query)
disabled_account_df = execute_query(query=disabled_account_query)

# Load the CSV file

url = "https://docs.google.com/spreadsheets/d/1JvJ5Pa5qFDvXq1KaR0YTiReUM39P0berAgtSEkvCnIs/export?format=csv"

account_list_df = pd.read_csv(url)

# Create a DataFrame for each column
datong_acc_list_df = account_list_df[['Datong']].dropna(inplace=False)
roposo_acc_list_df = account_list_df[['Roposo','Media_Buyer']].dropna(inplace=False)
shiprocket_acc_list_df = account_list_df[['Shiprocket']].dropna(inplace=False)

top_customers_flag = []
for index, row in df.iterrows():
    if row['ad_account_id'] in datong_acc_list_df.values:
        top_customers_flag.append('Datong')
    elif row['dt'] > datetime(2024, 9, 30).date() and row['ad_account_id'] in roposo_acc_list_df.values:
        top_customers_flag.append('Roposo')
    elif row['ad_account_id'] in shiprocket_acc_list_df.values:
        top_customers_flag.append('Shiprocket')
    else:
        top_customers_flag.append('Others')

df['top_customers_flag'] = top_customers_flag
df = df.merge(roposo_acc_list_df, how='left', left_on='ad_account_id', right_on='Roposo')
df['Media_Buyer'] = df['Media_Buyer'].fillna('Null')
df = df.drop(columns=['Roposo'])


#chaning proper format of date
df['dt'] = pd.to_datetime(df['dt']).dt.date
df['spend'] = pd.to_numeric(df['spend'], errors='coerce')
df['spend'] = df['spend'].map(int)
df['euid'] = pd.to_numeric(df['euid'], errors='coerce')
df =  df.fillna("Unknown")

#sort by spend
df = df.sort_values(by='spend', ascending=False)

#remove duplicate by subset of dt and ad_account_id
df = df.drop_duplicates(subset=['dt', 'ad_account_id'], keep='first')


# #adspends query
# top_spends_df['dt'] = pd.to_datetime(top_spends_df['dt'])

# #ai spends
# ai_spends_df['spend'] = pd.to_numeric(ai_spends_df['spend'], errors='coerce')

# # Drop the currency_code column and rename columns as required
# # top_spends_df = top_spends_df.drop(columns=['currency_code'])
# # top_spends_df = top_spends_df.rename(columns={"converted_currency": "currency", "converted_spend": "spend"})
# top_spends_df['spend'] = pd.to_numeric(top_spends_df['spend'], errors='coerce')

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

#datong df
# datong_api_df['dt'] = pd.to_datetime(datong_api_df['dt']).dt.date
# datong_api_df['spend'] = pd.to_numeric(datong_api_df['spend'], errors='coerce')
# datong_api_df['euid'] = pd.to_numeric(datong_api_df['euid'], errors='coerce')
# datong_api_df['total_spend'] = pd.to_numeric(datong_api_df['total_spend'], errors='coerce')
# datong_api_df['per'] = pd.to_numeric(datong_api_df['per'], errors='coerce')


#removed revenue analysis, "AI account spends","Datong API VS Total Spends",
# "joystick"
#Sidebar
with st.sidebar:
    selected = option_menu(
        menu_title="Navigation",  # Required
        options=["Login","Ad account stats"],  # Required
        icons=["lock","airplane-engines"],  # Optional: icons from the Bootstrap library
        menu_icon="cast",  # Optional: main menu icon
        default_index=0,  # Default active menu item
    )
        # Add a refresh button to the sidebar
    if st.button("Refresh Data", key="refresh_button"):
        st.cache_data.clear()  # Clear cached data
        st.success("Cache cleared and data refreshed!")


# st.warning('Only Ad360 Accounts Spends data are available for now', icon="⚠️")

#Page sections
if selected == "Login":

    st.session_state.status = st.session_state.get("status", "unverified")
    st.title("Login page")

    def check_password():
        if hmac.compare_digest(st.session_state.password, st.secrets.password):
            st.session_state.status = "verified"
        else:
            st.session_state.status = "incorrect"
        st.session_state.password = ""

    def login_prompt():
        st.text_input("Enter password:", key="password", on_change=check_password)
        if st.session_state.status == "incorrect":
            st.warning("Incorrect password. Please try again.")

    def logout():
        st.session_state.status = "unverified"

    def welcome():
        st.success("Login successful.")
        st.button("Log out", on_click=logout)


    if st.session_state.status != "verified":
        login_prompt()
        st.stop()
    welcome()

elif selected == "Ad account stats" and st.session_state.status == "verified":

    st.title("Ad Account Stats")
  

    def extract_link(json_str):
        try:
            obj = json.loads(json_str)
            story_spec = obj.get("object_story_spec", {})

            # Search in video_data, link_data, photo_data
            for key in ["video_data", "link_data", "photo_data"]:
                if key in story_spec:
                    data_block = story_spec[key]

                    # 1. call_to_action.value.link
                    link = (
                        data_block.get("call_to_action", {})
                                .get("value", {})
                                .get("link")
                    )
                    if link:
                        return link

                    # 2. fallback to top-level "link"
                    if "link" in data_block:
                        return data_block["link"]

            return None
        except:
            return None

    def extract_asset_link(json_str):
        try:
            obj = json.loads(json_str)
            link_urls = obj.get("link_urls", [])
            if link_urls and "website_url" in link_urls[0]:
                return link_urls[0]["website_url"]
            return None
        except:
            return None

    # Combined extraction with fallback
    def get_final_link(row):
        link = extract_link(row["object_story_spec"])
        if not link:  # fallback to asset_feed_spec
            link = extract_asset_link(row["asset_feed_spec"])
        return link

    # Get user input for ad account IDs (comma separated)
    # Load ad account IDs from Google Sheet
    sheet_url = "https://docs.google.com/spreadsheets/d/1mRUXiE1JPg6L1p26XrnHZXffPIsTnAl-KIFE2GFeEus/export?format=csv"
    sheet_df = pd.read_csv(sheet_url)
    ad_account_ids = sheet_df['ad_account_id'].dropna().astype(str).tolist()

    links_df = pd.DataFrame()
    ad_stats_df = pd.DataFrame()
    if ad_account_ids:
        ad_accounts_str = ",".join([f"'{x}'" for x in ad_account_ids])
        # Ad Creative Links Query
        adcreative_query = f"""
            SELECT DISTINCT ad_account_id, adcreative_id, object_story_spec, asset_feed_spec
            FROM zocket_global.fb_adcreative_details_v3
            WHERE ad_account_id IN ({ad_accounts_str})
        """
        links_df = execute_query(query=adcreative_query)

        links_df["link"] = links_df.apply(get_final_link, axis=1)
        links_df["domain"] = links_df["link"].apply(lambda x: urlparse(x).netloc)

        # Combine all unique domains per ad account, separated by comma
        domains_per_account = (
            links_df.groupby('ad_account_id')['domain']
            .apply(lambda x: ', '.join(sorted(set(str(val) for val in x.dropna()))))
            .reset_index()
            .rename(columns={'domain': 'unique_domains'})
        )

        # Merge domains info into main df
        df = df.merge(domains_per_account, on='ad_account_id', how='left')
        # Fetch campaign objectives per ad account (Redshift uses LISTAGG)
        campaign_objectives_query = f"""
        SELECT ad_account_id, LISTAGG(DISTINCT objective, ', ') WITHIN GROUP (ORDER BY objective) AS campaign_objectives
        FROM zocket_global.fb_campaign_details_v3
        WHERE ad_account_id IN ({ad_accounts_str})
        GROUP BY ad_account_id
        """
        campaign_objectives_df = execute_query(query=campaign_objectives_query)

        # Fetch targeting types per ad account (Redshift uses LISTAGG)
        targeting_type_query = f"""
        SELECT ad_account_id, LISTAGG(DISTINCT destination_type, ', ') WITHIN GROUP (ORDER BY destination_type) AS targeting_type
        FROM zocket_global.fb_adsets_details_v3
        WHERE ad_account_id IN ({ad_accounts_str})
        GROUP BY ad_account_id
        """
        targeting_type_df = execute_query(query=targeting_type_query)

        # Merge with main df
        df = df.merge(campaign_objectives_df, on='ad_account_id', how='left')
        df = df.merge(targeting_type_df, on='ad_account_id', how='left')
        # Get all unique currencies except USD
        unique_currencies = df['currency_code'].unique()
        unique_currencies = [c for c in unique_currencies if c != 'USD']

        # Fetch latest conversion rates for each currency (using exchangerate-api.com as example)
        conversion_rates = {}
        api_url = "https://open.er-api.com/v6/latest/USD"
        try:
            response = requests.get(api_url)
            if response.status_code == 200:
                rates = response.json().get('rates', {})
                for currency in unique_currencies:
                    if currency in rates:
                        conversion_rates[currency] = float(rates[currency])
                    else:
                        conversion_rates[currency] = 1.0
            else:
                conversion_rates = {currency: 1.0 for currency in unique_currencies}
        except Exception:
            conversion_rates = {currency: 1.0 for currency in unique_currencies}
        conversion_rates = {currency: 1.0 / rate if rate != 0 else 1.0 for currency, rate in conversion_rates.items()}
        conversion_rates['USD'] = 1.0

        def convert_row_to_usd(row):
            rate = conversion_rates.get(row['currency_code'], 1.0)
            return row['spend'] * rate

        df['spend_in_usd'] = df.apply(convert_row_to_usd, axis=1)

        # Filter df for these ad accounts
        ad_stats_df = df[df['ad_account_id'].isin(ad_account_ids)].copy()
        if ad_stats_df.empty:
            st.warning("No data found for the ad account IDs from the sheet.")
        else:
            today = datetime.now().date()
            last_7_days = today - timedelta(days=7)
            last_30_days = today - timedelta(days=30)
            start_of_quarter = today.replace(month=((today.month - 1) // 3) * 3 + 1, day=1)

            results = []
            for ad_acc in ad_account_ids:
                acc_df = ad_stats_df[ad_stats_df['ad_account_id'] == ad_acc]
                last_7_spend = acc_df[acc_df['dt'] >= last_7_days]['spend_in_usd'].sum()
                last_30_spend = acc_df[acc_df['dt'] >= last_30_days]['spend_in_usd'].sum()
                qtd_spend = acc_df[acc_df['dt'] >= start_of_quarter]['spend_in_usd'].sum()
                acc_status = disabled_account_df[disabled_account_df['ad_account_id'] == ad_acc]['flag']
                acc_status = acc_status.iloc[0] if not acc_status.empty else "Active"
                active_status = "Active" if last_7_spend > 0 else "Inactive"
                ad_account_name = acc_df['ad_account_name'].iloc[0] if not acc_df.empty else ""
                domains = acc_df['unique_domains'].iloc[0] if 'unique_domains' in acc_df and not acc_df.empty else ""
                objectives = acc_df['campaign_objectives'].iloc[0] if 'campaign_objectives' in acc_df and not acc_df.empty else ""
                targeting = acc_df['targeting_type'].iloc[0] if 'targeting_type' in acc_df and not acc_df.empty else ""
                results.append({
                    "Ad Account ID": ad_acc,
                    "Ad Account Name": ad_account_name,
                    "Last 7 Days Spend (USD)": last_7_spend,
                    "Last 30 Days Spend (USD)": last_30_spend,
                    "Quarter Till Date Spend (USD)": qtd_spend,
                    "Account Status": acc_status,
                    "Active Status (last 7d)": active_status,
                    "Domains": domains,
                    "Objective": objectives,
                    "Targeting": targeting
                })
            st.dataframe(pd.DataFrame(results), use_container_width=True)
    else:
        st.info("No ad account IDs found in the sheet.")