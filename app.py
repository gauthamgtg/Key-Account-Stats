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
    (SELECT max(cast(aas.euid as float)) as euid ,ad_account_id,date(date_start) as dt,sum(spend)spend
    from  ad_account_spends aas 
    group by 2,3),
    total_payment AS
        (select cast(td.euid as float)euid,ad_account,sum(adspend_amount) total_paid 
        from payment_trans_details td
        group by 1,2)

select eu.euid,eu.business_name,eu.company_name,dt,efaa.ad_account_name,
case when efaa.flag is null then 'INR' else efaa.flag end as currency_code,a.ad_account_id,total_spent,total_paid,spend,curr_month_spend,
case when lower(eu.company_name) like '%datong%' or lower(eu.company_name) like '%omaza%' then 'Datong' 
        when eu.euid in (2310,2309,2202,2201,2181,2168,2100,2051,2281,2394) then 'FB Boost'
        when eu.euid in (1911)then 'Adfly' 
        when eu.euid in  ( 527, 785, 1049, 1230, 1231) or a.ad_account_id ='act_797532865863232' then 'Eleganty'
        when a.ad_account_id in 
        (
        'act_3563973227209697',
        'act_957109429531250',
        'act_759315738654233',
        'act_723792699245884',
        'act_604278331059492',
        'act_881404577110091',
        'act_397827242568247',
        'act_586902686585383',
        'act_965249685184093',
        'act_1306548849911815',
        'act_565292762205849',
        'act_281403371310608',
        'act_2059544207742648',
        'act_873580428253310',
        'act_1097129248477609',
        'act_308308454982919',
        'act_427844130047005',
        'act_1653390585242405',
        'act_1860659564374272',
        'act_1292987141870282',
        'act_1068783194317542',
        'act_216902994241137',
        'act_6915108528593741',
        'act_881404577110091',
        'act_1237873617243932',
        'act_571119348702020',
        'act_789733129886592'
        ) and dt>='2024-10-01' then 'Roposo'
        else 'Others' end as top_customers_flag
from enterprise_users eu
    left join (
        select euid,ad_account_id,dt,sum(spend)spend
        from spends
        group by 1,2,3
    )a
    on eu.euid=a.euid
    left join (
        select euid,ad_account_id,sum(spend)total_spent
        from spends
        group by 1,2
    )ts
    on a.euid=ts.euid and a.ad_account_id=ts.ad_account_id
    LEFT join 
    ( 
        SELECT cast(aas.euid as float) as euid ,ad_account_id,sum(spend)curr_month_spend
    from  spends aas
    where extract(month from dt)= extract(month from current_date) and
          extract(year from dt)= extract(year from current_date)
    group by 1,2
    ) cms
    on cms.euid=eu.euid and cms.ad_account_id=a.ad_account_id
    left join total_payment b
    on  b.euid=eu.euid and concat('act_', b.ad_account)=a.ad_account_id
    left join 
        (
            SELECT ad_account_id,ad_account_name,currency as flag
            FROM
            (
            select ad_account_id,ad_account_name,fb_user_id,row_number() over(partition by ad_account_id order by fb_user_id) as rank,currency
            from 
            (select * from enterprise_facebook_ad_account
            where status='true')a
            )a
            where rank=1 
        ) efaa
    on concat('act_',efaa.ad_account_id)= a.ad_account_id
    where a.ad_account_id is not null
    order by euid desc
    '''



# sub_query = '''
# SELECT * FROM
# (
# SELECT euid,ad_account_id,ad_account_name,sub_start, sub_end, 
# currency, plan_amount,plan_limit,flag,total_subscription_days, subscription_days_completed,adspends_added,
# ROUND(expected_per_day_spend, 2) AS expected_per_day_spend,
# ROUND(expected_per_day_spend * subscription_days_completed, 2) AS expected_TD_spend,
# adspends_added AS actual_TD_spend,
# -- round((cast(subscription_days_completed as float)/ total_subscription_days)*100,2) AS expected_TD_util,
# case when subscription_days_completed =0 then 0 else ROUND((adspends_added / (expected_per_day_spend * subscription_days_completed)) * 100, 2) end AS actual_TD_util,
# case when subscription_days_completed =0 then 0 else ROUND(((expected_per_day_spend * subscription_days_completed) / plan_limit) * 100, 2) end AS expected_util,
# ROUND((adspends_added / plan_limit) * 100, 2) AS overall_util
# ,row_number() over(partition by ad_account_id ORDER by sub_start desc) as rw

# FROM
# (
# SELECT  euid,ad_account_id,ad_account_name,created_at as sub_start,expiry_date as sub_end, 
# currency, plan_amount,plan_limit,flag,
# datediff('day',sub_start,expiry_date) as total_subscription_days,
# (datediff('day',sub_start,current_date)) as subscription_days_completed,
# (plan_limit/datediff('day',sub_start,expiry_date)) as expected_per_day_spend,
# (adspend_added+transfer_amount) as adspends_added
# FROM
# (
# SELECT  a.euid,a.ad_account_id,ad_account_name,a.created_at,a.expiry_date, currency,
# usd_amount as plan_amount,
# case when currency='INR' then price_range else usd_price_range end as plan_limit,transfer_amount,flag, sum(adspend_added) as adspend_added

# FROM
# (
# SELECT  a.euid,a.ad_account_id,a.created_at,a.expiry_date,a.amount as subamt,sp.plan_id,
#  ad_account_name,
# coalesce(efaa.currency,faa.currency,'INR') as currency,
#  sp.plan_name, sp.usd_amount,sp.usd_price_range,sp.price_range, transfer_amount,flag
# FROM
# (
# select distinct payment_id, euid, 
# case when ad_account_id = 'act_1735955147217127' then '1735955147217127' else ad_account_id end as ad_account_id,
# case when transfer_euid is not null then 'Transfered' else 'New' end as flag,
# (transfer_amount)transfer_amount,
# date(usd.created_at) created_at, expiry_date,amount,plan_id
# from user_subscription_data usd 
# where  coalesce(tranferred_at,expiry_date) >= current_date and id!=1952
# -- group by 1,2,3,4
# order by 3
# )a
# left join 
# fb_ad_accounts faa on concat('act_',a.ad_account_id) = faa.ad_account_id
# left join 
# (
# SELECT ad_account_id,ad_account_name,currency
# FROM
# (
# select ad_account_id,ad_account_name,fb_user_id,row_number() over(partition by ad_account_id order by fb_user_id) as rank,currency
# from 
# (select * from enterprise_facebook_ad_account
# where status='true')a
# )a
# where rank=1 
# ) efaa
# on a.ad_account_id = efaa.ad_account_id 
# left join subscription_plan sp on a.plan_id = sp.plan_id
# )a
# left JOIN
# (
#     SELECT euid,ad_account,date(payment_date) as payment_date,sum(adspend_amount) as adspend_added
#     from payment_trans_details
#     GROUP by 1,2,3
# ) as adspends 
# on a.euid=adspends.euid and a.ad_account_id=adspends.ad_account and adspends.payment_date>=a.created_at
# group by 1,2,3,4,5,6,7,8,9,10

# )
# order by 2
# )
# )
# where 
# euid not in (701,39)
#     '''

list_query = '''
SELECT distinct b.app_business_id as euid, a.ad_account_id, a.name as ad_account_name, b.name as business_manager_name,eu.business_name,eu.company_name
FROM fb_ad_accounts a
	LEFT JOIN fb_business_managers b ON b.id = a.app_business_manager_id
   left join enterprise_users eu on b.app_business_id=eu.euid
   order by 1 desc
'''

all_spends_query='''

SELECT euid,dt,ad_account_id,ad_account_name,currency_code,case when currency_code = 'INR' then cast(spend as text)
when currency_code='EUR' then cast(spend*1.09 as text)
when currency_code='GBP' then cast(spend*1.3 as text)
when currency_code='AUD' then cast(spend*0.66 as text)
when currency_code='USD' then cast(spend as text) end as converted_spend,
case when currency_code = 'INR' then 'INR'
when currency_code='EUR' then 'USD'
when currency_code='GBP' then 'USD'
when currency_code='AUD' then 'USD'
when currency_code='USD' then 'USD' end as converted_currency,
spend as original_spend
FROM
(SELECT
 a.euid,a.ad_account_id,ad_account_name,case when a.ad_account_id = 'act_507277141809499' then 'USD'
when a.ad_account_id = 'act_1250764673028073' then 'USD'
when efaa.flag is null then 'INR' 
else efaa.flag end as currency_code,date_start as dt, spend
    from  ad_account_spends a 
left join 
        (
            SELECT ad_account_id,ad_account_name,currency as flag
            FROM
            (
            select ad_account_id,ad_account_name,fb_user_id,row_number() over(partition by ad_account_id order by fb_user_id) as rank,currency
            from 
            (select * from enterprise_facebook_ad_account
            where status='true')a
            )a
            where rank=1 
        ) efaa
    on concat('act_',efaa.ad_account_id)= a.ad_account_id
)
'''


@st.cache_data(ttl=86400)  # 86400 seconds = 24 hours
@redshift_connection(db,name,passw,server,port)
def execute_query(connection, cursor,query):

    cursor.execute(query)
    column_names = [desc[0] for desc in cursor.description]
    result = pd.DataFrame(cursor.fetchall(), columns=column_names)

    return result

df = execute_query(query=query)
# sub_df = execute_query(query=sub_query)
list_df = execute_query(query=list_query)
spends_df = execute_query(query=all_spends_query)

#chaning proper format of date
df['dt'] = pd.to_datetime(df['dt']).dt.date

#changing spend to numeric
df['spend'] = pd.to_numeric(df['spend'], errors='coerce')
df['euid'] = pd.to_numeric(df['euid'], errors='coerce')

# #Revenue analysis query

# sub_df['euid'] = pd.to_numeric(sub_df['euid'], errors='coerce')
# sub_df['plan_amount'] = pd.to_numeric(sub_df['plan_amount'], errors='coerce')
# # sub_df['ad_account_id'] = pd.to_numeric(sub_df['ad_account_id'], errors='coerce')
# sub_df['sub_start'] = pd.to_datetime(sub_df['sub_start']).dt.date
# sub_df['sub_end'] = pd.to_datetime(sub_df['sub_end']).dt.date
# sub_df['total_subscription_days'] = pd.to_numeric(sub_df['total_subscription_days'], errors='coerce')
# sub_df['subscription_days_completed'] = pd.to_numeric(sub_df['subscription_days_completed'], errors='coerce')
# sub_df['adspends_added'].fillna(0, inplace=True)
# sub_df['adspends_added'] = pd.to_numeric(sub_df['adspends_added'], errors='coerce')
# sub_df['expected_per_day_spend'] = pd.to_numeric(sub_df['expected_per_day_spend'], errors='coerce')
# sub_df['expected_td_spend'] = pd.to_numeric(sub_df['expected_td_spend'], errors='coerce')
# sub_df['actual_td_spend'] = pd.to_numeric(sub_df['actual_td_spend'], errors='coerce')
# sub_df['actual_td_util'] = pd.to_numeric(sub_df['actual_td_util'], errors='coerce')
# sub_df['expected_util'] = pd.to_numeric(sub_df['expected_util'], errors='coerce')
# sub_df['overall_util'] = pd.to_numeric(sub_df['overall_util'], errors='coerce')
# sub_df['rw'] = pd.to_numeric(sub_df['rw'], errors='coerce')

#adspends query
spends_df['dt'] = pd.to_datetime(spends_df['dt'])


# Drop the currency_code column and rename columns as required
spends_df = spends_df.drop(columns=['currency_code'])
spends_df = spends_df.rename(columns={"converted_currency": "currency", "converted_spend": "spend"})
spends_df['spend'] = pd.to_numeric(spends_df['spend'], errors='coerce')

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

#Sidebar
with st.sidebar:
    selected = option_menu(
        menu_title="Navigation",  # Required
        options=["Login","Key Account Stats", "Raw Data","Overall Stats - Ind","Overall Stats - US","Revenue-Analysis","Euid - adaccount mapping","Top accounts"],  # Required
        icons=["lock","airplane-engines", "table","currency-rupee",'currency-dollar','cash-coin','link',"graph-up"],  # Optional: icons from the Bootstrap library
        menu_icon="cast",  # Optional: main menu icon
        default_index=0,  # Default active menu item
    )
        # Add a refresh button to the sidebar
    if st.button("Refresh Data", key="refresh_button"):
        st.cache_data.clear()  # Clear cached data
        st.success("Cache cleared and data refreshed!")

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

    




if selected == "Key Account Stats" and st.session_state.status == "verified":
    st.title("Key Account Stats")
    st.write("Show detailed spends of top customers.")

    col1,col2 = st.columns(2)

    with col1:
    # Step 1: Ask the customer to choose a top_customers_flag
        flag_options = df['top_customers_flag'].unique()
        selected_flag = st.selectbox('Choose Top Customers Flag', flag_options, index=2)

    with col2:
    # Filter the dataframe based on the selected top_customers_flag
        filtered_df = df[df['top_customers_flag'] == selected_flag]

        # Step 2: Ask the customer to choose grouping (year, month, week, or date)
        grouping = st.selectbox('Choose Grouping', ['Year', 'Month', 'Week', 'Date'], index=1)


    col1, col2, col3 = st.columns(3)
    col1.metric("Total Spend",filtered_df['spend'].sum())
    col2.subheader("Start Date : " + str(filtered_df['dt'].min()))
    col3.subheader("Last Active Date : " + str(filtered_df['dt'].max()))

    # Filter data for yesterday and day before yesterday
    yesterday_data = filtered_df[filtered_df['dt'] == yesterday]
    day_before_yst_data = filtered_df[filtered_df['dt'] == day_before_yst]
    last_month_df = filtered_df[(filtered_df['dt'] >= last_month.replace(day=1)) & (filtered_df['dt'] < last_month.replace(day=1) + timedelta(days=32))]
    
    
    # Calculate the total spend for each day
    yst_spend = yesterday_data['spend'].sum()
    day_before_yst_spend = day_before_yst_data['spend'].sum()
    last_month_spend = last_month_df['spend'].sum()

    # Filter the DataFrame to get the current month’s data
    current_month_df = filtered_df[
        (pd.to_datetime(filtered_df['dt']).dt.month == current_month) &
        (pd.to_datetime(filtered_df['dt']).dt.year == current_year)
    ]


    # Calculate the total spend for the current month
    total_current_month_spend = current_month_df['spend'].sum()

    

    cols1, cols2, cols3 = st.columns(3)

    # Metric 1: Yesterday's Spend and change %
    cols1.metric("Yesterday Spend", f"{yst_spend}"
            #, f"{ind_spend_change:,.2f}%"
            )
    cols2.metric("Current Month Spend", f"{total_current_month_spend}"
            #, f"{ind_spend_change:,.2f}%"
            )
    cols3.metric("Last Month Spend", f"{last_month_spend}"
            #, f"{ind_spend_change:,.2f}%"
            )
    

    # Assuming your 'dt' column is already in date format (e.g., YYYY-MM-DD)
    if grouping == 'Year':
        filtered_df.loc[:, 'grouped_date'] = filtered_df['dt'].apply(lambda x: x.strftime('%Y'))  # Year format as 2024
    elif grouping == 'Month':
        filtered_df.loc[:, 'grouped_date'] = filtered_df['dt'].apply(lambda x: x.strftime('%b-%y'))  # Month format as Jan-24
    elif grouping == 'Week':
        filtered_df.loc[:, 'grouped_date'] = filtered_df['dt'].apply(lambda x: f"{x.strftime('%Y')} - week {x.isocalendar()[1]}")  # Week format as 2024 - week 24
    else:
        filtered_df.loc[:, 'grouped_date'] = filtered_df['dt']  # Just use the date as is (in date format)


    # Aggregate the spend values by the selected grouping
    grouped_df = filtered_df.groupby(['euid','grouped_date'])['spend'].sum().reset_index()

    # Step 5: Pivot the DataFrame so that each 'euid' becomes a column
    pivot_df = grouped_df.pivot(index='grouped_date', columns='euid', values='spend')

    # Step 6: Display the table of top_customers_flag and grouped spend values
    st.write(f"Grouped Spend for Top Customers Flag: {selected_flag}")
    pivots_df = grouped_df.pivot(index='euid', columns='grouped_date', values='spend')
    # st.button("Transpose", type="primary")
# Initialize session state for button press
    if 'show_transposed' not in st.session_state:
        st.session_state['show_transposed'] = False  # Default to showing non-transposed data

    # Button to toggle between showing the original pivot_df and transposed pivots_df
    if st.button("Transpose"):
        # Toggle the state of transposed view
        st.session_state['show_transposed'] = not st.session_state['show_transposed']

    # Display the appropriate DataFrame based on the button's state
    if st.session_state['show_transposed']:
        st.write("Pivot Data:")
        st.dataframe(pivots_df, use_container_width=True)
        # st.write(pivots_df)  # Show transposed
    else:
        st.write("Pivot Data:")
        st.dataframe(pivot_df, use_container_width=True)
        # st.write(pivot_df)  # Show original
    
    # Step 7: Display the line chart with different 'euid' values in different colors
    st.line_chart(pivot_df)

    # st.dataframe(yesterday_data, use_container_width=True)
    
    # Aggregate the spend values by the selected grouping
    grouped_data_adacclevel = filtered_df.groupby(['euid','ad_account_id','grouped_date'])['spend'].sum().reset_index()
    pivoted_data_adacclevel = grouped_data_adacclevel.pivot(columns='grouped_date', index=['euid','ad_account_id'], values='spend')
    pivoted_data_adacclevels = grouped_data_adacclevel.pivot(index='grouped_date', columns=['ad_account_id'], values='spend')
    # st.dataframe(grouped_data_adacclevel, use_container_width=True)
    st.dataframe(pivoted_data_adacclevel, use_container_width=True)

    if 'show_transposedd' not in st.session_state:
        st.session_state['show_transposedd'] = False  # Default to showing non-transposed data

    # Button to toggle between showing the original pivot_df and transposed pivots_df
    if st.button("Transposè"):
        # Toggle the state of transposed view
        st.session_state['show_transposedd'] = not st.session_state['show_transposedd']

    # Display the appropriate DataFrame based on the button's state
    if st.session_state['show_transposedd']:
        st.write("Pivot Data:")
        st.dataframe(pivoted_data_adacclevel, use_container_width=True)
        # st.write(pivots_df)  # Show transposed
    else:
        st.write("Pivot Data:")
        st.dataframe(pivoted_data_adacclevels, use_container_width=True)
        # st.write(pivot_df)  # Show original



    st.session_state['grouped_data_adacclevel'] = grouped_data_adacclevel #to store the data in global variable and get it in else condition
    st.session_state['pivoted_data_adacclevel'] = pivoted_data_adacclevel #to store the data in global variable and get it in else condition

elif selected == "Raw Data" and st.session_state.status == "verified":
    st.title("Raw Data Page")
    st.write("This is where raw data will be displayed.")
    st.write(df)



    # Initialize session state for button press
    if 'show_inverted' not in st.session_state:
        st.session_state['show_inverted'] = False  # Default to showing non-transposed data

    # Button to toggle between showing the original pivot_df and transposed pivots_df
    if st.button("Invert"):
        # Toggle the state of transposed view
        st.session_state['show_inverted'] = not st.session_state['show_inverted']

    # Display the appropriate DataFrame based on the button's state
    if st.session_state['show_inverted']:
        st.write("Pivot Data:")
        st.dataframe(grouped_data_adacclevel, use_container_width=True)
        st.write(grouped_data_adacclevel)  # Show transposed
    else:
        st.write("Pivot Data:")
        st.dataframe(pivoted_data_adacclevel, use_container_width=True)
        st.write(pivoted_data_adacclevel)  # Show original

    st.write(st.session_state['grouped_data_adacclevel'])


elif selected == "Overall Stats - Ind" and st.session_state.status == "verified":

    st.title("Overall Stats - India")

    indian_df = df[df['currency_code'].str.lower() == 'inr']
    
    # Group the DataFrame by 'euid', 'ad_account_id', and 'dt', and sum 'spend'
    ind_grouped_data_adacclevel = indian_df.groupby(['euid', 'ad_account_id', 'dt'])['spend'].sum().reset_index()


    # Filter data for yesterday and day before yesterday
    ind_yesterday_data = ind_grouped_data_adacclevel[ind_grouped_data_adacclevel['dt'] == yesterday]
    ind_day_before_yst_data = ind_grouped_data_adacclevel[ind_grouped_data_adacclevel['dt'] == day_before_yst]

    # Calculate the total spend for each day
    ind_yst_spend = ind_yesterday_data['spend'].sum()
    ind_day_before_yst_spend = ind_day_before_yst_data['spend'].sum()

    # Calculate the number of unique ad_account_id for each day
    ind_num_ad_accounts_yesterday = ind_yesterday_data['ad_account_id'].nunique()
    ind_num_ad_accounts_day_before_yst = ind_day_before_yst_data['ad_account_id'].nunique()

    # Ensure we have data to avoid division by zero
    if ind_day_before_yst_spend > 0:
        ind_spend_change = round(((ind_yst_spend - ind_day_before_yst_spend) / ind_day_before_yst_spend) * 100, 2)
    else:
        ind_spend_change = 0

    if ind_num_ad_accounts_day_before_yst > 0:
        ind_ad_account_change = round(((ind_num_ad_accounts_yesterday - ind_num_ad_accounts_day_before_yst) / ind_num_ad_accounts_day_before_yst) * 100, 2)
    else:
        ind_ad_account_change = 0

    # Calculate average spend per ad account for yesterday and day before yesterday
    if ind_num_ad_accounts_yesterday > 0:
        ind_avg_spend_per_account_yesterday = ind_yst_spend / ind_num_ad_accounts_yesterday
    else:
        ind_avg_spend_per_account_yesterday = 0

    if ind_num_ad_accounts_day_before_yst > 0:
        ind_avg_spend_per_account_day_before = ind_day_before_yst_spend / ind_num_ad_accounts_day_before_yst
    else:
        ind_avg_spend_per_account_day_before = 0

    # Calculate the change in average spend per ad account
    if ind_avg_spend_per_account_day_before > 0:
        avg_ind_spend_change = round(((ind_avg_spend_per_account_yesterday - ind_avg_spend_per_account_day_before) / ind_avg_spend_per_account_day_before) * 100, 2)
    else:
        avg_ind_spend_change = 0

    
    # Filter the DataFrame to get the current month’s data
    ind_current_month_df = indian_df[
        (pd.to_datetime(indian_df['dt']).dt.month == current_month) &
        (pd.to_datetime(indian_df['dt']).dt.year == current_year)
    ]

    # Calculate the total spend for the current month
    ind_current_month_spend = ind_current_month_df['spend'].sum()

    # Display the metrics
    col1, col2, col3, col4 = st.columns(4)

    # Metric 1: Yesterday's Spend and change %
    col1.metric("Yesterday Spend", f"₹{ind_yst_spend}", f"{ind_spend_change:,.2f}%")

    # Metric 2: Number of Ad Accounts and change %
    col2.metric("Ad Accounts", ind_num_ad_accounts_yesterday, f"{ind_ad_account_change}%")

    # Metric 3: Average Spend per Ad Account and change %
    col3.metric("Avg Spend per Ad Account", f"₹{round(ind_avg_spend_per_account_yesterday, 2)}", f"{avg_ind_spend_change}%")

    # Display the current month spend as a metric
    col4.metric(label="Current Month Spend", value=f"₹{ind_current_month_spend:,.2f}")


    st.write("Yesterday spend data:")
    st.dataframe(indian_df[indian_df['dt']==yesterday], use_container_width=True)

    st.write("Current Month spend data:")
    ind_grouped_data_adacclevel = ind_current_month_df.groupby([pd.to_datetime(ind_current_month_df['dt']).dt.strftime('%b %y'), 'euid','ad_account_id'])['spend'].sum().reset_index(name='spend').sort_values(by='spend', ascending=False)

    # #Dataframe grouped by month and year of dt
    # ind_grouped_data_adacclevel = indian_df[indian_df['dt'].dt.month == current_month].groupby([pd.to_datetime(indian_df['dt']).dt.strftime('%b %y'), 'euid'])['spend'].sum().reset_index(name='spend').sort_values(by='spend', ascending=False)
    st.dataframe(ind_grouped_data_adacclevel, use_container_width=True)
    
    st.write("Current Month spend data:")
    st.dataframe(ind_current_month_df, use_container_width=True)

    st.write("Overall spend data:")
    # st.dataframe(indian_df, use_container_width=True)
    st.dataframe(ind_grouped_data_adacclevel, use_container_width=True)

    st.write('Day level spends')
    ind_grouped_data = indian_df.groupby(['dt'])['spend'].sum().reset_index().sort_values(by='dt', ascending=False)
    st.dataframe(ind_grouped_data, use_container_width=True)

    st.line_chart(ind_grouped_data, x='dt', y='spend')


elif selected == "Overall Stats - US" and st.session_state.status == "verified":

    st.title("Overall Stats - US")

    us_df = df[df['currency_code'].str.lower() != 'inr']

    us_df = us_df.loc[:, us_df.columns != 'company_name']

    # Identify unique currency codes other than 'USD'
    non_usd_currencies = us_df['currency_code'].unique()
    non_usd_currencies = [currency for currency in non_usd_currencies if currency != 'USD']

    # Create a dictionary to store the conversion rates entered by the user
    conversion_rates = {}

    # Predefine default values for specific currencies
    default_values = {
                        'EUR': 1.1,
                        'GBP': 1.3,
                        'AUD': 0.75
                    }

    # Display input boxes for each unique currency code other than 'USD'
    st.write("Enter conversion rates for the following currencies:")
   
    # Create columns dynamically based on the number of currencies
    cols = st.columns(3)  # Adjust the number of columns (3 in this case)

    # Iterate over non-USD currencies and display them in columns
    for idx, currency in enumerate(non_usd_currencies):
        default_value = default_values.get(currency, 1.0)  # Use default value if defined, otherwise 1.0
        with cols[idx % 3]:  # Rotate through the columns
            conversion_rates[currency] = st.number_input(
                f"{currency} to USD:", value=default_value, min_value=0.0, step=0.001, format="%.3f"
            )

    # Function to convert the spend to USD using the entered conversion rates
    def convert_to_usd(row):
        if row['currency_code'] == 'USD':
            return row['spend']
        elif row['currency_code'] in conversion_rates:
            return row['spend'] / conversion_rates[row['currency_code']]
        return row['spend']

    # Create the 'spend_in_usd' column
    us_df['spend_in_usd'] = us_df.apply(lambda row: convert_to_usd(row), axis=1)

    
    # Group the DataFrame by 'euid', 'ad_account_id', and 'dt', and sum 'spend'
    us_grouped_data_adacclevel = us_df.groupby([ 'ad_account_id', 'dt'])['spend_in_usd'].sum().reset_index()

    # Filter data for yesterday and day before yesterday
    us_yesterday_data = us_grouped_data_adacclevel[us_grouped_data_adacclevel['dt'] == yesterday]
    us_day_before_yst_data = us_grouped_data_adacclevel[us_grouped_data_adacclevel['dt'] == day_before_yst]

    # Calculate the total spend for each day
    us_yst_spend = round(us_yesterday_data['spend_in_usd'].sum(),2)
    us_day_before_yst_spend = us_day_before_yst_data['spend_in_usd'].sum()

    # Calculate the number of unique ad_account_id for each day
    us_num_ad_accounts_yesterday = us_yesterday_data['ad_account_id'].nunique()
    us_num_ad_accounts_day_before_yst = us_day_before_yst_data['ad_account_id'].nunique()

    # Ensure we have data to avoid division by zero
    if us_day_before_yst_spend > 0:
        us_spend_change = round(((us_yst_spend - us_day_before_yst_spend) / us_day_before_yst_spend) * 100, 2)
    else:
        us_spend_change = 0

    if us_num_ad_accounts_day_before_yst > 0:
        us_ad_account_change = round(((us_num_ad_accounts_yesterday - us_num_ad_accounts_day_before_yst) / us_num_ad_accounts_day_before_yst) * 100, 2)
    else:
        us_ad_account_change = 0

    # Calculate average spend per ad account for yesterday and day before yesterday
    if us_num_ad_accounts_yesterday > 0:
        us_avg_spend_per_account_yesterday = us_yst_spend / us_num_ad_accounts_yesterday
    else:
        us_avg_spend_per_account_yesterday = 0

    if us_num_ad_accounts_day_before_yst > 0:
        us_avg_spend_per_account_day_before = us_day_before_yst_spend / us_num_ad_accounts_day_before_yst
    else:
        us_avg_spend_per_account_day_before = 0

    # Calculate the change in average spend per ad account
    if us_avg_spend_per_account_day_before > 0:
        us_avg_spend_change = round(((us_avg_spend_per_account_yesterday - us_avg_spend_per_account_day_before) / us_avg_spend_per_account_day_before) * 100, 2)
    else:
        us_avg_spend_change = 0

    
    # Filter the DataFrame to get the current month’s data
    current_month_df = us_df[
        (pd.to_datetime(us_df['dt']).dt.month == current_month) &
        (pd.to_datetime(us_df['dt']).dt.year == current_year)
    ]

    # Calculate the total spend for the current month
    total_current_month_spend = current_month_df['spend_in_usd'].sum()


    # Display the metrics
    col1, col2, col3, col4 = st.columns(4)

    # Metric 1: Yesterday's Spend and change %
    col1.metric("Yesterday Spend", f"${us_yst_spend}", f"{us_spend_change}%")

    # Metric 2: Number of Ad Accounts and change %
    col2.metric("Ad Accounts", us_num_ad_accounts_yesterday, f"{us_ad_account_change}%")

    # Metric 3: Average Spend per Ad Account and change %
    col3.metric("Avg Spend per Ad Account", f"${round(us_avg_spend_per_account_yesterday, 2)}", f"{us_avg_spend_change}%")

    # Display the current month spend as a metric
    col4.metric(label="Current Month Spend (in USD)", value=f"${total_current_month_spend:,.2f}")

    st.write("Yesterday spend data:")
    st.dataframe(us_df[us_df['dt']==yesterday], use_container_width=True)

    st.write("Current Month spend data:")
    st.dataframe(current_month_df, use_container_width=True)

    # Display the updated DataFrame
    st.write("Full Table with Spend in USD:")
    st.dataframe(us_df, use_container_width=True)

    # Display the updated DataFrame
    st.write("Updated Table with Spend in USD:")
    st.dataframe(us_grouped_data_adacclevel, use_container_width=True)

    st.write('Day level spends')
    us_grouped_data = us_df.groupby(['dt'])['spend_in_usd'].sum().reset_index().sort_values(by='dt', ascending=False)
    st.dataframe(us_grouped_data, use_container_width=True)

    st.line_chart(us_grouped_data, x='dt', y='spend_in_usd')


# if selected == "Revenue-Analysis" and st.session_state.status == "verified":

#     st.dataframe(sub_df, use_container_width=True)

#     # Streamlit App
#     st.title("Ad Subscription Dashboard")

#     # Currency Filter
#     currency_option = st.selectbox("Select Currency", ["All", "India", "US"])
#     if currency_option == "India":
#         currency_filter = "INR"
#         filtered_df = sub_df[sub_df['currency'] == currency_filter].reset_index(drop=True)
#     elif currency_option == "US":
#         currency_filter = "INR"
#         filtered_df = sub_df[sub_df['currency'] != currency_filter]
#     else:
#         filtered_df = sub_df  # Show all data if "All" is selected

#     # Key Metrics Display
#     st.header("Key Metrics")

#     # Arrange key metrics in columns for better layout
#     col1, col2, col3, col4 = st.columns(4)

#     # Calculations for metrics
#     total_accounts = filtered_df['ad_account_id'].nunique()
#     total_subscription_amount = filtered_df['plan_amount'].sum()
#     avg_plan_amount = filtered_df['plan_amount'].mean()
#     avg_utilization = filtered_df['actual_td_util'].mean()
#     total_spend = filtered_df['actual_td_spend'].sum()
#     expected_spend = filtered_df['expected_td_spend'].sum()

#     # Display metrics in columns
#     col1.metric("Total Accounts", total_accounts)
#     col2.metric("Total Subscription Amount", f"{total_subscription_amount:,.2f} {currency_option}")
#     col3.metric("Average Plan Amount", f"{avg_plan_amount:,.2f} {currency_option}")
#     col4.metric("Average Utilization (%)", f"{avg_utilization:.2f}%")

#     col1.metric("Total Spend", f"{total_spend:,.2f} {currency_option}")
#     col2.metric("Expected Spend", f"{expected_spend:,.2f} {currency_option}")

#     # Define filter categories
#     no_adspends = filtered_df[filtered_df['adspends_added'] == 0].reset_index(drop=True)
#     need_attention = filtered_df[filtered_df['actual_td_util'] < 30].reset_index(drop=True)
#     potential_upgrade = filtered_df[filtered_df['actual_td_util'] > 70].reset_index(drop=True)
#     upcoming_renewals = filtered_df[filtered_df['sub_end'] <= date.today() + timedelta(days=7)].reset_index(drop=True)

#     #All the active accounts
#     st.subheader("Active Subscriptions")
#     st.metric("Number of Accounts", filtered_df.shape[0])
#     st.dataframe(filtered_df)

#     # Display the number of accounts per category with metrics
#     st.header("Account Divisions")

#     # Categories metrics display
#     st.subheader("Subscription with No Adspends")
#     st.metric("Number of Accounts", no_adspends.shape[0])
#     st.dataframe(no_adspends)

#     st.subheader("Ad Accounts that Need Attention")
#     st.metric("Number of Accounts", need_attention.shape[0])
#     st.dataframe(need_attention)

#     st.subheader("Potential Upgrades")
#     st.metric("Number of Accounts", potential_upgrade.shape[0])
#     st.dataframe(potential_upgrade)

#     st.subheader("Upcoming Renewals")
#     st.metric("Number of Accounts", upcoming_renewals.shape[0])
#     st.dataframe(upcoming_renewals)


if selected == "Euid - adaccount mapping" and st.session_state.status == "verified":

    st.title("Euid - adaccount mapping")
    st.dataframe(list_df, use_container_width=True)

    euid = st.number_input("Type an euid")

    filtered_list_df = list_df[list_df['euid'] == euid]
    st.dataframe(filtered_list_df, use_container_width=True)

if selected == "Top accounts" and st.session_state.status == "verified":
        
    
    # Streamlit App
    st.title("Top 10 Businesses by Spend")

    # Currency Filter
    currency_option = st.selectbox("Select Currency", ["All", "INR", "USD"])
    if currency_option != "All":
        filtered_df = spends_df[spends_df['currency'] == currency_option]
    else:
        filtered_df = spends_df

    # Date Range Selection
    time_frame = st.selectbox("Select Time Frame", ["Last 30 Days", "Last 90 Days", "Overall", "Custom Date Range"])

    #Top Numbers
    n = st.number_input("Number of records to display", min_value=1, value=10)

    if time_frame == "Last 30 Days":
        start_date = datetime.now() - timedelta(days=30)
        end_date = datetime.now()
    elif time_frame == "Last 90 Days":
        start_date = datetime.now() - timedelta(days=90)
        end_date = datetime.now()
    elif time_frame == "Overall":
        start_date = filtered_df['dt'].min()
        end_date = filtered_df['dt'].max()
    else:  # Custom Date Range
        start_date = st.date_input("Start Date", value=datetime.now() - timedelta(days=30))
        end_date = st.date_input("End Date", value=datetime.now())

    # Filter DataFrame by selected date range
    filtered_df = filtered_df[(filtered_df['dt'] >= pd.to_datetime(start_date)) & (filtered_df['dt'] <= pd.to_datetime(end_date))]

    # Aggregate spend per business and get the top 10
    top_spenders = (
        filtered_df.groupby([ "ad_account_id"])["spend"]
        .sum()
        .reset_index()
        .sort_values(by="spend", ascending=False)
        .head(n)
    )

    filtered_df = filtered_df[['euid', 'ad_account_id', 'ad_account_name']]

    top_businesses = pd.merge(top_spenders, filtered_df, on="ad_account_id", how="left") \
                   .drop_duplicates(subset="ad_account_id") \
                   .sort_values(by="spend", ascending=False).reset_index(drop=True)

    top_businesses = top_businesses[['euid', 'ad_account_id', 'ad_account_name', 'spend']]

    # Display top 10 businesses
    st.header("Top 10 Businesses by Spend")
    st.write(f"Showing data from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
    st.dataframe(top_businesses, use_container_width=True)