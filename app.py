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


db=st.secrets["db"]
name=st.secrets["name"]
passw=st.secrets["passw"]
server=st.secrets["server"]
port=st.secrets["port"]
st.set_page_config( page_title = "Spend Stats",
    page_icon=":bar_chart:",
    layout="wide",
    initial_sidebar_state="expanded")

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
        when eu.euid in (2201,2168,2202,2181,2051,2100,2310,2309,2281) then 'FB Boost'
        when eu.euid in (1911)then 'Adfly' 
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
@st.cache_data
@redshift_connection(db,name,passw,server,port)
def execute_query(connection, cursor,query):

    cursor.execute(query)
    column_names = [desc[0] for desc in cursor.description]
    result = pd.DataFrame(cursor.fetchall(), columns=column_names)

    return result

df = execute_query(query=query)

#chaning proper format of date
df['dt'] = pd.to_datetime(df['dt']).dt.date

#changing spend to numeric
df['spend'] = pd.to_numeric(df['spend'], errors='coerce')
df['euid'] = pd.to_numeric(df['euid'], errors='coerce')
grouped_data_adacclevel = None
pivoted_data_adacclevel = None
# Calculate yesterday and day before yesterday's dates
yesterday = (date.today() - timedelta(days=1))
day_before_yst = (date.today() - timedelta(days=2))
current_month = datetime.now().month
current_year = datetime.now().year


with st.sidebar:
    selected = option_menu(
        menu_title="Navigation",  # Required
        options=["Login","Main Page", "Raw Data","Overall Stats - Ind","Overall Stats - US"],  # Required
        icons=["lock","house", "table","currency-rupee",'currency-dollar'],  # Optional: icons from the Bootstrap library
        menu_icon="cast",  # Optional: main menu icon
        default_index=0,  # Default active menu item
    )


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




if selected == "Main Page" and st.session_state.status == "verified":
    st.title("Key Account Stats")
    st.write("Show detailed spends of top customers.")

    col1,col2 = st.columns(2)

    with col1:
    # Step 1: Ask the customer to choose a top_customers_flag
        flag_options = df['top_customers_flag'].unique()
        selected_flag = st.selectbox('Choose Top Customers Flag', flag_options, index=1)

    with col2:
    # Filter the dataframe based on the selected top_customers_flag
        filtered_df = df[df['top_customers_flag'] == selected_flag]

        # Step 2: Ask the customer to choose grouping (year, month, week, or date)
        grouping = st.selectbox('Choose Grouping', ['Year', 'Month', 'Week', 'Date'], index=1)


    col1, col2, col3 = st.columns(3)
    col1.metric("Total Spend",filtered_df['spend'].sum())
    col2.subheader("Start Date : " + str(filtered_df['dt'].min()))
    col3.subheader("Last Active Date : " + str(filtered_df['dt'].max()))

   

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


    # Aggregate the spend values by the selected grouping
    grouped_data_adacclevel = filtered_df.groupby(['euid','ad_account_id','grouped_date'])['spend'].sum().reset_index()
    pivoted_data_adacclevel = grouped_data_adacclevel.pivot(columns='grouped_date', index=['euid','ad_account_id'], values='spend')
    st.dataframe(pivoted_data_adacclevel, use_container_width=True)

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
    total_current_month_spend = ind_current_month_df['spend'].sum()

    # Display the metrics
    col1, col2, col3, col4 = st.columns(4)

    # Metric 1: Yesterday's Spend and change %
    col1.metric("Yesterday Spend", f"₹{ind_yst_spend}", f"{ind_spend_change}%")

    # Metric 2: Number of Ad Accounts and change %
    col2.metric("Ad Accounts", ind_num_ad_accounts_yesterday, f"{ind_ad_account_change}%")

    # Metric 3: Average Spend per Ad Account and change %
    col3.metric("Avg Spend per Ad Account", f"₹{round(ind_avg_spend_per_account_yesterday, 2)}", f"{avg_ind_spend_change}%")

    # Display the current month spend as a metric
    col4.metric(label="Current Month Spend", value=f"₹{total_current_month_spend:,.2f}")


    st.write("Yesterday spend data:")
    st.dataframe(indian_df[indian_df['dt']==yesterday], use_container_width=True)

    st.write("Current Month spend data:")
    st.dataframe(ind_current_month_df, use_container_width=True)


    st.dataframe(indian_df, use_container_width=True)
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