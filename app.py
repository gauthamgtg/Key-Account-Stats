from curses.ascii import alt
from urllib.error import URLError
import pandas as pd
import numpy as np
import streamlit as st
import matplotlib.pyplot as plt
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
    (SELECT cast(aas.euid as float) as euid ,ad_account_id,date(date_start) as dt,sum(spend)spend
    from  ad_account_spends aas 
    group by 1,2,3),
    total_payment AS
        (select cast(td.euid as float)euid,ad_account,sum(adspend_amount) total_paid 
        from payment_trans_details td
        group by 1,2)

select eu.euid,eu.business_name,eu.company_name,dt,efaa.ad_account_name,
efaa.flag as currency_code,a.ad_account_id,total_spent,total_paid,spend,curr_month_spend,
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
    left join enterprise_facebook_account efa on efa.euid = eu.euid 
    left join 
    (
    select ad_account_id,ad_account_name,
        CASE WHEN currency IS NULL OR LOWER(currency) = 'inr' THEN 'India' 
        ELSE 'US' 
        END AS flag,
    max(expiry_date) as expiry_date,max(spend_cap) as spend_cap 
    from enterprise_facebook_ad_account
    group by 1,2,3
    )efaa
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


with st.sidebar:
    selected = option_menu(
        menu_title="Navigation",  # Required
        options=["Login","Main Page", "Raw Data"],  # Required
        icons=["lock","house", "table"],  # Optional: icons from the Bootstrap library
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
