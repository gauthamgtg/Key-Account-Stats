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
SELECT name,ad_account_id,currency,amount_due FROM zocket_global.fb_child_ad_accounts where amount_due>0 order by amount_due desc
'''


# @st.cache_data(ttl=36400)  # 86400 seconds = 24 hours
@redshift_connection(db,name,passw,server,port)
def execute_query(connection, cursor,query):

    cursor.execute(query)
    column_names = [desc[0] for desc in cursor.description]
    result = pd.DataFrame(cursor.fetchall(), columns=column_names)

    return result

# df = execute_query(query=query)


st.title('FB Balance')



# --- Proper Session State Authentication and Flow for Data Download ---

# Ensure session state for tracking authentication
if "authenticated" not in st.session_state:
    st.session_state["authenticated"] = False
if "auth_error" not in st.session_state:
    st.session_state["auth_error"] = ""

def check_password():
    input_pass = st.session_state.get("input_password", "")
    if input_pass == st.secrets.password:
        st.session_state["authenticated"] = True
        st.session_state["auth_error"] = ""
    else:
        st.session_state["authenticated"] = False
        st.session_state["auth_error"] = "Incorrect password. Please try again."
        st.session_state["input_password"] = ""  # clear input for retry

if not st.session_state["authenticated"]:
    st.text_input("Enter password:", key="input_password", type="password", on_change=check_password)
    if st.session_state["auth_error"]:
        st.error(st.session_state["auth_error"])
    else:
        st.info("Please enter password to access data.")
else:
    st.success("Password correct! You can now download data.")
    if "fb_child_ad_accounts_df" not in st.session_state:
        df = execute_query(query=query)
        st.session_state["fb_child_ad_accounts_df"] = df
    else:
        df = st.session_state["fb_child_ad_accounts_df"]

    st.dataframe(df)

    csv = df.to_csv(index=False).encode("utf-8")
    st.download_button(
        label="Export to CSV",
        data=csv,
        file_name="fb_child_ad_accounts.csv",
        mime="text/csv"
    )
    if st.button("Log out"):
        st.session_state["authenticated"] = False
        st.session_state["input_password"] = ""
        st.experimental_rerun()