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

sub_query = '''
SELECT * FROM
(
SELECT euid,ad_account_id,ad_account_name,sub_start, sub_end, 
currency, plan_amount,plan_limit,flag,total_subscription_days, subscription_days_completed,adspends_added,
ROUND(expected_per_day_spend, 2) AS expected_per_day_spend,
ROUND(expected_per_day_spend * subscription_days_completed, 2) AS expected_TD_spend,
adspends_added AS actual_TD_spend,
-- round((cast(subscription_days_completed as float)/ total_subscription_days)*100,2) AS expected_TD_util,
case when subscription_days_completed =0 then 0 else ROUND((adspends_added / (expected_per_day_spend * subscription_days_completed)) * 100, 2) end AS actual_TD_util,
case when subscription_days_completed =0 then 0 else ROUND(((expected_per_day_spend * subscription_days_completed) / plan_limit) * 100, 2) end AS expected_util,
ROUND((adspends_added / plan_limit) * 100, 2) AS overall_util,
row_number() over(partition by ad_account_id ORDER by sub_start desc) as rw

FROM
(
SELECT  euid,ad_account_id,ad_account_name,created_at as sub_start,expiry_date as sub_end, 
currency, plan_amount,REPLACE(plan_limit, ',', '') as plan_limit,flag,
datediff('day',sub_start,expiry_date) as total_subscription_days,
(datediff('day',sub_start,current_date)) as subscription_days_completed,
case when datediff('day',sub_start,expiry_date)=0 then 0 else (REPLACE(plan_limit, ',', '')/datediff('day',sub_start,expiry_date)) end as expected_per_day_spend,
(adspend_added+transfer_amount) as adspends_added
FROM
(
SELECT  a.euid,a.ad_account_id,ad_account_name,a.created_at,a.expiry_date, currency,
usd_amount as plan_amount,
case when currency='INR' then price_range else usd_price_range end as plan_limit,transfer_amount,flag, sum(adspend_added) as adspend_added

FROM
(
SELECT  a.euid,a.ad_account_id,a.created_at,a.expiry_date,a.amount as subamt,sp.plan_id,
 faa.name as ad_account_name,
faa.currency as currency,
 sp.plan_name, sp.usd_amount,sp.usd_price_range,sp.price_range, transfer_amount,flag
FROM
(
select distinct payment_id, euid, 
case when ad_account_id = 'act_1735955147217127' then '1735955147217127' else ad_account_id end as ad_account_id,
case when transfer_euid is not null then 'Transfered' else 'New' end as flag,
(transfer_amount)transfer_amount,tranferred_at,
date(usd.created_at) created_at, expiry_date,amount,plan_id
from user_subscription_data usd 
where  expiry_date >= current_date and id!=1952
order by 3
)a
left join 
fb_ad_accounts faa on concat('act_',a.ad_account_id) = faa.ad_account_id
left join subscription_plan sp on a.plan_id = sp.plan_id
)a
left JOIN
(
    SELECT euid,ad_account,date(payment_date) as payment_date,sum(adspend_amount) as adspend_added
    from payment_trans_details
    GROUP by 1,2,3
) as adspends 
on a.euid=adspends.euid and a.ad_account_id=adspends.ad_account and adspends.payment_date>=a.created_at
group by 1,2,3,4,5,6,7,8,9,10

)
order by 4
)
)
where 
euid not in (701,39)
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

#adlevel query
zocket_ai_campaigns_spends_query='''
select ggci.ad_account_id,ggci.currency,date(date_start) as dt,account_name,gc.campaign_id as campaign_id,c.name as campaign_name,
case when date(bu.created_at) >= current_date - 30 then 'Onboarded <30d ago' else 'Onboarded >30d ago' end as User_flag,buid,bu.email,
SUM(ggci.spend)spend
FROM
    zocket_global.campaigns c
    join 
    (SELECT
    id ,name,brand_name,json_extract_path_text(json_extract_array_element_text(business_user_ids, 0), 'role') AS role,
    json_extract_path_text(json_extract_array_element_text(business_user_ids, 0), 'business_user_id') AS buid
FROM
    zocket_global.business_profile
WHERE
    json_extract_path_text(json_extract_array_element_text(business_user_ids, 0), 'role') = 'owner' )bp on c.business_id=bp.id
    join zocket_global.business_users bu on bp.buid = bu.id
    join zocket_global.fb_campaigns gc on gc.app_campaign_id = c.id 
    join zocket_global.fb_adsets fbadset on gc.id = fbadset.campaign_id
    join zocket_global.fb_ads fbads on fbadset.id = fbads.adset_id
    join zocket_global.fb_ads_age_gender_metrics_v3 ggci on ggci.ad_id = fbads.ad_id
where date(date_start)>='2024-01-01' and date(date_start)<current_date
and c.imported_at is null
group by 1,2,3,4,5,6,7,8,9
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

bp_buid_query = '''

SELECT
    id as bid,json_extract_path_text(json_extract_array_element_text(business_user_ids, 0), 'role') AS role,
    json_extract_path_text(json_extract_array_element_text(business_user_ids, 0), 'business_user_id') AS buid
FROM
    zocket_global.business_profile
WHERE
    json_extract_path_text(json_extract_array_element_text(business_user_ids, 0), 'role') = 'owner'
    
    '''

overages_query = '''
SELECT case when b.business_user_id is null then 'Normal' else 'PM Model' end as Flag,a.* from zocket_global.wallet_payment_transactions a
left join
(SELECT DISTINCT
business_user_id
		FROM 
			zocket_global.subscription_products sp
		INNER JOIN 
			zocket_global.subscription_products_pricing spp ON sp.id = spp.product_id
		INNER JOIN 
			zocket_global.subscription_plan spn ON sp.id = spn.product_id
		INNER JOIN 
			zocket_global.user_subscriptions us ON spn.id = us.plan_id
WHERE sp.adspend_limit=0
)b
on a.user_id = b.business_user_id
where payment_date>='2025-01-01'
'''

payments_query = '''

SELECT * from zocket_global.partner_payment_transactions

;'''

us_finance_all_transactions_query = '''

SELECT payment_transcation_id, cast(business_user_id as VARCHAR)buid,cast(business_id as VARCHAR)bid,'zocketai sub' as flag,'usd' as currency,start_date,end_date,amount,0 as gateway_charge,0 as processing_fee,0 as tax,0 as convenience_fee 
from
(
select us.id,	business_id,	business_user_id,	us.plan_id	,amount	,start_date	,end_date	,is_free_trial	,subscription_id	,subscription_schedule_id	,subscription_status	,cancelled_at	,cancel_at,	status	,source	,payment_transaction_id	,invoice_id	,renewed_at	,renewed_subscription_id	,downgraded_at	,downgraded_to_plan_id	,upgraded_at	,upgraded_to_plan_id	,deleted_at	,us.created_at	,us.updated_at	,free_trial_days	,initial_start_date	,user_enabled_custom_plan_id	,is_unlimited	, pt.payment_transcation_id
from zocket_global.user_subscriptions us 
inner join zocket_global.payment_transactions pt on us.payment_transaction_id = pt.id
where us.source = 'STRIPE' 
)

union all

SELECT payment_id,cast(euid as VARCHAR)euid,cast(ad_account_id as varchar)adacc,'enterprise sub' as flag,'usd' as currency,start_date,end_date,usd_amount as amount,0 as gateway_charge,0 as processing_fee,0 as tax,0 as convenience_fee 
from
(
SELECT payment_id,euid,ad_account_id,transfer_ad_account_id,transfer_euid,transfer_amount,invoice_id,tranferred_at,
'enterprise' as flag,sp.amount as sp_amount,usd.amount as usd_amount,usd.created_at as start_date,expiry_date as end_date, sp.usd_price_range
 from user_subscription_data usd
left join subscription_plan sp on usd.plan_id = sp.plan_id
where payment_id like '%pi%' or payment_id like '%cs_live%'
)
union all


SELECT receiver_id,buid::varchar,business_id::varchar,'adspends' as flag,currency,'01/01/2001' as start_date,'01/01/2001' as end_date,adspend_amount, gateway_charge,processing_fee,tax,convenience_fee
 from
(
SELECT euid as buid,euid as business_id,ad_account,date(payment_date)as dt, total_amount,receiver_id,currency,
gateway_charge,adspend_amount,processing_fee,tax,convenience_fee,'enterprise' as flag
from payment_trans_details
where lower(currency)!='inr'

union all

SELECT user_id as buid,business_id,ad_account_id,date(created_at)as dt, total_amount,payment_id as receiver_id,currency,
gateway_processing_fee as gateway_charge,final_adspend_amount as adspend_amount,overage_fee as processing_fee,tax,
0 as convenience_fee,'zocket.ai' as flag
FROM
(
SELECT 
                        poa.id, 
                        poa.total_amount, 
                        poa.tax,
                        poa.overage_fee,
                        DATE(DATEADD(minute, 330, poa.created_at)) AS created_at, 
                        pt.gateway_processing_fee, 
                        poa.ad_account_id, 
                        poa.business_id,
                        poa.user_id,
                        poa.final_adspend_amount, 
                        pt.currency,
                        poa.api_status,
                        DATE(DATEADD(minute, 330, pt.payment_date)) AS payment_date,
                        bu.email,
                        bu.name,
                        faa.name,
                        pt.payment_id
                FROM zocket_global.wallet_payment_order_adspend poa
                INNER JOIN zocket_global.wallet_payment_transactions pt ON poa.Payment_transaction_id = pt.id
                JOIN zocket_global.business_users bu on bu.id = poa.user_id
                JOIN zocket_global.fb_ad_accounts faa on faa.ad_account_id = concat('act_', poa.ad_account_id)
                WHERE poa.status = '1'
                ORDER BY poa.id DESC 
)
union all
select coalesce(p.platform_user_id,zocket_user_id) as buid,0 as bid,'master wallet' as ad_account_id,date(a.created_at)as dt, amount,payment_id as receiver_id,a.currency,
gateway_processing_fee as gateway_charge,amount - gateway_processing_fee as adspend_amount,0 as processing_fee,tax,
0 as convenience_fee,'Adspends' as flag,bu.name, bu.mobile, bu.email, bu.city, bu.state,bu.country, 'zocket.ai' as gst_number
from zocket_global.partner_payment_transactions a
left join zocket_global.business_users bu on a.zocket_user_id = bu.id
left join zocket_global.partners p on a.partner_id = p.id
)
;'''

finance_all_trxn_query = '''SELECT * FROM
(
SELECT payment_transcation_id, cast(business_user_id as VARCHAR)buid,cast(business_id as VARCHAR)bid,'Subscription' as flag,'usd' as currency,start_date,end_date,amount,0 as gateway_charge,0 as processing_fee,0 as tax,0 as convenience_fee, bu.name, bu.mobile, bu.email, bu.city, bu.state,bu.country, bu.gst_number as gst_number
from
(
select us.id,	business_id,	business_user_id,	us.plan_id	,amount	,start_date	,end_date	,is_free_trial	,subscription_id	,subscription_schedule_id	,subscription_status	,cancelled_at	,cancel_at,	status	,source	,payment_transaction_id	,invoice_id	,renewed_at	,renewed_subscription_id	,downgraded_at	,downgraded_to_plan_id	,upgraded_at	,upgraded_to_plan_id	,deleted_at	,us.created_at	,us.updated_at	,free_trial_days	,initial_start_date	,user_enabled_custom_plan_id	,is_unlimited	, pt.payment_transcation_id
from zocket_global.user_subscriptions us 
inner join zocket_global.payment_transactions pt on us.payment_transaction_id = pt.id
) a
left join zocket_global.business_users bu on a.business_user_id = bu.id

union all

SELECT payment_id,cast(a.euid as VARCHAR)euid,cast(ad_account_id as varchar)adacc,'Subscription' as flag,'usd' as currency,start_date,end_date,usd_amount as amount,0 as gateway_charge,0 as processing_fee,0 as tax,0 as convenience_fee ,bu.business_name, bu.mobile, bu.email, bu.city, bu.state,bu.country_code,gst_number as gst_number
from
(
SELECT payment_id,euid,ad_account_id,transfer_ad_account_id,transfer_euid,transfer_amount,invoice_id,tranferred_at,
'Subscription' as flag,sp.amount as sp_amount,usd.amount as usd_amount,usd.created_at as start_date,expiry_date as end_date, sp.usd_price_range
 from user_subscription_data usd
left join subscription_plan sp on usd.plan_id = sp.plan_id
) a
left join enterprise_users bu on a.euid = bu.euid
union all


SELECT receiver_id,buid::varchar,business_id::varchar,'Adspends' as flag,currency,'01/01/2001' as start_date,'01/01/2001' as end_date,adspend_amount, gateway_charge,processing_fee,tax,convenience_fee,business_name, mobile, email, city, state, country_code,gst_number as gst_number
 from
(
SELECT a.euid as buid,a.euid as business_id,ad_account,date(payment_date)as dt, total_amount,receiver_id,currency,
gateway_charge,adspend_amount,processing_fee,tax,convenience_fee,'Adspends' as flag,bu.business_name, bu.mobile, bu.email, bu.city, bu.state,bu.country_code,gst_number as gst_number
from payment_trans_details a
left join enterprise_users bu on a.euid = bu.euid 

union all

SELECT user_id as buid,business_id,ad_account_id,date(a.created_at)as dt, total_amount,payment_id as receiver_id,currency,
gateway_processing_fee as gateway_charge,final_adspend_amount as adspend_amount,overage_fee as processing_fee,tax,
0 as convenience_fee,'Adspends' as flag, bu.name, bu.mobile, bu.email, bu.city, bu.state,bu.country, bu.gst_number as gst_number
FROM
(
SELECT 
                        poa.id, 
                        poa.total_amount, 
                        poa.tax,
                        poa.overage_fee,
                        DATE(DATEADD(minute, 330, poa.created_at)) AS created_at, 
                        pt.gateway_processing_fee, 
                        poa.ad_account_id, 
                        poa.business_id,
                        poa.user_id,
                        poa.final_adspend_amount, 
                        pt.currency,
                        poa.api_status,
                        DATE(DATEADD(minute, 330, pt.payment_date)) AS payment_date,
                        bu.email,
                        bu.name,
                        faa.name,
                        pt.payment_id
                FROM zocket_global.wallet_payment_order_adspend poa
                INNER JOIN zocket_global.wallet_payment_transactions pt ON poa.Payment_transaction_id = pt.id
                JOIN zocket_global.business_users bu on bu.id = poa.user_id
                JOIN zocket_global.fb_ad_accounts faa on faa.ad_account_id = concat('act_', poa.ad_account_id)
                WHERE poa.status = '1'
                ORDER BY poa.id DESC 
)a
left join zocket_global.business_users bu on a.user_id = bu.id

union all
select coalesce(p.platform_user_id,zocket_user_id) as buid,0 as bid,'master wallet' as ad_account_id,date(a.created_at)as dt, amount,payment_id as receiver_id,a.currency,
gateway_processing_fee as gateway_charge,amount - gateway_processing_fee as adspend_amount,0 as processing_fee,tax,
0 as convenience_fee,'Adspends' as flag,bu.name, bu.mobile, bu.email, bu.city, bu.state,bu.country, bu.gst_number as gst_number
from zocket_global.partner_payment_transactions a
left join zocket_global.business_users bu on a.zocket_user_id = bu.id
left join zocket_global.partners p on a.partner_id = p.id
)
)
'''

# datong_api_query='''

# SELECT (euid::float)euid,ad_account_name,aas.ad_account_id,currency_code,aas.dt,spend,total_spend
# ,(spend/total_spend)*100 as per
# from
# (select euid,date(date_start)dt,ad_account_id,spend as total_spend from ad_account_spends )aas
# left join 
# (
# select
# ggci.ad_account_id,ggci.currency as currency_code,date(date_start) as dt,account_name as ad_account_name,SUM(ggci.spend)spend
# FROM
#     zocket_global.campaigns c
#     join zocket_global.fb_campaigns gc on gc.app_campaign_id = c.id 
#     join zocket_global.fb_adsets fbadset on gc.id = fbadset.campaign_id
#     join zocket_global.fb_ads fbads on fbadset.id = fbads.adset_id
#     join zocket_global.fb_ads_age_gender_metrics_v3 ggci on ggci.ad_id = fbads.ad_id
# where date(date_start)>='2024-01-01'
# and c.imported_at is null
# group by 1,2,3,4
# )ai
# on aas.ad_account_id=ai.ad_account_id AND date(aas.dt)=date(ai.dt)
# where aas.ad_account_id in (
#     'act_517235807318296',
#         'act_331025229860027',
#         'act_1026427545158424',
#         'act_818603109556933',
#         'act_245995025197404',
#         'act_3592100964402439',
#         'act_3172162799744723',
#         'act_1980162379033639',
#         'act_1364907264123936',
#         'act_749694046972238',
#         'act_1841833786300802',
#         'act_206144919151515',
#         'act_324812700362567',
#         'act_3505294363025995',
#         'act_7780020542024454',
#         'act_650302000225354',
#         'act_1769761460112751',
#         'act_659696249436257',
#         'act_1729204737559911',
#         'act_383479978116390',
#         'act_1729204737559911'
# )
# '''



@st.cache_data(ttl=36400)  # 86400 seconds = 24 hours
@redshift_connection(db,name,passw,server,port)
def execute_query(connection, cursor,query):

    cursor.execute(query)
    column_names = [desc[0] for desc in cursor.description]
    result = pd.DataFrame(cursor.fetchall(), columns=column_names)

    return result

# df = execute_query(query=query)
df = execute_query(query=query)
sub_df = execute_query(query=sub_query)
list_df = execute_query(query=list_query)
# top_spends_df = execute_query(query=top_spends_query)
# ai_spends_df = execute_query(query=ai_spends_query)
ai_campaign_spends_df = execute_query(query=zocket_ai_campaigns_spends_query)
disabled_account_df = execute_query(query=disabled_account_query)
bid_buid_df = execute_query(query=bp_buid_query)
# datong_api_df = execute_query(query=datong_api_query)
overages_df = execute_query(query=overages_query) 
payments_df = execute_query(query=payments_query)

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

#Revenue analysis query

sub_df['euid'] = pd.to_numeric(sub_df['euid'], errors='coerce')
sub_df['plan_amount'] = pd.to_numeric(sub_df['plan_amount'], errors='coerce')
# sub_df['ad_account_id'] = pd.to_numeric(sub_df['ad_account_id'], errors='coerce')
sub_df['sub_start'] = pd.to_datetime(sub_df['sub_start']).dt.date
sub_df['sub_end'] = pd.to_datetime(sub_df['sub_end']).dt.date
sub_df['total_subscription_days'] = pd.to_numeric(sub_df['total_subscription_days'], errors='coerce')
sub_df['subscription_days_completed'] = pd.to_numeric(sub_df['subscription_days_completed'], errors='coerce')
sub_df['adspends_added'].fillna(0, inplace=True)
sub_df['adspends_added'] = pd.to_numeric(sub_df['adspends_added'], errors='coerce')
sub_df['expected_per_day_spend'] = pd.to_numeric(sub_df['expected_per_day_spend'], errors='coerce')
sub_df['expected_td_spend'] = pd.to_numeric(sub_df['expected_td_spend'], errors='coerce')
sub_df['actual_td_spend'] = pd.to_numeric(sub_df['actual_td_spend'], errors='coerce')
sub_df['actual_td_util'] = pd.to_numeric(sub_df['actual_td_util'], errors='coerce')
sub_df['expected_util'] = pd.to_numeric(sub_df['expected_util'], errors='coerce')
sub_df['overall_util'] = pd.to_numeric(sub_df['overall_util'], errors='coerce')
sub_df['rw'] = pd.to_numeric(sub_df['rw'], errors='coerce')

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
        options=["Login","Key Account Stats","Overall Stats - Ind","Overall Stats - US","Euid - adaccount mapping","BID - BUID Mapping","Top accounts","FB API Campaign spends","Disabled Ad Accounts","Summary","BM Summary","Stripe lookup","Ad360 Subscription-Analysis", "Raw Data","Overages","US Finance Mappings","Overall Finance Mappings"],  # Required
        icons=["lock","airplane-engines","currency-rupee",'currency-dollar','link',"link-45deg","graph-up","suit-spade","slash-circle","book","book-fill","bi-stripe","dice-6-fill", "table","bi-coin","bi-safe2-fill","bi-piggy-bank"],  # Optional: icons from the Bootstrap library
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

if selected == "Key Account Stats" and st.session_state.status == "verified":

    # ad_acc = st.text_input("Enter ad acc :")

    # st.dataframe(df[df['ad_account_id'] == ad_acc], use_container_width=True)

    st.title("Key Account Stats")
    st.write("Show detailed spends of top customers.")

    col1,col2 = st.columns(2)

    with col1:
    # Step 1: Ask the customer to choose a top_customers_flag
        flag_options = df['top_customers_flag'].unique()
        selected_flag = st.selectbox('Choose Top Customers Flag', flag_options, index=1)
        if selected_flag == "Others":
            euids = st.text_input("Enter euids (comma separated):",value = "744")
            euids = [int(x) for x in euids.split(",")]
            

    with col2:

        # Step 2: Ask the customer to choose grouping (year, month, week, or date)
        grouping = st.selectbox('Choose Grouping', ['Year', 'Month', 'Week', 'Date'], index=1)

    df = df.merge(disabled_account_df[['ad_account_id', 'flag']], on='ad_account_id', how='left')

    df['flag'] = df['flag'].fillna('Active')

    # Filter the dataframe based on the selected top_customers_flag
    if selected_flag == "Others":
        filtered_df = df[df['euid'].isin(euids)]
    else:
        filtered_df = df[df['top_customers_flag'] == selected_flag]

    # st.dataframe(filtered_df, use_container_width=True)

    col1, col2, col3 = st.columns(3)
    col1.metric("Total Spend",filtered_df['spend'].sum().round().astype(int))
    col2.subheader("Start Date : " + str(filtered_df['dt'].min()))
    col3.subheader("Last Active Date : " + str(filtered_df['dt'].max()))

    # Filter data for yesterday and day before yesterday
    yesterday_data = filtered_df[filtered_df['dt'] == yesterday]
    day_before_yst_data = filtered_df[filtered_df['dt'] == day_before_yst]
    if current_month == 1:
        last_month_df = filtered_df[(pd.to_datetime(filtered_df['dt']).dt.month == 12) & (pd.to_datetime(filtered_df['dt']).dt.year == current_year-1)]
    else:
        last_month_df = filtered_df[(pd.to_datetime(filtered_df['dt']).dt.month == current_month-1) & (pd.to_datetime(filtered_df['dt']).dt.year == current_year)]

    # Calculate the total spend for each day
    yst_spend = yesterday_data['spend'].sum().round().astype(int)
    day_before_yst_spend = day_before_yst_data['spend'].sum().round().astype(int)
    last_month_spend = last_month_df['spend'].sum().round().astype(int)

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

    #display pivot table
    st.header(f"Spend Data Ad Account Level- {grouping}")
    grouped_df = filtered_df.groupby(['euid','ad_account_id','ad_account_name','business_manager_name','grouped_date','flag','Media_Buyer'])[['spend']].sum().reset_index()
    pivot_df = grouped_df.pivot(index=['euid','ad_account_id','ad_account_name','business_manager_name','flag','Media_Buyer'], columns='grouped_date', values='spend')
        # Sort the columns by date
    if grouping == 'Year':
        pivot_df = pivot_df[sorted(pivot_df.columns, key=lambda x: pd.to_datetime(x, format='%Y'), reverse=True)]
    elif grouping == 'Month':
        pivot_df = pivot_df[sorted(pivot_df.columns, key=lambda x: pd.to_datetime(x, format='%b-%y'), reverse=True)]
    elif grouping == 'Week':
        pivot_df = pivot_df[sorted(pivot_df.columns, key=lambda x: (int(x.split(' - week ')[0]), int(x.split(' - week ')[1])), reverse=True)]
    else:  # Date
        pivot_df = pivot_df[sorted(pivot_df.columns, key=lambda x: pd.to_datetime(x), reverse=True)]

    st.dataframe(pivot_df, use_container_width=True)


    yst_stats_df = filtered_df[filtered_df['dt'] == yesterday].groupby(['euid','ad_account_id','ad_account_name','business_manager_name','flag','Media_Buyer'], as_index=False)['spend'].sum().sort_values(by='spend', ascending=False).reset_index(drop=True)
    yst_stats_df.index +=1
    current_month_stats_df = filtered_df[filtered_df['dt'].apply(lambda x: x.month == current_month and x.year == current_year)].groupby(['euid','ad_account_id','ad_account_name','business_manager_name','flag','Media_Buyer'], as_index=False)['spend'].sum().sort_values(by='spend', ascending=False).reset_index(drop=True)
    current_month_stats_df.index +=1
    Overall_spend = filtered_df.groupby(['euid','ad_account_id','ad_account_name','business_manager_name','flag','Media_Buyer'], as_index=False)['spend'].sum().sort_values(by='spend', ascending=False).reset_index(drop=True)
    Overall_spend.index +=1
    
    st.write("Yesterday spend data:")
    st.dataframe(yst_stats_df, use_container_width=True)
    
    st.write("Current Month spend data:")
    st.dataframe(current_month_stats_df, use_container_width=True)

    st.write("Overall spend data:")
    st.dataframe(Overall_spend, use_container_width=True)

    summary_df = filtered_df
    summary_df.loc[:, 'month'] = filtered_df['dt'].apply(lambda x: x.strftime('%b-%y'))  # Month format as Jan-24
    # st.dataframe(summary_df, use_container_width=True)
    summary_df = summary_df.merge(yst_stats_df, on=['euid','ad_account_id'], how='left', suffixes=('', '_yesterday'))
    summary_df = summary_df.fillna(0)
    # st.dataframe(summary_df, use_container_width=True)
    summary_df = summary_df.merge(current_month_stats_df, on=['euid','ad_account_id'], how='left', suffixes=('', '_curr_month'))
    summary_df = summary_df.fillna(0)
    summary_df = summary_df.merge(Overall_spend, on=['euid','ad_account_id'], how='left', suffixes=('', '_total'))
    summary_df = summary_df.fillna(0)
    # print(summary_df.columns)
    # st.dataframe(summary_df, use_container_width=True)
    summary_df = summary_df.groupby(['euid','ad_account_id','business_name','company_name','flag','Media_Buyer','month','spend_yesterday','spend_curr_month','spend_total'])['spend'].sum().reset_index()
    summary_df = summary_df.pivot(index=['euid','ad_account_id','business_name','company_name','flag','Media_Buyer','spend_total','spend_curr_month','spend_yesterday'], columns='month', values='spend')


    # Sort the columns by date
    summary_df = summary_df[sorted(summary_df.columns, key=lambda x: pd.to_datetime(x, format='%b-%y'), reverse=True)]

    st.title("Spend Data Ad Account Level")
    st.dataframe(summary_df, use_container_width=True)


    summary_df = summary_df[sorted(summary_df.columns, key=lambda x: pd.to_datetime(x, format='%b-%y'), reverse=True)]

    st.header(f"Euid Level {grouping} Data")
    st.dataframe(filtered_df.groupby(['euid'])['spend'].sum(), use_container_width=True)

    #display overall data
    st.header(f"Overall {grouping} Data")
    st.dataframe(filtered_df.groupby(['grouped_date'])['spend'].sum(), use_container_width=True)

elif selected == "Overall Stats - Ind" and st.session_state.status == "verified":

    st.title("Overall Stats - India")

    indian_df = df[df['currency_code'].str.lower() == 'inr']

    # indian_df['dt'] = pd.to_datetime(indian_df['dt'])
    
    # Group the DataFrame by 'euid', 'ad_account_id', and 'dt', and sum 'spend'
    ind_grouped_data_adacclevel = indian_df.groupby(['euid', 'ad_account_id', 'dt','ad_account_name', 'currency_code', 'business_name', 'company_name'])['spend'].sum().reset_index()

    # Filter data for yesterday and day before yesterday
    ind_yesterday_data = ind_grouped_data_adacclevel[ind_grouped_data_adacclevel['dt'] == yesterday]
    ind_day_before_yst_data = ind_grouped_data_adacclevel[ind_grouped_data_adacclevel['dt'] == day_before_yst]

    # Calculate the total spend for each day
    ind_yst_spend = ind_yesterday_data['spend'].sum().round()
    ind_day_before_yst_spend = ind_day_before_yst_data['spend'].sum().round()

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
    
    # Filter the DataFrame to get the current month’s data
    ind_current_month_df = indian_df[pd.to_datetime(indian_df['dt']).dt.to_period('M') == pd.to_datetime('today').to_period('M')]
    ind_last_month_df = indian_df[pd.to_datetime(indian_df['dt']).dt.to_period('M') == (pd.to_datetime('today') - pd.DateOffset(months=1)).to_period('M')]

    # Calculate the total spend for the current month
    ind_current_month_spend = ind_current_month_df['spend'].sum()
    ind_last_month_spend = ind_last_month_df['spend'].sum()

    # Display the metrics
    col1, col2, col3, col4 = st.columns(4)

    # Metric 1: Yesterday's Spend and change %
    col1.metric("Yesterday Spend", f"₹{ind_yst_spend}", f"{ind_spend_change:,.2f}%")

    # Metric 2: Number of Ad Accounts and change %
    col2.metric("Active Ad Accounts Yesterday", ind_num_ad_accounts_yesterday, f"{ind_ad_account_change}%")

    # Metric 3: Average Spend per Ad Account and change %
    # col3.metric("Avg Spend per Ad Account yesterday", f"₹{round(ind_avg_spend_per_account_yesterday, 2)}", f"{avg_ind_spend_change}%")

    # Display the current month spend as a metric
    col3.metric(label="Current Month Spend", value=f"₹{ind_current_month_spend:}")

    # Metric 4: Last Month Spend and change %
    col4.metric("Last Month Spend", f"₹{ind_last_month_spend:,.2f}")

    #add a pivot 


    
    st.write("Date wise spend data:")
    ind_grouped_data_adacclevel['euid'] = pd.to_numeric(ind_grouped_data_adacclevel['euid'], errors='coerce')
    st.dataframe(
        ind_grouped_data_adacclevel.pivot(
            index=['euid', 'ad_account_id', 'ad_account_name', 'business_name', 'company_name', 'currency_code'], 
            columns='dt', 
            values='spend'
        ).sort_index(axis=1, ascending=False), 
        use_container_width=True
    )


    st.write("Yesterday spend data:")
    st.dataframe(ind_yesterday_data[['euid','ad_account_id','ad_account_name','currency_code','business_name','company_name','spend','dt']].sort_values(by='spend', ascending=False).reset_index(drop=True), use_container_width=True)

    st.write("Current Month spend data:")
    ind_grouped_data_adacclevel = ind_current_month_df.groupby([pd.to_datetime(ind_current_month_df['dt']).dt.strftime('%b %y'), 'euid','ad_account_id','ad_account_name','currency_code','business_name','company_name'])['spend'].sum().reset_index(name='spend').sort_values(by='spend', ascending=False).reset_index(drop=True)
    ind_grouped_data_adacclevel.index += 1


    # #Dataframe grouped by month and year of dt
    # ind_grouped_data_adacclevel = indian_df[indian_df['dt'].dt.month == current_month].groupby([pd.to_datetime(indian_df['dt']).dt.strftime('%b %y'), 'euid'])['spend'].sum().reset_index(name='spend').sort_values(by='spend', ascending=False)
    st.dataframe(ind_grouped_data_adacclevel, use_container_width=True)

    st.write("Last Month spend data:")
    ind_grouped_data_adacclevel = ind_last_month_df.groupby([pd.to_datetime(ind_last_month_df['dt']).dt.strftime('%b %y'), 'euid','ad_account_id','ad_account_name','currency_code','business_name','company_name'])['spend'].sum().reset_index(name='spend').sort_values(by='spend', ascending=False).reset_index(drop=True)
    ind_grouped_data_adacclevel.index += 1
    st.dataframe(ind_grouped_data_adacclevel, use_container_width=True)

    # Get top 10 overall spending ad accounts
    top_spending_ad_accounts = indian_df.groupby(['ad_account_id', 'ad_account_name'])['spend'].sum().reset_index(name='total_spend').sort_values(by='total_spend', ascending=False).head(10)

    st.write("Top 10 Overall Spending Ad Accounts:")
    st.dataframe(top_spending_ad_accounts, use_container_width=True)

    indian_df['euid'].replace('Unknown', 0, inplace=True)
    # Group by
    grouping = st.selectbox("Group by", ["Year", "Month", "Date"])
    if grouping == "Year":
        grouped_df = indian_df.groupby([pd.to_datetime(indian_df['dt']).dt.year, 'euid', 'ad_account_id', 'ad_account_name', 'currency_code'])['spend'].sum().reset_index()
    elif grouping == "Month":
        grouped_df = indian_df.groupby([pd.to_datetime(indian_df['dt']).dt.strftime('%b %y'), 'euid', 'ad_account_id', 'ad_account_name', 'currency_code'])['spend'].sum().reset_index()
    else:  # Date
        grouped_df = indian_df.groupby([pd.to_datetime(indian_df['dt']).dt.strftime('%Y-%m-%d'), 'euid', 'ad_account_id', 'ad_account_name', 'currency_code'])['spend'].sum().reset_index()

    # Add start date and end date filter
    st.write("Select Date Range to Filter Spend Data:")
    min_date = pd.to_datetime(grouped_df['dt']).min().date()
    max_date = pd.to_datetime(grouped_df['dt']).max().date()
    start_date = st.date_input("Start Date", value=min_date, min_value=min_date, max_value=max_date)
    end_date = st.date_input("End Date", value=max_date, min_value=min_date, max_value=max_date)

    # Filter grouped_df by selected date range
    grouped_df = grouped_df[(pd.to_datetime(grouped_df['dt']).dt.date >= start_date) & (pd.to_datetime(grouped_df['dt']).dt.date <= end_date)]

    st.write(f"Spend Data Grouped by {grouping}:")
    grouped_df.index += 1
    grouped_df = grouped_df.sort_values(by='dt', ascending=False)
    # st.dataframe(grouped_df, use_container_width=True)
    pivot_df = grouped_df.pivot(index=['euid','ad_account_name','ad_account_id','currency_code'], columns='dt', values='spend')
    pivot_df = pivot_df.reindex(sorted(pivot_df.columns, reverse=True), axis=1)
    st.dataframe(pivot_df, use_container_width=True)


elif selected == "Overall Stats - US" and st.session_state.status == "verified":

    st.title("Overall Stats - US")

    us_df = df[df['currency_code'].str.lower() != 'inr']
    # ai_spends_df = ai_spends_df[ai_spends_df['currency_code'].str.lower() != 'inr']

    # us_df = us_df[['euid', 'ad_account_name',  'ad_account_id','currency_code', 'dt','spend']]

    # st.dataframe(us_df, use_container_width=True)

    # # business_id,account_name,cs.ad_account_id,currency,date(date_start)dt,sum(spend)spend

    # ai_spends_df = ai_spends_df[['euid', 'ad_account_name',  'ad_account_id','currency_code', 'dt','spend']]

    # us_df = pd.concat([us_df,ai_spends_df], ignore_index=True)

    # #remove duplicates
    # us_df = us_df.drop_duplicates(subset=['ad_account_id', 'dt'], keep='first')

    us_df['spend'] = pd.to_numeric(us_df['spend'], errors='coerce')
    us_df['spend'] = us_df['spend'].fillna(0)
    us_df = us_df.fillna("Unknown")

    # st.dataframe(us_df, use_container_width=True)

    # Identify unique currency codes other than 'USD'
    non_usd_currencies = us_df['currency_code'].unique()
    non_usd_currencies = [currency for currency in non_usd_currencies if currency != 'USD']

    # Create a dictionary to store the conversion rates entered by the user
    conversion_rates = {}

    # Predefine default values for specific currencies
    default_values = {
                        'EUR': 1.05,
                        'GBP': 1.27,
                        'AUD': 0.65,
                        'CAD': 0.70
                    }

    # Display input boxes for each unique currency code other than 'USD'
    st.write("Enter conversion rates for the following currencies:")
   
    # Create columns dynamically based on the number of currencies
    cols = st.columns(4)  # Adjust the number of columns (3 in this case)

    # Iterate over non-USD currencies and display them in columns
    for idx, currency in enumerate(non_usd_currencies):
        default_value = default_values.get(currency, 1.0)  # Use default value if defined, otherwise 1.0
        with cols[idx % 4]:  # Rotate through the columns
            conversion_rates[currency] = st.number_input(
                f"{currency} to USD:", value=default_value, min_value=0.0, step=0.001, format="%.3f"
            )

    # Function to convert the spend to USD using the entered conversion rates
    def convert_to_usd(row):
        if row['currency_code'] == 'USD':
            return row['spend']
        elif row['currency_code'] in conversion_rates:
            return row['spend'] * conversion_rates[row['currency_code']]
        return row['spend']

    # Create the 'spend_in_usd' column
    us_df['spend_in_usd'] = us_df.apply(lambda row: float(convert_to_usd(row)), axis=1)

       # Group the DataFrame by 'euid', 'ad_account_id', and 'dt', and sum 'spend'
    us_grouped_data_adacclevel = us_df.groupby(['euid', 'ad_account_id', 'dt','ad_account_name', 'currency_code'])['spend'].sum().reset_index()

    # Filter data for yesterday and day before yesterday
    us_yesterday_data = us_grouped_data_adacclevel[us_grouped_data_adacclevel['dt'] == yesterday]
    us_day_before_yst_data = us_grouped_data_adacclevel[us_grouped_data_adacclevel['dt'] == day_before_yst]

    # Calculate the total spend for each day
    us_yst_spend = us_yesterday_data['spend'].sum().round()
    us_day_before_yst_spend = us_day_before_yst_data['spend'].sum().round()

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

        # Filter the DataFrame to get the current month’s data
    us_current_month_df = us_df[pd.to_datetime(us_df['dt']).dt.to_period('M') == pd.to_datetime('today').to_period('M')]
    us_last_month_df = us_df[pd.to_datetime(us_df['dt']).dt.to_period('M') == (pd.to_datetime('today') - pd.DateOffset(months=1)).to_period('M')]

    # Calculate the total spend for the current month
    us_current_month_spend = us_current_month_df['spend'].sum()
    us_last_month_spend = us_last_month_df['spend'].sum()

    # Display the metrics
    col1, col2, col3, col4 = st.columns(4)

    # Metric 1: Yesterday's Spend and change %
    col1.metric("Yesterday Spend", f"${us_yst_spend}", f"{us_spend_change}%")

    # Metric 2: Number of Ad Accounts and change %
    col2.metric("Ad Accounts", us_num_ad_accounts_yesterday, f"{us_ad_account_change}%")

    # Display the current month spend as a metric
    col3.metric(label="Current Month Spend (in USD)", value=f"${us_current_month_spend:}")

    # Display the last month spend as a metric
    col4.metric("Last Month Spend", f"${us_last_month_spend:,.2f}")

    st.write("Yesterday spend data:")
    st.dataframe(us_df[us_df['dt']==yesterday].sort_values(by='spend_in_usd', ascending=False).reset_index(drop=True), use_container_width=True)
    
    st.write("Current Month spend data:")
    us_grouped_data_adacclevel = us_current_month_df.groupby(
        [pd.to_datetime(us_current_month_df['dt']).dt.strftime('%b %y'), 'euid', 'ad_account_id', 'ad_account_name', 'currency_code']
    )[['spend', 'spend_in_usd']].agg(
        total_spend_in_usd=('spend_in_usd', 'sum'),
        avg_spend_last_30_days=('spend_in_usd', lambda x: x[-30:].mean())
    ).sort_values(by='total_spend_in_usd', ascending=False).reset_index()
    us_grouped_data_adacclevel.index += 1
    st.dataframe(us_grouped_data_adacclevel, use_container_width=True)

    st.write("Last month spend data:")
    us_grouped_data_adacclevel = us_last_month_df.groupby([pd.to_datetime(us_last_month_df['dt']).dt.strftime('%b %y'), 'euid', 'ad_account_id', 'ad_account_name', 'currency_code'])[[ 'spend','spend_in_usd']].sum().sort_values(by='spend_in_usd', ascending=False).reset_index()
    us_grouped_data_adacclevel.index += 1
    st.dataframe(us_grouped_data_adacclevel, use_container_width=True)

    #top 10 spenders
    st.write("Top 10 spenders:")
    top_spenders = (
        us_grouped_data_adacclevel.groupby([ 'euid', 'ad_account_id', 'ad_account_name'])["spend_in_usd"]
        .sum()
        .reset_index()
        .sort_values(by="spend_in_usd", ascending=False)
        .head(10)
    )

    st.dataframe(top_spenders, use_container_width=True)

    # Group by
    grouping = st.selectbox("Group by", ["Year", "Month", "Date"])
    if grouping == "Year":
        grouped_df = us_df.groupby([pd.to_datetime(us_df['dt']).dt.year, 'euid', 'ad_account_id', 'ad_account_name', 'currency_code'])['spend_in_usd'].sum().reset_index()
    elif grouping == "Month":
        grouped_df = us_df.groupby([pd.to_datetime(us_df['dt']).dt.strftime('%b %y'), 'euid', 'ad_account_id', 'ad_account_name', 'currency_code'])['spend_in_usd'].sum().reset_index()
    else:  # Date
        grouped_df = us_df.groupby([pd.to_datetime(us_df['dt']).dt.strftime('%Y-%m-%d'), 'euid', 'ad_account_id', 'ad_account_name', 'currency_code'])['spend_in_usd'].sum().reset_index()
   
    # Add start date and end date filter
    st.write("Select Date Range to Filter Spend Data:")
    min_date = pd.to_datetime(grouped_df['dt']).min().date()
    max_date = pd.to_datetime(grouped_df['dt']).max().date()
    start_date = st.date_input("Start Date", value=min_date, min_value=min_date, max_value=max_date)
    end_date = st.date_input("End Date", value=max_date, min_value=min_date, max_value=max_date)

    # Filter grouped_df by selected date range
    grouped_df = grouped_df[(pd.to_datetime(grouped_df['dt']).dt.date >= start_date) & (pd.to_datetime(grouped_df['dt']).dt.date <= end_date)]

    st.write(f"Spend Data Grouped by {grouping}:")
    grouped_df.index += 1
    grouped_df = grouped_df.sort_values(by='dt', ascending=False)
    # Replace 'Unknown' in 'euid' with 0 and ensure numeric type
    grouped_df['euid'] = grouped_df['euid'].replace('Unknown', 0)
    grouped_df['euid'] = pd.to_numeric(grouped_df['euid'], errors='coerce').fillna(0).astype(int)
    pivot_df = grouped_df.pivot(index=['euid','ad_account_name','ad_account_id','currency_code'], columns='dt', values='spend_in_usd')
    pivot_df = pivot_df.reindex(sorted(pivot_df.columns, reverse=True), axis=1)
    st.dataframe(pivot_df, use_container_width=True)



elif selected == "Ad360 Subscription-Analysis" and st.session_state.status == "verified":
    # Streamlit App
    st.title("Ad360 Subscription Dashboard")

    # Currency Filter
    currency_option = st.selectbox("Select Currency", ["India", "US"])
    if currency_option == "India":
        currency_filter = "INR"
        filtered_df = sub_df[sub_df['currency'] == currency_filter].reset_index(drop=True)
    elif currency_option == "US":
        currency_filter = "INR"
        filtered_df = sub_df[sub_df['currency'] != currency_filter]
    # else:
    #     filtered_df = sub_df  # Show all data if "All" is selected

    filtered_df = filtered_df[filtered_df['rw']==1]
    # Key Metrics Display
    st.header("Key Metrics")

    # Arrange key metrics in columns for better layout
    col1, col2, col3, col4 = st.columns(4)

    # Calculations for metrics
    total_accounts = filtered_df['ad_account_id'].nunique()
    total_subscription_amount = filtered_df['plan_amount'].sum()
    avg_plan_amount = filtered_df['plan_amount'].mean()
    avg_utilization = filtered_df['actual_td_util'].mean()
    total_spend = filtered_df['actual_td_spend'].sum()
    expected_spend = filtered_df['expected_td_spend'].sum()

    # Display metrics in columns
    col1.metric("Total Accounts", total_accounts)
    col2.metric("Total Subscription Amount", f"{total_subscription_amount:,.2f}")
    col3.metric("Average Plan Amount", f"{avg_plan_amount:,.2f}")
    col4.metric("Average Utilization (%)", f"{avg_utilization:.2f}%")

    col1.metric("Total Spend", f"{total_spend:,.2f}")
    col2.metric("Expected Spend", f"{expected_spend:,.2f} ")

    # Define filter categories
    no_adspends = filtered_df[filtered_df['adspends_added'] == 0].reset_index(drop=True)
    need_attention = filtered_df[filtered_df['actual_td_util'] < 30].reset_index(drop=True)
    potential_upgrade = filtered_df[filtered_df['actual_td_util'] > 70].reset_index(drop=True)
    upcoming_renewals = filtered_df[filtered_df['sub_end'] <= date.today() + timedelta(days=7)].reset_index(drop=True)

    #All the active accounts
    st.subheader("Active Subscriptions")
    st.metric("Number of Accounts", filtered_df.shape[0])
    st.dataframe(filtered_df)

    # Display the number of accounts per category with metrics
    st.header("Account Divisions")

    # Categories metrics display
    st.subheader("Subscription with No Adspends")
    st.metric("Number of Accounts", no_adspends.shape[0])
    st.dataframe(no_adspends)

    st.subheader("Ad Accounts that Need Attention")
    st.metric("Number of Accounts", need_attention.shape[0])
    st.dataframe(need_attention)

    st.subheader("Potential Upgrades")
    st.metric("Number of Accounts", potential_upgrade.shape[0])
    st.dataframe(potential_upgrade)

    st.subheader("Upcoming Renewals")
    st.metric("Number of Accounts", upcoming_renewals.shape[0])
    st.dataframe(upcoming_renewals)
    

elif selected == "Euid - adaccount mapping" and st.session_state.status == "verified":
    st.title("Euid - adaccount mapping")
    st.dataframe(list_df, use_container_width=True)

    euid = st.text_input("Type an euid or a list of euids separated by comma").strip()
    if euid:
        if ',' in euid:
            try:
                euid_list = [int(x.strip()) for x in euid.split(',') if x.strip().isdigit()]
                filtered_list_df = list_df[list_df['euid'].isin(euid_list)]
            except ValueError:
                st.error("Please enter valid integers separated by commas.")
                filtered_list_df = pd.DataFrame()  # Empty DataFrame
        else:
            try:
                euid_int = int(euid)
                filtered_list_df = list_df[list_df['euid'] == euid_int]
            except ValueError:
                st.error("Please enter a valid integer.")
                filtered_list_df = pd.DataFrame()  # Empty DataFrame
    else:
        filtered_list_df = pd.DataFrame()  # Empty DataFrame

    st.dataframe(filtered_list_df, use_container_width=True)
    ad_account_ids = st.text_input("Type ad account ids (comma separated)").strip()

    if ad_account_ids:
        ad_account_ids_list = [x.strip() for x in ad_account_ids.split(',') if x.strip()]
        filtered_list_df = list_df[list_df['ad_account_id'].isin(ad_account_ids_list)]
    else:
        filtered_list_df = pd.DataFrame()  # Empty DataFrame if no input
    st.dataframe(filtered_list_df, use_container_width=True)

elif selected == "Top accounts" and st.session_state.status == "verified":
    
    # Streamlit App
    st.title("Top 10 Businesses by Spend")

    non_usd_currencies = df['currency_code'].unique()
    non_usd_currencies = [currency for currency in non_usd_currencies if currency != 'USD']

       # Create a dictionary to store the conversion rates entered by the user
    conversion_rates = {}

    # Predefine default values for specific currencies
    default_values = {
                        'EUR': 1.08,
                        'GBP': 1.30,
                        'AUD': 0.66,
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

    # Display input boxes for each unique currency code other than 'USD'
    st.write("Enter conversion rates for the following currencies:")
   
    # Create columns dynamically based on the number of currencies
    cols = st.columns(4)  # Adjust the number of columns (3 in this case)

    # Iterate over non-USD currencies and display them in columns
    for idx, currency in enumerate(non_usd_currencies):
        default_value = default_values.get(currency, 1.0)  # Use default value if defined, otherwise 1.0
        with cols[idx % 4]:  # Rotate through the columns
            conversion_rates[currency] = st.number_input(
                f"{currency} to USD:", value=default_value, min_value=0.0, step=0.001, format="%.3f"
            )

    def convert_to_usd(row):
        if row['currency_code'] == 'USD':
            return row['spend']
        elif row['currency_code'] in conversion_rates:
            return row['spend'] * conversion_rates[row['currency_code']]
        return row['spend']

    # usdtoinr = st.number_input("Enter conversion rates for the USD to INR:", min_value=0.0, value=84.2, step=0.01)

    # Create the 'spend_in_usd' column
    df['spend_in_usd'] = df.apply(lambda row: convert_to_usd(row), axis=1)
    # df['spend_in_inr'] = df['spend_in_usd'] * usdtoinr

    # st.dataframe(df, use_container_width=True)



    # Date Range Selection
    time_frame = st.selectbox("Select Time Frame", ["Last 30 Days","Last 60 Days", "Last 90 Days", "Overall", "Custom Date Range"])

    # Currency Filter
    currency_option = st.selectbox("Select BM", ["All", "INR", "USD"])
    if currency_option == "USD":
        filtered_df = df[df['currency_code'] != 'INR']
    if currency_option == "INR":
        filtered_df = df[df['currency_code'] == 'INR']
    if currency_option == "All":
        filtered_df = df

    #Top Numbers
    n = st.number_input("Number of records to display", min_value=1, value=10)

    if time_frame == "Last 30 Days":
        start_date = datetime.now() - timedelta(days=30)
        end_date = datetime.now()
    elif time_frame == "Last 60 Days":
        start_date = datetime.now() - timedelta(days=60)
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
    filtered_df = filtered_df[(filtered_df['dt'] >= start_date.date()) & (filtered_df['dt'] <= end_date.date())]

    # Aggregate spend per business and get the top 10
    top_spenders = (
        filtered_df.groupby([ "ad_account_id"])["spend_in_usd"]
        .sum()
        .reset_index()
        .sort_values(by="spend_in_usd", ascending=False)
        .head(n)
    )

    filtered_df = filtered_df[['euid', 'ad_account_id', 'ad_account_name','business_manager_name']]

    top_businesses = pd.merge(top_spenders, filtered_df, on="ad_account_id", how="left") \
                   .drop_duplicates(subset="ad_account_id") \
                   .sort_values(by="spend_in_usd", ascending=False).reset_index(drop=True)
    
    top_businesses = pd.merge(top_businesses, disabled_account_df[["ad_account_id", "flag"]], on="ad_account_id", how="left") \
                   .drop_duplicates(subset="ad_account_id") \
                   .sort_values(by="spend_in_usd", ascending=False).reset_index(drop=True)
    
    top_businesses['flag'] = top_businesses['flag'].fillna("Active")

    # st.dataframe(top_businesses, use_container_width=True)

    top_businesses = top_businesses[['euid', 'ad_account_id', 'ad_account_name','business_manager_name', 'spend_in_usd','flag']]

    # Display top 10 businesses
    st.header(f"Top {n} {currency_option} Businesses by Spend")
    st.write(f"Showing data from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")

    top_businesses.index += 1
    st.dataframe(top_businesses, use_container_width=True)

# elif selected == "AI account spends" and st.session_state.status == "verified":

#     st.title("AI Spend Dashboard")

#     grouping = st.selectbox('Choose Grouping', ['Year', 'Month', 'Week', 'Date'], index=1)

#     ai_spends_df['dt'] = pd.to_datetime(ai_spends_df['dt'])

#     col1,col2 = st.columns(2)
#     with col1:
#         start_date = st.date_input("Start Date", value=datetime(2024, 9, 1))
#     with col2:
#         end_date = st.date_input("End Date", value=datetime.now())

#     ai_spends_df = ai_spends_df[(ai_spends_df['dt'] >= pd.to_datetime(start_date)) & (ai_spends_df['dt'] <= pd.to_datetime(end_date))]

#     # User option to select time frame
#     # time_frame = st.selectbox("Select Time Frame", ["Day", "Week", "Month", "Year"])

#     # Today's and yesterday's dates for calculating metrics
#     today = datetime.now().date()
#     yesterday = today - timedelta(days=1)

#        # Assuming your 'dt' column is already in date format (e.g., YYYY-MM-DD)
#     if grouping == 'Year':
#         ai_spends_df.loc[:, 'grouped_date'] = ai_spends_df['dt'].apply(lambda x: x.strftime('%Y'))  # Year format as 2024
#     elif grouping == 'Month':
#         ai_spends_df.loc[:, 'grouped_date'] = ai_spends_df['dt'].apply(lambda x: x.strftime('%b-%y'))  # Month format as Jan-24
#     elif grouping == 'Week':
#         ai_spends_df.loc[:, 'grouped_date'] = ai_spends_df['dt'].apply(lambda x: f"{x.strftime('%Y')} - week {x.isocalendar()[1]}")  # Week format as 2024 - week 24
#     else:
#         ai_spends_df.loc[:, 'grouped_date'] = ai_spends_df['dt']  # Just use the date as is (in date format)

#     # Aggregate the spend values by the selected grouping
#     grouped_df = ai_spends_df.groupby(['ad_account_name','ad_account_id','currency_code','grouped_date','name'])['spend'].sum().reset_index()
#     # Metrics Calculation
#     # Today's Spend
#     today_spend = ai_spends_df[ai_spends_df['dt'].dt.date == today]['spend'].sum()
#     # Yesterday's Spend
#     yesterday_spend = ai_spends_df[ai_spends_df['dt'].dt.date == yesterday]['spend'].sum()
#     # Spend Change from Yesterday
#     spend_change = ((today_spend - yesterday_spend) / yesterday_spend * 100) if yesterday_spend != 0 else 0
#     # Current Month Spend
#     current_month_spend = ai_spends_df[ai_spends_df['dt'].dt.to_period("M") == today.strftime("%Y-%m")]['spend'].sum()
#     # Last Month Spend
#     last_month = (today.replace(day=1) - timedelta(days=1)).strftime("%Y-%m")
#     last_month_spend = ai_spends_df[ai_spends_df['dt'].dt.to_period("M") == last_month]['spend'].sum()
#     # Number of Active Ad Accounts
#     active_ad_accounts = ai_spends_df['ad_account_id'].nunique()


#     col1, col2 = st.columns(2)
    
#     # Display Metrics
#     col1.metric("Today's Spend", f"${today_spend:,.2f}")
#     # st.metric("Change from Yesterday", f"{spend_change:.2f}%")
#     col2.metric("Current Month Spend", f"${current_month_spend:,.2f}")
#     col1.metric("Last Month Spend", f"${last_month_spend:,.2f}")
#     col2.metric("Active Ad Accounts", active_ad_accounts)

#     # Display grouped data
#     st.header(f"Spend Data - {grouping} View")

#     pivoted_df = grouped_df.pivot(index=['ad_account_name','ad_account_id','currency_code','name'], columns='grouped_date', values='spend')
#     st.dataframe(pivoted_df, use_container_width=True)

#     # Display full table
#     st.header("Full Table")
#     st.dataframe(ai_spends_df, use_container_width=True)


elif selected == "FB API Campaign spends" and st.session_state.status == "verified":

    st.title("FB API Campaign Spend Dashboard")
    st.text("Excludes today's Data.")

    ai_campaign_spends_df['spend'] = pd.to_numeric(ai_campaign_spends_df['spend'], errors='coerce')
    ai_campaign_spends_df['buid'] = pd.to_numeric(ai_campaign_spends_df['buid'], errors='coerce')

    ai_campaign_spends_df = ai_campaign_spends_df[pd.to_datetime(ai_campaign_spends_df['dt']).dt.date != datetime.now().date()]

    #Arrange key metrics in columns for better layout
    col1, col2 = st.columns(2)

    with col1:

    # Currency Filter
        currency_option = st.selectbox("Select BM", ["All", "IND BM", "US BM"], index=0)

        start_date = st.date_input("Start Date", value=datetime(2024, 9, 1))

        onboarding_flag = st.selectbox("Onboarding Flag", ["All", "Onboarded >30d ago", "Onboarded <30d ago"], index=0)
        
    if currency_option == "IND BM":
        ai_campaign_spends_df = ai_campaign_spends_df[ai_campaign_spends_df['currency'] == "INR"]
    if currency_option == "US BM":
        ai_campaign_spends_df = ai_campaign_spends_df[ai_campaign_spends_df['currency'] != "INR"]
    else:
        ai_campaign_spends_df = ai_campaign_spends_df

    with col2:

        grouping = st.selectbox('Choose Grouping', ['Year', 'Month', 'Week', 'Date'], index=1)

        end_date = st.date_input("End Date", value=ai_campaign_spends_df['dt'].max())

        buid = st.number_input("Enter buid", min_value=0, value=0, step=1)

    non_usd_currencies = ai_campaign_spends_df['currency'].unique()
    non_usd_currencies = [currency for currency in non_usd_currencies if currency != 'USD']

       # Create a dictionary to store the conversion rates entered by the user
    conversion_rates = {}

    # Predefine default values for specific currencies
    default_values = {
                        'EUR': 1.08,
                        'GBP': 1.30,
                        'AUD': 0.66,
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

    # Display input boxes for each unique currency code other than 'USD'
    st.write("Enter conversion rates for the following currencies:")
   
    # Create columns dynamically based on the number of currencies
    cols = st.columns(4)  # Adjust the number of columns (3 in this case)

    # Iterate over non-USD currencies and display them in columns
    for idx, currency in enumerate(non_usd_currencies):
        default_value = default_values.get(currency, 1.0)  # Use default value if defined, otherwise 1.0
        with cols[idx % 4]:  # Rotate through the columns
            conversion_rates[currency] = st.number_input(
                f"{currency} to USD:", value=default_value, min_value=0.0, step=0.001, format="%.3f"
            )

    def convert_to_usd(row):
        if row['currency'] == 'USD':
            return row['spend']
        elif row['currency'] in conversion_rates:
            return row['spend'] * conversion_rates[row['currency']]
        return row['spend']
    
    # def convert_to_inr(row):
    #     if row['currency'] == 'INR':
    #         return row['spend']
    #     elif row['currency'] in conversion_rates:
    #         return row['spend_in_usd'] * conversion_rates[row['currency']]
    #     return row['spend']

    # Create the 'spend_in_usd' column
    ai_campaign_spends_df['spend_in_usd'] = ai_campaign_spends_df.apply(lambda row: convert_to_usd(row), axis=1)
    # ai_campaign_spends_df['spend_in_inr'] = ai_campaign_spends_df.apply(lambda row: convert_to_inr(row), axis=1)
    

    ai_campaign_spends_df['dt'] = pd.to_datetime(ai_campaign_spends_df['dt'])

    # Today's and yesterday's dates for calculating metrics
    today = datetime.now().date()
    yesterday = today - timedelta(days=1)


    # Assuming your 'dt' column is already in date format (e.g., YYYY-MM-DD)
    if grouping == 'Year':
        ai_campaign_spends_df.loc[:, 'grouped_date'] = ai_campaign_spends_df['dt'].apply(lambda x: x.strftime('%Y'))  # Year format as 2024
    elif grouping == 'Month':
        ai_campaign_spends_df.loc[:, 'grouped_date'] = ai_campaign_spends_df['dt'].apply(lambda x: x.strftime('%b-%y'))  # Month format as Jan-24
    elif grouping == 'Week':
        ai_campaign_spends_df.loc[:, 'grouped_date'] = ai_campaign_spends_df['dt'].apply(lambda x: f"{x.strftime('%Y')} - week {x.isocalendar()[1]}")  # Week format as 2024 - week 24
    else:
        ai_campaign_spends_df.loc[:, 'grouped_date'] = ai_campaign_spends_df['dt']  # Just use the date as is (in date format)

    if onboarding_flag == "Onboarded <30d ago":
        ai_campaign_spends_df = ai_campaign_spends_df[ai_campaign_spends_df['user_flag'] == 'Onboarded <30d ago']
    elif onboarding_flag == "Onboarded >30d ago":
        ai_campaign_spends_df = ai_campaign_spends_df[ai_campaign_spends_df['user_flag'] == 'Onboarded >30d ago']
    else:
        ai_campaign_spends_df = ai_campaign_spends_df
    
    if buid != 0:
        ai_campaign_spends_df = ai_campaign_spends_df[ai_campaign_spends_df['buid'] == buid]

    # Metrics Calculation
    # Today's Spend
            # if currency_option == "IND BM":
            #     today_spend = ai_campaign_spends_df[ai_campaign_spends_df['dt'].dt.date == today]['spend'].sum()
            # if currency_option == "US BM":
            #     today_spend = ai_campaign_spends_df[ai_campaign_spends_df['dt'].dt.date == today]['spend_in_usd'].sum()
            # else:
            #     today_spend = ai_campaign_spends_df[ai_campaign_spends_df['dt'].dt.date == today]['spend_in_inr'].sum() 
    today_spend = ai_campaign_spends_df[ai_campaign_spends_df['dt'].dt.date == today]['spend_in_usd'].sum() 
    Overall_spend = ai_campaign_spends_df[ai_campaign_spends_df['dt'].dt.year == today.year]['spend_in_usd'].sum()
    
    # Yesterday's Spend
    yesterday_spend = ai_campaign_spends_df[ai_campaign_spends_df['dt'].dt.date == yesterday]['spend_in_usd'].sum()
    day_before_yesterday_spend = ai_campaign_spends_df[ai_campaign_spends_df['dt'].dt.date == day_before_yst]['spend_in_usd'].sum()

    # Spend Change from Yesterday
    tdy_spend_change = ((today_spend - yesterday_spend) / yesterday_spend * 100) if yesterday_spend != 0 else 0
    spend_change = ((yesterday_spend - day_before_yesterday_spend) / day_before_yesterday_spend * 100) if day_before_yesterday_spend != 0 else 0
    # Current Month Spend
    current_month_spend = ai_campaign_spends_df[ai_campaign_spends_df['dt'].dt.to_period("M") == today.strftime("%Y-%m")]['spend_in_usd'].sum()
    # Last Month Spend
    last_month = (today.replace(day=1) - timedelta(days=1)).strftime("%Y-%m")
    last_month_spend = ai_campaign_spends_df[ai_campaign_spends_df['dt'].dt.to_period("M") == last_month]['spend_in_usd'].sum()
    # Number of Active Ad Accounts
    active_ad_accounts = ai_campaign_spends_df['ad_account_id'].nunique()

     # Get today's date to identify the current and last month
    today = datetime.now().date()
    current_month_period = today.strftime("%Y-%m")  # e.g., "2024-10"
    last_month_period = (today.replace(day=1) - timedelta(days=1)).strftime("%Y-%m")  # Previous month in "YYYY-MM" format

    # Filter for current month
    current_month_data = ai_campaign_spends_df[ai_campaign_spends_df['dt'].dt.to_period("M") == current_month_period]
    # Filter for last month
    last_month_data = ai_campaign_spends_df[ai_campaign_spends_df['dt'].dt.to_period("M") == last_month_period]

    # Calculate average spend per account for the current month
    total_spend_current_month = current_month_data['spend_in_usd'].sum()
    unique_accounts_current_month = current_month_data['ad_account_id'].nunique()
    avg_spend_current_month = total_spend_current_month / unique_accounts_current_month if unique_accounts_current_month > 0 else 0

    # Calculate average spend per account for the last month
    total_spend_last_month = last_month_data['spend_in_usd'].sum()
    unique_accounts_last_month = last_month_data['ad_account_id'].nunique()
    avg_spend_last_month = total_spend_last_month / unique_accounts_last_month if unique_accounts_last_month > 0 else 0

    # Calculate percentage change in average spend per account from last month to current month
    average_spend_change = ((avg_spend_current_month - avg_spend_last_month) / avg_spend_last_month * 100) if avg_spend_last_month > 0 else None
    
    # Display Metrics
    last_year_spend = ai_campaign_spends_df[ai_campaign_spends_df['dt'].dt.year == today.year-1]['spend_in_usd'].sum()
    print(last_year_spend)
    overall_spend_change = ((Overall_spend - last_year_spend) / last_year_spend * 100) if last_year_spend != 0 else 0
    col1.metric("Overall Spend (YTD)", f"${int(Overall_spend):,}",f"{overall_spend_change:,.2f}%")
    # col1.metric("Today's Spend", f"${int(today_spend):,}",f"{tdy_spend_change:,.2f}%")
    
    col2.metric("Yesterday Spend", f"${int(yesterday_spend):,}",f"{spend_change:,.2f}%")
    col1.metric("Current Month Spend", f"${int(current_month_spend):,}")
    col2.metric("Last Month Spend", f"${int(last_month_spend):,}")
    col1.metric("Average Spend per Account - Current Month", f"${avg_spend_current_month:,.2f}",f"{average_spend_change:.2f}%" if average_spend_change is not None else "N/A")
    col2.metric("Average Spend per Account - Last Month", f"${avg_spend_last_month:,.2f}")
    # col2.metric("Active Ad Accounts", active_ad_accounts)

    ai_campaign_spends_df = ai_campaign_spends_df[(ai_campaign_spends_df['dt'] >= pd.to_datetime(start_date)) & (ai_campaign_spends_df['dt'] <= pd.to_datetime(end_date))]

    # Aggregate the spend values by the selected grouping
    grouped_df = ai_campaign_spends_df.groupby(['buid','email','ad_account_id','account_name','currency','grouped_date'])['spend'].sum().reset_index()
   
    # Display grouped data
    st.header(f"Spend Data Ad Account Level- {grouping}")
    pivot_df = grouped_df.pivot(index=['buid','email','account_name','ad_account_id','currency'], columns='grouped_date', values='spend')

    # st.dataframe(grouped_df, use_container_width=True)

    # Sort the columns by date in descending order
    if grouping == 'Year':
        pivot_df = pivot_df[sorted(pivot_df.columns, key=lambda x: pd.to_datetime(x, format='%Y'), reverse=True)]
    elif grouping == 'Month':
        pivot_df = pivot_df[sorted(pivot_df.columns, key=lambda x: pd.to_datetime(x, format='%b-%y'), reverse=True)]
    elif grouping == 'Week':
        pivot_df = pivot_df[sorted(pivot_df.columns, key=lambda x: (int(x.split(' - week ')[0]), int(x.split(' - week ')[1])), reverse=True)]
    else:  # Date
        pivot_df = pivot_df[sorted(pivot_df.columns, key=lambda x: pd.to_datetime(x), reverse=True)]


    st.dataframe(pivot_df, use_container_width=True)
   
    # Display grouped data
    st.header(f"Spend Data Ad Account Level - {grouping} USD View")
    usd_grouped_df = ai_campaign_spends_df.groupby(['buid','email','ad_account_id','account_name','grouped_date'])['spend_in_usd'].sum().reset_index()
    pivot_df = usd_grouped_df.pivot(index=['buid','email','account_name','ad_account_id'], columns='grouped_date', values='spend_in_usd')

    # Sort the columns by date in descending order
    if grouping == 'Year':
        pivot_df = pivot_df[sorted(pivot_df.columns, key=lambda x: pd.to_datetime(x, format='%Y'), reverse=True)]
    elif grouping == 'Month':
        pivot_df = pivot_df[sorted(pivot_df.columns, key=lambda x: pd.to_datetime(x, format='%b-%y'), reverse=True)]
    elif grouping == 'Week':
        pivot_df = pivot_df[sorted(pivot_df.columns, key=lambda x: (int(x.split(' - week ')[0]), int(x.split(' - week ')[1])), reverse=True)]
    else:  # Date
        pivot_df = pivot_df[sorted(pivot_df.columns, key=lambda x: pd.to_datetime(x), reverse=True)]

    # st.dataframe(grouped_df, use_container_width=True)
    st.dataframe(pivot_df.style.format("{:.2f}"), use_container_width=True)

    st.header("Campaign Level Data")

    # Aggregate the spend values by the selected grouping
    grouped_df = ai_campaign_spends_df.groupby(['buid','email','ad_account_id','account_name','campaign_name','campaign_id','currency','grouped_date'])['spend'].sum().reset_index()
   
    # Display grouped data
    st.header(f"Spend Data Campaign Level- {grouping}")
    pivot_df = grouped_df.pivot(index=['buid','email','account_name','ad_account_id','campaign_name','campaign_id','currency'], columns='grouped_date', values='spend')

    # Sort the columns by date in descending order
    if grouping == 'Year':
        pivot_df = pivot_df[sorted(pivot_df.columns, key=lambda x: pd.to_datetime(x, format='%Y'), reverse=True)]
    elif grouping == 'Month':
        pivot_df = pivot_df[sorted(pivot_df.columns, key=lambda x: pd.to_datetime(x, format='%b-%y'), reverse=True)]
    elif grouping == 'Week':
        pivot_df = pivot_df[sorted(pivot_df.columns, key=lambda x: (int(x.split(' - week ')[0]), int(x.split(' - week ')[1])), reverse=True)]
    else:  # Date
        pivot_df = pivot_df[sorted(pivot_df.columns, key=lambda x: pd.to_datetime(x), reverse=True)]

    # st.dataframe(grouped_df, use_container_width=True)
    st.dataframe(pivot_df, use_container_width=True)

    # Display grouped data
    st.header(f"Spend Data Campaign Level - {grouping} USD View")
    usd_grouped_df = ai_campaign_spends_df.groupby(['buid','email','ad_account_id','account_name','campaign_name','campaign_id','grouped_date'])['spend_in_usd'].sum().reset_index()
    pivot_df = usd_grouped_df.pivot(index=['buid','email','account_name','ad_account_id','campaign_name','campaign_id'], columns='grouped_date', values='spend_in_usd')

    # Sort the columns by date in descending order
    pivot_df = pivot_df.reindex(sorted(pivot_df.columns, reverse=True), axis=1)


    st.dataframe(pivot_df, use_container_width=True)
    
    # Display full table
    st.header("Full Table")
    st.dataframe(ai_campaign_spends_df, use_container_width=True)

   
elif selected == "Disabled Ad Accounts" and st.session_state.status == "verified":

    st.title("Disabled/Reactivated Ad Accounts Dashboard")

    col1,col2,col3,col4 = st.columns(4)

    today = date.today()
    current_month_period = today.strftime("%Y-%m")
    last_month_period = pd.to_datetime(today).to_period("M") - 1

    # Ad Metrics
    today_reactivated_accounts = disabled_account_df[(disabled_account_df['disable_date'].dt.date == today) & (disabled_account_df['flag'] == 'Reactivated')].shape[0]
    yesterday_reactivated_accounts = disabled_account_df[(disabled_account_df['disable_date'].dt.date == yesterday) & (disabled_account_df['flag'] == 'Reactivated')].shape[0]
    current_month_reactivated_accounts = disabled_account_df[(disabled_account_df['disable_date'].dt.to_period("M") == current_month_period) & (disabled_account_df['flag'] == 'Reactivated')].shape[0]
    last_month_reactivated_accounts = disabled_account_df[(disabled_account_df['disable_date'].dt.to_period("M") == last_month_period) & (disabled_account_df['flag'] == 'Reactivated')].shape[0]
    today_disabled_accounts = disabled_account_df[(disabled_account_df['disable_date'].dt.date == today) & (disabled_account_df['flag'] == 'Disabled')].shape[0]
    yesterday_disabled_accounts = disabled_account_df[(disabled_account_df['disable_date'].dt.date == yesterday) & (disabled_account_df['flag'] == 'Disabled')].shape[0]
    current_month_disabled_accounts = disabled_account_df[(disabled_account_df['disable_date'].dt.to_period("M") == current_month_period) & (disabled_account_df['flag'] == 'Disabled')].shape[0]
    last_month_disabled_accounts = disabled_account_df[(disabled_account_df['disable_date'].dt.to_period("M") == last_month_period) & (disabled_account_df['flag'] == 'Disabled')].shape[0]
    
    
    col1.metric("Total Disabled Ad Accounts Today", today_disabled_accounts)
    col2.metric("Total Disabled Ad Accounts Yesterday", yesterday_disabled_accounts)
    col3.metric("Total Disabled Ad Accounts Current Month", current_month_disabled_accounts)
    col4.metric("Total Disabled Ad Accounts Last Month", last_month_disabled_accounts)
    
    col1.metric("Total Reactivated Ad Accounts Today", today_reactivated_accounts)
    col2.metric("Total Reactivated Ad Accounts Yesterday", yesterday_reactivated_accounts)
    col3.metric("Total Reactivated Ad Accounts Current Month", current_month_reactivated_accounts)
    col4.metric("Total Reactivated Ad Accounts Last Month", last_month_reactivated_accounts)


    disabled_account_df = disabled_account_df.sort_values(by='disable_date', ascending=False)

    # st.dataframe(disabled_account_df, use_container_width=True)

    flag = st.selectbox("Select Disabled/Reactived", ("Disabled", "Reactivated"))

    disabled_account_df = disabled_account_df.merge(roposo_acc_list_df, left_on='ad_account_id', right_on='Roposo', how='left')
    disabled_account_df = disabled_account_df.merge(datong_acc_list_df, left_on='ad_account_id', right_on='Datong', how='left')
    disabled_account_df = disabled_account_df.merge(shiprocket_acc_list_df, left_on='ad_account_id', right_on='Shiprocket', how='left')


    disabled_account_df['Roposo'] = disabled_account_df['Roposo'].fillna('')
    disabled_account_df['Roposo'] = disabled_account_df['Roposo'].apply(lambda x: 'Roposo' if x != '' else 'Others')

    disabled_account_df['Shiprocket'] = disabled_account_df['Shiprocket'].fillna('')
    disabled_account_df['Shiprocket'] = disabled_account_df['Shiprocket'].apply(lambda x: 'Shiprocket' if x != '' else 'Others')
    
    disabled_account_df['Datong'] = disabled_account_df['Datong'].fillna('')
    disabled_account_df['Datong'] = disabled_account_df['Datong'].apply(lambda x: 'Datong' if x != '' else 'Others')
    
    disabled_account_df['top_customers_flag'] = disabled_account_df.apply(lambda row: row['Roposo'] if row['Roposo'] != 'Others' else (row['Shiprocket'] if row['Shiprocket'] != 'Others' else row['Datong']), axis=1)
    
    disabled_account_df = disabled_account_df[['euid','ad_account_id', 'ad_account_name','currency','bm_name','disable_reason', 'disable_date', 'top_customers_flag', 'flag']]
    
    
    st.title(f"{flag} Ad Accounts")

    if flag == "Disabled":
        disabled_account_df = disabled_account_df[disabled_account_df['flag'] == 'Disabled']
        disabled_account_df = disabled_account_df.loc[:, disabled_account_df.columns != 'reactivation_date'].reset_index(drop=True)
        disabled_account_df.index+=1
    else:
        disabled_account_df = disabled_account_df[disabled_account_df['flag'] == 'Reactivated'].reset_index(drop=True)
        disabled_account_df.index+=1

    st.dataframe(disabled_account_df, use_container_width=True)


# elif selected == "Datong API VS Total Spends" and st.session_state.status == "verified":

#     st.title("Datong API VS Total Spends")

#     #warning message if currency contains other than inr
#     if not datong_api_df['currency_code'].eq('INR').all():
#         st.write("Warning: Currency column contains other than INR")

#     datong_api_df['dt'] = pd.to_datetime(datong_api_df['dt'])

#     st.dataframe(datong_api_df, use_container_width=True)

#     #group by choosing date
#     grouping = st.selectbox('Choose Grouping', ['Year', 'Month', 'Week', 'Date'], index=1)

#     # Assuming your 'dt' column is already in date format (e.g., YYYY-MM-DD)
#     if grouping == 'Year':
#         datong_api_df.loc[:, 'grouped_date'] = datong_api_df['dt'].apply(lambda x: x.strftime('%Y'))  # Year format as 2024
#     elif grouping == 'Month':
#         datong_api_df.loc[:, 'grouped_date'] = datong_api_df['dt'].apply(lambda x: x.strftime('%b-%y'))  # Month format as Jan-24
#     elif grouping == 'Week':
#         datong_api_df.loc[:, 'grouped_date'] = datong_api_df['dt'].apply(lambda x: f"{x.strftime('%Y')} - week {x.isocalendar()[1]}")  # Week format as 2024 - week 24
#     else:
#         datong_api_df.loc[:, 'grouped_date'] = datong_api_df['dt']  # Just use the date as is (in date format)

#     today = datetime.now().date()
#     yesterday = today - timedelta(days=1)

#     today_api_spend = datong_api_df[datong_api_df['dt'] == today]['spend'].sum()
#     today_tot_spend = datong_api_df[datong_api_df['dt'] == today]['total_spend'].sum() 
#     per_today_spend= (today_api_spend/today_tot_spend)*100

#     Overall_api_spend = datong_api_df['spend'].sum()
#     Overall_tot_spend = datong_api_df['total_spend'].sum() 
#     per_ovr_spend = (Overall_api_spend/Overall_tot_spend)*100

#     # Yesterday's api Spend
#     yesterday_api_spend = datong_api_df[datong_api_df['dt'].dt.date == yesterday]['spend'].sum()
#     day_before_yesterday_api_spend = datong_api_df[datong_api_df['dt'].dt.date == day_before_yst]['spend'].sum()
    

#     # Yesterday's total Spend
#     yesterday_total_spend = datong_api_df[datong_api_df['dt'].dt.date == yesterday]['total_spend'].sum()
#     day_before_yesterday_total_spend = datong_api_df[datong_api_df['dt'].dt.date == day_before_yst]['total_spend'].sum()

#     per_yst_spend = (yesterday_api_spend/yesterday_total_spend)*100
#     per_day_before_yesterday_spend = (day_before_yesterday_api_spend/day_before_yesterday_total_spend)*100

#     # Spend Change from Yesterday
#     tdy_spend_change = ((today_api_spend - yesterday_api_spend) / yesterday_api_spend * 100) if yesterday_api_spend != 0 else 0
#     spend_change = ((yesterday_api_spend - day_before_yesterday_api_spend) / day_before_yesterday_api_spend * 100) if day_before_yesterday_api_spend != 0 else 0

#     # Spend Change from Yesterday
#     tdy_spend_change = ((today_tot_spend - yesterday_total_spend) / yesterday_total_spend * 100) if yesterday_total_spend != 0 else 0
#     spend_change = ((yesterday_total_spend - day_before_yesterday_total_spend) / day_before_yesterday_total_spend * 100) if day_before_yesterday_total_spend != 0 else 0

#     # Current Month Spend
#     current_month_api_spend = datong_api_df[datong_api_df['dt'].dt.to_period("M") == today.strftime("%Y-%m")]['spend'].sum()
#     current_month_total_spend = datong_api_df[datong_api_df['dt'].dt.to_period("M") == today.strftime("%Y-%m")]['total_spend'].sum()
#     per_current_month_spend = (current_month_api_spend/current_month_total_spend)*100

#     # Last Month Spend
#     last_month = (today.replace(day=1) - timedelta(days=1)).strftime("%Y-%m")
#     last_month_api_spend = datong_api_df[datong_api_df['dt'].dt.to_period("M") == last_month]['spend'].sum()
#     last_month_total_spend = datong_api_df[datong_api_df['dt'].dt.to_period("M") == last_month]['total_spend'].sum()
#     per_last_month_spend = (last_month_api_spend/last_month_total_spend)*100

#     # Number of Active Ad Accounts
#     active_ad_accounts = datong_api_df['ad_account_id'].nunique()

#      # Get today's date to identify the current and last month
#     today = datetime.now().date()
#     current_month_period = today.strftime("%Y-%m")  # e.g., "2024-10"
#     last_month_period = (today.replace(day=1) - timedelta(days=1)).strftime("%Y-%m")  # Previous month in "YYYY-MM" format

#     # Filter for current month
#     current_month_data = datong_api_df[datong_api_df['dt'].dt.to_period("M") == current_month_period]
#     # Filter for last month
#     last_month_data = datong_api_df[datong_api_df['dt'].dt.to_period("M") == last_month_period]


#     col1, col2, col3 = st.columns(3)

#     # Display Metrics
#     col1.metric("Overall API Spend (YTD)", f"₹{Overall_api_spend:,.2f}")
#     col2.metric("Overall Total Spend (YTD)", f"₹{Overall_tot_spend:,.2f}")
#     col3.metric("Percentage of Overall Spend (YTD)", f"{per_ovr_spend:,.2f}%")
#     # col1.metric("Today's Spend", f"${today_spend:,.2f}",f"{tdy_spend_change:,.2f}%")


#     col1.metric("Today API Spend", f"₹{today_api_spend:,.2f}")
#     col2.metric("Today Total Spend", f"₹{today_tot_spend:,.2f}")
#     col3.metric("Percentage of Today Spend", f"{per_today_spend:,.2f}%")
    
#     col1.metric("Yesterday API Spend", f"₹{yesterday_api_spend:,.2f}",f"{spend_change:,.2f}%")
#     col2.metric("Yesterday Total Spend", f"₹{yesterday_total_spend:,.2f}",f"{spend_change:,.2f}%")
#     col3.metric("Percentage of Yesterday Spend", f"{per_yst_spend:,.2f}%")


#     col1.metric("API Current Month Spend", f"₹{current_month_api_spend:,.2f}")
#     col2.metric("Total Current Month Spend", f"₹{current_month_total_spend:,.2f}")
#     col3.metric("Percentage of Current Month Spend", f"{per_current_month_spend:,.2f}%")


#     col1.metric("Last Month Spend", f"₹{last_month_api_spend:,.2f}")
#     col2.metric("Last Month Spend", f"₹{last_month_total_spend:,.2f}")
#     col3.metric("Percentage of Last Month Spend", f"{per_last_month_spend:,.2f}%")

#     # col2.metric("Active Ad Accounts", active_ad_accounts)

#     # Aggregate the spend values by the selected grouping
#     grouped_df = datong_api_df.groupby(['ad_account_id','ad_account_name','grouped_date'])[['spend','total_spend']].sum().reset_index()

#     st.line_chart(grouped_df, x='grouped_date', y=['spend', 'total_spend'])
   
#     # Display grouped data
#     st.header(f"Spend Data Ad Account Level- {grouping}")
#     pivot_df = grouped_df.pivot(index=['ad_account_name','ad_account_id'], columns='grouped_date', values=['spend','total_spend'])

    

#     # st.dataframe(grouped_df, use_container_width=True)
#     st.dataframe(pivot_df, use_container_width=True)
    
#     # Display full table
#     st.header("Full Table")
#     st.dataframe(datong_api_df, use_container_width=True)

   

elif selected == "Stripe lookup" and st.session_state.status == "verified":

    # Set your Stripe API key here
    stripe.api_key = stripe_key

    def get_balance_transaction_fee(balance_transaction_id):
        try:
            balance_txn = stripe.BalanceTransaction.retrieve(balance_transaction_id)
            fee_amount = balance_txn.fee  # in cents
            currency = balance_txn.currency.upper()
            return fee_amount, currency
        except Exception:
            return None, None

    def get_last_100_charges_by_billing_email(email):
        charges = stripe.Charge.list(limit=1000)
        matched_charges = [c for c in charges.data if c.billing_details and c.billing_details.email == email]
        return matched_charges

    def get_transaction_by_payment_intent(payment_intent_id):
        try:
            charge = stripe.Charge.list(payment_intent=payment_intent_id).data
            return charge[0] if charge else None
        except Exception:
            return None

    # Streamlit UI
    st.title("Stripe Transactions Lookup")
    

    st.header("Search by Email")
    email = st.text_input("Enter Email to Find Transactions")
    if st.button("Find Payments"):
        if email:
            with st.spinner("Searching the last 100 charges..."):
                transactions = get_last_100_charges_by_billing_email(email)
                
                if not transactions:
                    st.error(f"No transactions found for Email '{email}' in the last 100 charges.")
                else:
                    st.success(f"Found {len(transactions)} transaction(s) for Email '{email}'!")
                    data = []
                    for transaction in transactions:
                        fee_amount, fee_currency = (None, None)
                        if transaction.balance_transaction:
                            fee_amount, fee_currency = get_balance_transaction_fee(transaction.balance_transaction)
                        
                        amount = transaction.amount / 100
                        currency = transaction.currency.upper()
                        status = transaction.status.capitalize()
                        description = transaction.description or "No description"
                        date_str = datetime.utcfromtimestamp(transaction.created).strftime('%Y-%m-%d %H:%M:%S UTC')
                        payment_intent = transaction.payment_intent
                        charge_id = transaction.id
                        final_email = transaction.billing_details.email

                        fee_str = f"{(fee_amount / 100):.2f} {fee_currency}" if fee_amount is not None else "N/A"
                        
                        data.append({
                            "Charge ID": charge_id,
                            "Payment Intent ID": payment_intent,
                            "Status": status,
                            "Currency": currency,
                            "Amount": f"{amount:.2f} {currency}",
                            "Stripe Processing Fee": fee_str,
                            "Date": date_str,
                            "Description": description,
                            "Billing Email Used": final_email
                        })
                    
                    df = pd.DataFrame(data)
                    df = df[df['Status'] == 'Succeeded']
                    st.write("### Transactions Details")
                    st.dataframe(df)
        else:
            st.warning("Please enter a valid email address.")


    st.header("Search by Payment Intent ID")
    payment_intent_id = st.text_input("Enter Payment Intent ID")
    if st.button("Find Transaction"):
        if payment_intent_id:
            with st.spinner("Searching transaction details..."):
                transaction = get_transaction_by_payment_intent(payment_intent_id)
                if transaction:
                    fee_amount, fee_currency = (None, None)
                    if transaction.balance_transaction:
                        fee_amount, fee_currency = get_balance_transaction_fee(transaction.balance_transaction)
                    
                    amount = transaction.amount / 100
                    currency = transaction.currency.upper()
                    status = transaction.status.capitalize()
                    description = transaction.description or "No description"
                    date_str = datetime.utcfromtimestamp(transaction.created).strftime('%Y-%m-%d %H:%M:%S UTC')
                    payment_intent = transaction.payment_intent
                    charge_id = transaction.id
                    final_email = transaction.billing_details.email if transaction.billing_details else "N/A"

                    fee_str = f"{(fee_amount / 100):.2f} {fee_currency}" if fee_amount is not None else "N/A"

                    st.write("### Transaction Details")
                    st.json({
                        "Charge ID": charge_id,
                        "Payment Intent ID": payment_intent,
                        "Status": status,
                        "Currency": currency,
                        "Amount": f"{amount:.2f} {currency}",
                        "Stripe Processing Fee": fee_str,
                        "Date": date_str,
                        "Description": description,
                        "Billing Email Used": final_email
                    })
                else:
                    st.error("No transaction found for the given Payment Intent ID.")
        else:
            st.warning("Please enter a valid Payment Intent ID.")


elif selected == "Summary" and st.session_state.status == "verified":


    bm = st.selectbox("Choose BM", ["IND BM", "US BM"], index=0)

    if bm=="IND BM":
        df = df[df['currency_code'] == "INR"]
    if bm=="US BM":
        df = df[df['currency_code'] != "INR"]

    df['dt'] = pd.to_datetime(df['dt']).dt.date


    # Define date ranges
    today = datetime.now()
    yesterday = today - timedelta(days=1)
    last_7_days = today - timedelta(days=7)
    last_14_days = today - timedelta(days=14)
    last_30_days = today - timedelta(days=30)
    start_of_month = today.replace(day=1)
    start_of_last_month = (start_of_month - timedelta(days=1)).replace(day=1)
    end_of_last_month = start_of_month - timedelta(days=1)
    start_of_quarter = start_of_month.replace(month=((today.month - 1) // 3) * 3 + 1, day=1)
    start_of_year = today.replace(month=1, day=1)

    print("Today:", today.date())
    print("Yesterday:", yesterday.date())
    print("Last 7 Days:", last_7_days.date())
    print("Last 14 Days:", last_14_days.date())
    print("Last 30 Days:", last_30_days.date())
    print("Start of Month:", start_of_month.date())
    print("Start of Last Month:", start_of_last_month.date())
    print("End of Last Month:", end_of_last_month.date())
    print("Start of Quarter:", start_of_quarter.date())
    print("Start of Year:", start_of_year.date())

    # Helper function to calculate spend for a specific date range
    def calculate_spend(df, start_date, end_date=None):
        if end_date:
            return df[(df['dt'] >= pd.to_datetime(start_date).date()) & (df['dt'] <= pd.to_datetime(end_date).date())]['spend'].sum()
        return df[df['dt'] >= pd.to_datetime(start_date).date()]['spend'].sum()
    
    overall_spend = df['spend'].sum()
    total_this_year_spend = calculate_spend(df, start_of_year)
    total_this_quarter_spend = calculate_spend(df, start_of_quarter)
    total_this_month_spend = calculate_spend(df, start_of_month)
    total_last_month_spend = calculate_spend(df, start_of_last_month, end_of_last_month)
    total_yesterday_spend = calculate_spend(df, yesterday,yesterday)

    # Aggregate spend data
    aggregated_data = []
    for key, group in df.groupby(['business_name', 'company_name', 'ad_account_name', 'business_manager_name', 'ad_account_id','currency_code']):
        business_name, company_name, ad_account_name, business_manager_name, ad_account_id,currency_code = key
        latest_dt = group['dt'].max()
        
        total_spend = group['spend'].sum()
        this_year_spend = calculate_spend(group, start_of_year)
        this_quarter_spend = calculate_spend(group, start_of_quarter)
        this_month_spend = calculate_spend(group, start_of_month)
        last_month_spend = calculate_spend(group, start_of_last_month, end_of_last_month)
        yesterday_spend = calculate_spend(group, yesterday,yesterday)
        last_7_days_spend = calculate_spend(group, last_7_days)
        last_14_days_spend = calculate_spend(group, last_14_days)
        last_30_days_spend = calculate_spend(group, last_30_days)

        total_spend_contribution = (total_spend / overall_spend) * 100
        yesterday_contribution = (yesterday_spend / total_yesterday_spend) * 100 if total_spend > 0 else 0
        this_year_contribution = (this_year_spend / total_this_year_spend) * 100 if total_spend > 0 else 0
        this_quarter_contribution = (this_quarter_spend / total_this_quarter_spend) * 100 if total_spend > 0 else 0
        this_month_contribution = (this_month_spend / total_this_month_spend) * 100 if total_spend > 0 else 0
        last_month_contribution = (last_month_spend / total_last_month_spend) * 100 if total_spend > 0 else 0
        
        aggregated_data.append({
            'Business Name': business_name,
            'Company Name': company_name,
            'Ad Account Name': ad_account_name,
            'Business Manager Name': business_manager_name,
            'Ad Account ID': ad_account_id,
            'Currency': currency_code,
            'Latest Date': latest_dt,
            'Lifetime Spend': total_spend,
            'Lifetime Contribution (%)': total_spend_contribution,
            'This Year': this_year_spend,
            'This Year Contribution (%)': this_year_contribution,
            'This Quarter': this_quarter_spend,
            'This Quarter Contribution (%)': this_quarter_contribution,
            'This Month': this_month_spend,
            'This Month Contribution (%)': this_month_contribution,
            'Last Month': last_month_spend,
            'Last Month Contribution (%)': last_month_contribution,
            'Yesterday': yesterday_spend,
            'Yesterday Contribution (%)': yesterday_contribution,
            'Last 7 Days': last_7_days_spend,
            'Last 14 Days': last_14_days_spend,
            'Last 30 Days': last_30_days_spend,
            
        })

    # Create a new DataFrame for the summarized data
    summary_df = pd.DataFrame(aggregated_data).sort_values(by='This Year', ascending=False)
    summary_df = summary_df.merge(disabled_account_df[['ad_account_id', 'flag']], left_on='Ad Account ID',right_on='ad_account_id', how='left')
    summary_df['flag'] = summary_df['flag'].fillna('Active')
    # Move the flag column next to the currency column
    cols = list(summary_df.columns)
    cols.insert(5, cols.pop(cols.index('flag')))

    #drop ad_account_id column
    cols = [col for col in cols if col != 'ad_account_id']

    summary_df = summary_df[cols]

    # Streamlit App
    st.title("Meta Spend Summary")

    st.write(f"{bm} Spend Summary")
    st.dataframe(summary_df)
    
    st.title("Disabled Account Analysis")

    # Merge disabled_account_df with list_df on 'ad_account_id'
    merged_df = list_df.merge(disabled_account_df,on='ad_account_id', how='left')

    # Fill 'flag' column in merged_df with 'Active' where it is null or empty
    merged_df['flag'] = merged_df['flag'].fillna('Active')
    merged_df['flag'] = merged_df['flag'].replace('', 'Active')

    merged_df = merged_df [['euid_x','ad_account_id','ad_account_name_x','currency_x','flag','disable_date','reactivation_date','disable_reason','business_name','company_name','business_manager_name','business_manager_id','ad_account_created_at']]

    merged_df = merged_df.rename(columns={'ad_account_name_x':'ad_account_name','currency_x':'currency'})
    
    merged_df = merged_df.merge(roposo_acc_list_df, left_on='ad_account_id', right_on='Roposo', how='left')


    merged_df['Roposo'] = merged_df['Roposo'].fillna('')
    merged_df['Roposo'] = merged_df['Roposo'].apply(lambda x: 'Roposo' if x != '' else 'Others')


    st.write("Final DataFrame:")
    st.dataframe(merged_df, use_container_width=True)

    st.write("EUID X Disabled/Reactivated Accounts")
    df_counts = merged_df.groupby(['euid_x','flag']).size().reset_index(name='counts')
    df_pivot = df_counts.pivot(index='euid_x', columns='flag', values='counts')
    df_pivot['Total'] = df_pivot.sum(axis=1)
    st.dataframe(df_pivot, use_container_width=True)
    
    currency_options = ['All','INR','USD']
    currency = st.selectbox("Select BM", currency_options)
    if currency == 'All':
        merged_df = merged_df
    elif currency == 'INR':
        merged_df = merged_df[merged_df['currency'] == 'INR']
    else:
        merged_df = merged_df[merged_df['currency'] != 'INR']

    flag_options = ['All'] + list(merged_df['Roposo'].unique())
    flag = st.selectbox("Select Flag", flag_options)
    if flag == 'All':
        merged_df = merged_df
    else:
        merged_df = merged_df[merged_df['Roposo'] == flag]


    merged_df_monthly = merged_df.groupby([merged_df['ad_account_created_at'].dt.to_period('M'), 'flag'])['ad_account_id'].nunique().reset_index(name='unique_ad_account_ids')
    st.write("Monthly Unique Ad Account Creation VS Disabled/Reactivated Accounts")
    merged_df_monthly = merged_df_monthly.pivot(index='ad_account_created_at', columns='flag', values='unique_ad_account_ids')
    merged_df_monthly['Total'] = merged_df_monthly.sum(axis=1)
    st.dataframe(merged_df_monthly, use_container_width=True)

    daterange = st.date_input("Select Date Range", value=[datetime.now() - timedelta(days=30), datetime.now()])

    df = df[df['dt'] >= daterange[0]]
    df = df[df['dt'] <= daterange[1]]

    df = df.groupby('ad_account_id', as_index=False).agg({'spend': 'sum'})
    merged_df = merged_df.merge(df, on='ad_account_id', how='left')

    merged_df['spend'] = merged_df['spend'].fillna(0)

    st.dataframe(merged_df, use_container_width=True)




elif selected == "BM Summary" and st.session_state.status == "verified":

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

    # Display input boxes for each unique currency code other than 'USD'
    st.write("Enter conversion rates for the following currencies:")
   
    # Create columns dynamically based on the number of currencies
    cols = st.columns(4)  # Adjust the number of columns (3 in this case)

    # Iterate over non-USD currencies and display them in columns
    for idx, currency in enumerate(non_usd_currencies):
        default_value = default_values.get(currency, 1.0)  # Use default value if defined, otherwise 1.0
        with cols[idx % 4]:  # Rotate through the columns
            conversion_rates[currency] = st.number_input(
                f"{currency} to USD:", value=default_value, min_value=0.0, step=0.001, format="%.3f"
            )

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

elif selected == "BID - BUID Mapping" and st.session_state.status == "verified":

    st.title("BID - BUID Mapping")

    bid_buid_df = bid_buid_df[['bid','buid']]

    bid_selection = st.number_input("Enter BID", min_value=0, step=1, key='bid')

    st.dataframe(bid_buid_df[bid_buid_df['bid']==bid_selection], use_container_width=True)

    # buid_selection = st.number_input("Enter BUID", min_value=0, step=1, key='buid')

    # st.dataframe(bid_buid_df[bid_buid_df['buid']==buid_selection], use_container_width=True)




elif selected == "Raw Data" and st.session_state.status == "verified":
    st.title("Raw Data Page")
    st.write("This is where raw data will be displayed.")

    st.write("acc list")
    st.dataframe(account_list_df, use_container_width=True)

    st.write("datong_acc_list_df dump")
    st.dataframe(datong_acc_list_df, use_container_width=True)

    st.write("roposo acc list dump")
    st.dataframe(roposo_acc_list_df, use_container_width=True)

    df['dt'] = pd.to_datetime(df['dt'])

    st.write("Ad spends raw dump")

    # Use .date() to make default values compatible with st.date_input()
    start_date = st.date_input("Start Date", value=df['dt'].min().date())
    end_date = st.date_input("End Date", value=df['dt'].max().date())

    # Convert selected dates to datetime for filtering
    start_datetime = pd.Timestamp(start_date)
    end_datetime = pd.Timestamp(end_date)

    # Filter the DataFrame
    filtered_df = df[(df['dt'] >= start_datetime) & (df['dt'] <= end_datetime)]

    st.dataframe(filtered_df, use_container_width=True)

    st.write("Ad account and EUID list raw dump")
    st.dataframe(list_df, use_container_width=True)

    # st.write("Zocket AI Spends raw dump")
    # st.dataframe(ai_spends_df, use_container_width=True)

    st.write("FBI API spends raw dump")
    st.dataframe(ai_campaign_spends_df, use_container_width=True)

    st.write("Disabled accounts raw dump")
    st.dataframe(disabled_account_df, use_container_width=True)

    # st.write("Datong API raw dump")
    # st.dataframe(datong_api_df, use_container_width=True)

    st.write("FBI API spends raw dump")
    st.dataframe(ai_campaign_spends_df, use_container_width=True)


if selected == "Overages" and st.session_state.status == "verified":

    st.title("Payments dump")

    st.dataframe(payments_df, use_container_width=True)

    st.title("Overages dump")
    st.dataframe(overages_df, use_container_width=True)

    buid = st.number_input("Enter BUID", min_value=0, step=1, key='buid')

    st.write(f"Payments for BUID: {buid}")

    st.dataframe(payments_df[payments_df['zocket_user_id']==buid], use_container_width=True)

    st.write(f"Overages for BUID: {buid}")

    st.dataframe(overages_df[overages_df['user_id']==buid], use_container_width=True)

    st.title("Overages Summary")

    overages_df['overage_fee'] = overages_df['overage_fee'].astype(float)
    overages_df['month'] = pd.to_datetime(overages_df['payment_date']).dt.to_period('M')

    overages_summary = overages_df.groupby(['user_id','flag','month','currency']).agg({'overage_fee':'sum'}).reset_index()

    overages_summary = overages_summary.pivot(index=['user_id','flag','currency'], columns='month', values='overage_fee')

    st.dataframe(overages_summary, use_container_width=True)

    st.title("Payment Summary")

    payments_df['amount'] = payments_df['amount'].astype(float)
    payments_df['month'] = pd.to_datetime(payments_df['created_at']).dt.to_period('M')

    payments_summary = payments_df.groupby(['zocket_user_id','month']).agg({'amount':'sum'}).reset_index()

    payments_summary = payments_summary.pivot(index='zocket_user_id', columns='month', values='amount')

    st.dataframe(payments_summary, use_container_width=True)

elif selected == "US Finance Mappings" and st.session_state.status == "verified":

    finance_trxns_df = execute_query(query=us_finance_all_transactions_query)

    st.title("Finance Mappings")

    st.write("Finance Transactions dump")
    st.dataframe(finance_trxns_df, use_container_width=True)

    st.write("Finance Mappings")


    invoice_ids = st.text_input("Enter Invoice IDs", key='invoice_ids')

    if invoice_ids:
        invoice_ids_list = [x.strip() for x in invoice_ids.split(',') if x.strip()]
        finance_trxns_df = finance_trxns_df[finance_trxns_df['payment_transcation_id'].isin(invoice_ids_list)]
        st.dataframe(finance_trxns_df, use_container_width=True)



elif selected == "Overall Finance Mappings" and st.session_state.status == "verified":

    finance_trxns_df = execute_query(query=finance_all_trxn_query)

    st.title("Finance Mappings")

    st.write("Finance Transactions dump")
    st.dataframe(finance_trxns_df, use_container_width=True)

    st.write("Finance Mappings")


    invoice_ids = st.text_input("Enter Invoice IDs", key='invoice_ids')

    if invoice_ids:
        invoice_ids_list = [x.strip() for x in invoice_ids.split(',') if x.strip()]
        finance_trxns_df = finance_trxns_df[finance_trxns_df['payment_transcation_id'].isin(invoice_ids_list)]
        st.dataframe(finance_trxns_df, use_container_width=True)