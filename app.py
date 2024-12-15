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
    (SELECT  ad_account_id,date(date_start) as dt,max(spend)spend
    from  ad_account_spends aas 
	group by 1,2
    ),
    total_payment AS
        (select ad_account,sum(adspend_amount) total_paid 
        from payment_trans_details td
        group by 1,2)

select c.app_business_id as euid,eu.business_name,eu.company_name,dt,coalesce(b.name,d.name) as ad_account_name,
coalesce(c.name,e.name) as business_manager_name,
COALESCE(b.currency,d.currency,'INR') as currency_code,a.ad_account_id,spend,
case when a.ad_account_id
        in
        ('act_517235807318296',
        'act_331025229860027',
        'act_1026427545158424',
        'act_818603109556933',
        'act_245995025197404',
        'act_3592100964402439',
        'act_3172162799744723',
        'act_1980162379033639',
        'act_1364907264123936',
        'act_749694046972238',
        'act_1841833786300802',
        'act_206144919151515',
        'act_324812700362567',
        'act_3505294363025995',
        'act_7780020542024454',
        'act_650302000225354',
        'act_1769761460112751',
        'act_659696249436257',
        'act_1729204737559911',
        'act_383479978116390',
        'act_1729204737559911',
        'act_1065735074925239',
        'act_2018452351931643',
        'act_1307983750616862',
        'act_1521400741909811',
        'act_2954031968090066') then 'Datong' 
        when c.app_business_id in (2310,2309,2202,2201,2181,2168,2100,2051,2281,2394) then 'FB Boost'
        when c.app_business_id in (1911)then 'Adfly' 
        when c.app_business_id in  ( 527, 785, 1049, 1230, 1231) or a.ad_account_id ='act_797532865863232' then 'Eleganty'
        when a.ad_account_id in 
        ('act_926201962276582',
            'act_1518996148767815',
            'act_283182437693605',
            'act_870986978485811',
            'act_2977476152390374',
            'act_1584376045533840',
            'act_741576947416981',
            'act_476910184892869',
            'act_898889421329382',
            'act_1575019052979670',
            'act_1808581176617313',
            'act_884037987128219',
            'act_3506352382952497',
            'act_489316499946843',
            'act_607352157650024',
            'act_281403371310608',
            'act_151947304471405',
            'act_1068783194317542',
            'act_1068783194317542',
            'act_1292987141870282',
            'act_24221841557403126',
            'act_965249685184093',
            'act_565292762205849',
            'act_722518396265984',
            'act_1784123958655548',
            'act_901747601927271',
            'act_427844130047005',
            'act_1282562349131228',
            'act_1192025228177331',
            'act_688943616115812',
            'act_767160144989024',
            'act_308308454982919',
            'act_1083450619593533',
            'act_1653390585242405',
            'act_169277159280041',
            'act_889366715657115',
            'act_604278331059492',
            'act_1860659564374272',
            'act_873580428253310',
            'act_216902994241137',
            'act_5922789224424039',
            'act_735898258036410',
            'act_1306548849911815',
            'act_514009980689341',
            'act_1097129248477609',
            'act_1058565595757779',
            'act_881404577110091',
            'act_6105820032831596',
            'act_1394810381350847',
            'act_780922136928954',
            'act_2283830365110063',
            'act_1356626195134046',
            'act_880735654213569',
            'act_8766140423423832',
            'act_203005676060367',
            'act_1434558773983739',
            'act_1081617916800011',
            'act_589148723475422',
            'act_1096127282058443',
            'act_1273797260327201',
            'act_1122759572702577',
            'act_584527074259791',
            'act_254077160677497',
            'act_989565319878933',
            'act_889351973307670',
            'act_280940417737704',
            'act_3816960438516519',
            'act_2250289218672133',
            'act_1190414661670653',
            'act_1655128932085551',
            'act_292305926681452',
            'act_148992597824742',
            'act_1109580570350734',
            'act_3368605639943487',
            'act_627328960104400',
            'act_1576810802933554',
            'act_1069296384990185',
            'act_431263196219676',
            'act_1106653140826024',
            'act_557343190568045',
            'act_719717626460829',
            'act_1160684574649777',
            'act_825903966195572',
            'act_1455994885788787',
            'act_3613726142202247',
            'act_1583482562266651',
            'act_1949783425508754',
            'act_1129008205233310',
            'act_2067912740221796',
            'act_1789118071860690',
            'act_3741550799489465',
            'act_942819674438412',
            'act_886514543466261',
            'act_1223815532022788',
            'act_1241443840476355',
            'act_985610253609004',
            'act_1649931122255134',
            'act_612684737776138',
            'act_294008606434173',
            'act_189234800895571',
            'act_570111399050038',
            'act_533459472189951',
            'act_541172815361037',
            'act_1430955031173023',
            'act_164546532672807',
            'act_602613569385841',
            'act_1076609240606982',
            'act_417067534179569',
            'act_953514250166844',
            'act_397827242568247',
            'act_1100595954813761',
            'act_1708627536373549',
            'act_185568974220256',
            'act_2552378681618018',
            'act_1075356080721083',
            'act_893226298453970',
            'act_1318334285559258',
            'act_2569213876607328',
            'act_1294766011720496',
            'act_558388417004205',
            'act_1096414978286208',
            'act_1237873617243932',
            'act_1198598904358357',
            'act_1077926824004942',
            'act_1239769893841877',
            'act_583622110895013',
            'act_1116376310060202',
            'act_3996805340594743',
            'act_349751667850753',
            'act_3602799093345049',
            'act_725548192392270',
            'act_545773498304947',
            'act_3827416530834864',
            'act_653664360149199',
            'act_905633270654663',
            'act_3357095267872666',
            'act_5813305362113378',
            'act_598865381850585',
            'act_268437995509783',
            'act_527282746010422',
            'act_957109429531250',
            'act_3563973227209697',
            'act_2059544207742648',
            'act_6915108528593741',
            'act_723792699245884',
            'act_225215876674518',
            'act_589022446887185',
            'act_1733494130732206',
            'act_571119348702020',
            'act_789733129886592',
            'act_1143525397390351',
            'act_586902686585383',
            'act_504437282593792',
            'act_1644634809454304',
            'act_1343163873314208',
            'act_2951094505024021',
            'act_759315738654233',
            'act_1616818125847387',
            'act_4136352729838579',
            'act_1445453332977761',
            'act_2481281658838112',
            'act_1085376779696148') and dt>='2024-10-01' then 'Roposo' 
        else 'Others' end as top_customers_flag
from 
    spends a
    left join fb_ad_accounts b on a.ad_account_id = b.ad_account_id
    left join fb_business_managers c on c.id = b.app_business_manager_id
    left join enterprise_users eu on eu.euid=a.euid
    order by euid,dt desc
    '''

# enterprise_users='''select * from enterprise_users'''

# fb_ad_accounts='''select * from fb_ad_accounts'''

# fb_business_managers='''select * from fb_business_managers'''

sub_query = '''SELECT * FROM
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
currency, plan_amount,plan_limit,flag,
datediff('day',sub_start,expiry_date) as total_subscription_days,
(datediff('day',sub_start,current_date)) as subscription_days_completed,
case when datediff('day',sub_start,expiry_date)=0 then 0 else (plan_limit/datediff('day',sub_start,expiry_date)) end as expected_per_day_spend,
(adspend_added+transfer_amount) as adspends_added
FROM
(
SELECT  a.euid,a.ad_account_id,ad_account_name,a.created_at,a.expiry_date, currency,
usd_amount as plan_amount,
case when currency='INR' then price_range else usd_price_range end as plan_limit,transfer_amount,flag, sum(adspend_added) as adspend_added

FROM
(
SELECT  a.euid,a.ad_account_id,a.created_at,a.expiry_date,a.amount as subamt,sp.plan_id,
 ad_account_name,
coalesce(efaa.currency,faa.currency,'INR') as currency,
 sp.plan_name, sp.usd_amount,sp.usd_price_range,sp.price_range, transfer_amount,flag
FROM
(
select distinct payment_id, euid, 
case when ad_account_id = 'act_1735955147217127' then '1735955147217127' else ad_account_id end as ad_account_id,
case when transfer_euid is not null then 'Transfered' else 'New' end as flag,
(transfer_amount)transfer_amount,
date(usd.created_at) created_at, expiry_date,amount,plan_id
from user_subscription_data usd 
where  coalesce(tranferred_at,expiry_date) >= current_date and id!=1952
-- group by 1,2,3,4
order by 3
)a
left join 
fb_ad_accounts faa on concat('act_',a.ad_account_id) = faa.ad_account_id
left join 
(
SELECT ad_account_id,ad_account_name,currency
FROM
(
select ad_account_id,ad_account_name,fb_user_id,row_number() over(partition by ad_account_id order by fb_user_id) as rank,currency
from 
(select * from enterprise_facebook_ad_account
)a
)a
where rank=1 
) efaa
on a.ad_account_id = efaa.ad_account_id 
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
SELECT distinct b.app_business_id as euid, a.ad_account_id, a.name as ad_account_name, b.name as business_manager_name,eu.business_name,eu.company_name
FROM fb_ad_accounts a
	LEFT JOIN fb_business_managers b ON b.id = a.app_business_manager_id
   left join enterprise_users eu on b.app_business_id=eu.euid
union all
SELECT distinct b.app_business_id as euid, a.ad_account_id, a.name as ad_account_name, b.name as business_manager_name,eu.business_name,eu.company_name
FROM z_b.fb_ad_accounts a
	LEFT JOIN z_b.fb_business_managers b ON b.id = a.app_business_manager_id
   left join enterprise_users eu on b.app_business_id=eu.euid
   order by 1 desc
'''

top_spends_query='''

SELECT euid,dt,ad_account_id,ad_account_name,business_manager_name,currency_code as currency,
-- case when currency_code = 'INR' then cast(spend as text)
-- when currency_code='EUR' then cast(spend*1.09 as text)
-- when currency_code='GBP' then cast(spend*1.3 as text)
-- when currency_code='AUD' then cast(spend*0.66 as text)
-- when currency_code='USD' then cast(spend as text) end as converted_spend,
-- case when currency_code = 'INR' then 'INR'
-- when currency_code='EUR' then 'USD'
-- when currency_code='GBP' then 'USD'
-- when currency_code='AUD' then 'USD'
-- when currency_code='USD' then 'USD' end as converted_currency,
spend as spend
FROM
(SELECT
 a.euid,a.ad_account_id,fba.name as ad_account_name ,fbm.name as business_manager_name,
 case when a.ad_account_id = 'act_507277141809499' then 'USD'
when a.ad_account_id = 'act_1250764673028073' then 'USD'
when fba.currency is null then 'INR' 
else fba.currency end as currency_code,date_start as dt, spend
    from  
    (select business_manager_id,name,max(app_business_id)app_business_id,max(id)id
     from fb_business_managers
     group by 1,2) fbm 
left join 
fb_ad_accounts fba on fbm.id=fba.app_business_manager_id
LEFT join ad_account_spends a on a.ad_account_id=fba.ad_account_id
)

'''
# union_df = pd.concat([df1, df2])

# print(union_df)


ai_spends_query = '''

SELECT
business_id as euid,account_name as ad_account_name,cs.ad_account_id,currency as currency_code,bp.name as business_name,date(date_start)dt,b.name,sum(spend)spend
from zocket_global.fb_campaign_age_gender_metrics_v3 cs
left join 
(select ad_account_id,account_type,min(business_id)business_id , min(business_manager_id)business_manager_id
from zocket_global.fb_ad_accounts 
GROUP by 1,2 ) faa_ind 
on cs.ad_account_id=faa_ind.ad_account_id
LEFT join zocket_global.business_profile bp on faa_ind.business_id=bp.id
left join (SELECT name,business_manager_id from zocket_global.fb_child_business_managers) b 
on faa_ind.business_manager_id = b.business_manager_id
where faa_ind.account_type ='ZOCKET'
group by 1,2,3,4,5,6,7
order by 3
'''

#fb campaigns level query
# zocket_ai_campaigns_spends_query = '''
# select
#     date(date_start) as dt,
#     fcaa.business_id,
#     gc.campaign_name,
#     SUM(ggci.spend)spend,
#     ggci.ad_account_id,
#     ggci.currency,
#     ggci.account_name
# FROM
#  zocket_global.fb_campaigns gc 
#     join zocket_global.fb_campaign_age_gender_metrics_v3 ggci on gc.campaign_id = ggci.campaign_id 
#     left join zocket_global.fb_ad_accounts fcaa on ggci.ad_account_id= fcaa.ad_account_id
# where date(date_start)>='2024-09-01'
# group by
# 1,2,3,5,6,7

# '''

#adlevel query
zocket_ai_campaigns_spends_query='''
select
ggci.ad_account_id,ggci.currency,date(date_start) as dt,account_name,c.id as campaign_id,c.name as campaign_name,SUM(ggci.spend)spend
FROM
    zocket_global.campaigns c
    join zocket_global.fb_campaigns gc on gc.app_campaign_id = c.id 
    join zocket_global.fb_adsets fbadset on gc.id = fbadset.campaign_id
    join zocket_global.fb_ads fbads on fbadset.id = fbads.adset_id
    join zocket_global.fb_ads_age_gender_metrics_v3 ggci on ggci.ad_id = fbads.ad_id
where date(date_start)>='2024-01-01'
and c.imported_at is null
group by 1,2,3,4,5,6
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
select eu.euid,COALESCE(b.name,d.name)as name,a.ad_account_id,a.account_status,disable_reason,dateadd('minute',330,a.created_at) as dt,
COALESCE(b.currency,d.currency)as currency,
COALESCE(c.name,e.name)as bm_name,
 row_number() over(partition by a.ad_account_id order by a.created_at desc) as rw,
 lag(a.account_status,1) over(PARTITION by a.ad_account_id order by dateadd('minute',330,a.created_at)) as prev_status,
 lag(dateadd('minute',330,a.created_at),1) over(PARTITION by a.ad_account_id order by dateadd('minute',330,a.created_at)) as prev_dt
-- from "dev"."public"."ad_account_webhook" a
from "dev"."z_b"."ad_account_webhook" a
left join fb_ad_accounts b on a.ad_account_id = b.ad_account_id
left join fb_business_managers c on c.id = b.app_business_manager_id
left join z_b.fb_ad_accounts d on a.ad_account_id = d.ad_account_id
left join z_b.fb_business_managers e on e.id = d.app_business_manager_id
left join enterprise_users eu on c.app_business_id=eu.euid
order by 3
)
)
where flag !='Others'
'''

datong_api_query='''

SELECT (euid::float)euid,ad_account_name,aas.ad_account_id,currency_code,aas.dt,spend,total_spend
,(spend/total_spend)*100 as per
from
(select euid,date(date_start)dt,ad_account_id,spend as total_spend from ad_account_spends )aas
left join 
(
select
ggci.ad_account_id,ggci.currency as currency_code,date(date_start) as dt,account_name as ad_account_name,SUM(ggci.spend)spend
FROM
    zocket_global.campaigns c
    join zocket_global.fb_campaigns gc on gc.app_campaign_id = c.id 
    join zocket_global.fb_adsets fbadset on gc.id = fbadset.campaign_id
    join zocket_global.fb_ads fbads on fbadset.id = fbads.adset_id
    join zocket_global.fb_ads_age_gender_metrics_v3 ggci on ggci.ad_id = fbads.ad_id
where date(date_start)>='2024-01-01'
and c.imported_at is null
group by 1,2,3,4
)ai
on aas.ad_account_id=ai.ad_account_id AND date(aas.dt)=date(ai.dt)
where aas.ad_account_id in (
    'act_517235807318296',
        'act_331025229860027',
        'act_1026427545158424',
        'act_818603109556933',
        'act_245995025197404',
        'act_3592100964402439',
        'act_3172162799744723',
        'act_1980162379033639',
        'act_1364907264123936',
        'act_749694046972238',
        'act_1841833786300802',
        'act_206144919151515',
        'act_324812700362567',
        'act_3505294363025995',
        'act_7780020542024454',
        'act_650302000225354',
        'act_1769761460112751',
        'act_659696249436257',
        'act_1729204737559911',
        'act_383479978116390',
        'act_1729204737559911'
)
'''




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
ai_spends_df = execute_query(query=ai_spends_query)
ai_campaign_spends_df = execute_query(query=zocket_ai_campaigns_spends_query)
disabled_account_df = execute_query(query=disabled_account_query)
datong_api_df = execute_query(query=datong_api_query) 


#chaning proper format of date
df['dt'] = pd.to_datetime(df['dt']).dt.date
df['spend'] = pd.to_numeric(df['spend'], errors='coerce')
df['euid'] = pd.to_numeric(df['euid'], errors='coerce')
df =  df.fillna("Unknown")

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
datong_api_df['dt'] = pd.to_datetime(datong_api_df['dt']).dt.date
datong_api_df['spend'] = pd.to_numeric(datong_api_df['spend'], errors='coerce')
datong_api_df['euid'] = pd.to_numeric(datong_api_df['euid'], errors='coerce')
datong_api_df['total_spend'] = pd.to_numeric(datong_api_df['total_spend'], errors='coerce')
datong_api_df['per'] = pd.to_numeric(datong_api_df['per'], errors='coerce')

#Sidebar
with st.sidebar:
    selected = option_menu(
        menu_title="Navigation",  # Required
        options=["Login","Key Account Stats", "Raw Data","Overall Stats - Ind","Overall Stats - US","Revenue-Analysis","Euid - adaccount mapping","Top accounts","AI account spends","FB API Campaign spends","Disabled Ad Accounts","Datong API VS Total Spends"],  # Required
        icons=["lock","airplane-engines", "table","currency-rupee",'currency-dollar','cash-coin','link',"graph-up","robot","suit-spade","slash-circle","joystick"],  # Optional: icons from the Bootstrap library
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
    last_month_df = filtered_df[(filtered_df['dt'].apply(pd.to_datetime).dt.month == current_month-1) & (filtered_df['dt'].apply(pd.to_datetime).dt.year == current_year)]

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
    grouped_df = filtered_df.groupby(['euid','ad_account_id','ad_account_name','business_manager_name','business_name','grouped_date'])[['spend']].sum().reset_index()
    pivot_df = grouped_df.pivot(index=['euid','ad_account_id','ad_account_name','business_manager_name','business_name'], columns='grouped_date', values='spend')
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


    yst_stats_df = filtered_df[filtered_df['dt'] == yesterday].groupby(['euid','ad_account_id','ad_account_name','business_manager_name','business_name'], as_index=False)['spend'].sum().sort_values(by='spend', ascending=False).reset_index(drop=True)
    yst_stats_df.index +=1
    current_month_stats_df = filtered_df[filtered_df['dt'].apply(lambda x: x.month == current_month and x.year == current_year)].groupby(['euid','ad_account_id','ad_account_name','business_manager_name','business_name'], as_index=False)['spend'].sum().sort_values(by='spend', ascending=False).reset_index(drop=True)
    current_month_stats_df.index +=1
    Overall_spend = filtered_df.groupby(['euid','ad_account_id','ad_account_name','business_manager_name','business_name'], as_index=False)['spend'].sum().sort_values(by='spend', ascending=False).reset_index(drop=True)
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
    summary_df = summary_df.groupby(['euid','ad_account_id','business_name','company_name','month','spend_yesterday','spend_curr_month','spend_total'])['spend'].sum().reset_index()
    summary_df = summary_df.pivot(index=['euid','ad_account_id','business_name','company_name','spend_total','spend_curr_month','spend_yesterday'], columns='month', values='spend')


    # Sort the columns by date
    summary_df = summary_df[sorted(summary_df.columns, key=lambda x: pd.to_datetime(x, format='%b-%y'), reverse=True)]

    st.title("Spend Data Ad Account Level")
    st.dataframe(summary_df, use_container_width=True)


    #display full table

elif selected == "Raw Data" and st.session_state.status == "verified":
    st.title("Raw Data Page")
    st.write("This is where raw data will be displayed.")

    # st.write("tes")
    # st.dataframe(df, use_container_width=True)

    st.write("Enterprise spends raw dump")
    st.dataframe(df, use_container_width=True)

    st.write("Subscriptions raw dump")
    st.dataframe(sub_df, use_container_width=True)

    st.write("Ad account and EUID list raw dump")
    st.dataframe(list_df, use_container_width=True)

    st.write("Zocket AI Spends raw dump")
    st.dataframe(ai_spends_df, use_container_width=True)

    st.write("FBI API spends raw dump")
    st.dataframe(ai_campaign_spends_df, use_container_width=True)

    st.write("Disabled accounts raw dump")
    st.dataframe(disabled_account_df, use_container_width=True)

    st.write("Datong API raw dump")
    st.dataframe(datong_api_df, use_container_width=True)

    st.write("FBI API spends raw dump")
    st.dataframe(ai_campaign_spends_df, use_container_width=True)


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

    # Calculate the total spend for the current month
    ind_current_month_spend = ind_current_month_df['spend'].sum()

    # Display the metrics
    col1, col2, col3 = st.columns(3)

    # Metric 1: Yesterday's Spend and change %
    col1.metric("Yesterday Spend", f"₹{ind_yst_spend}", f"{ind_spend_change:,.2f}%")

    # Metric 2: Number of Ad Accounts and change %
    col2.metric("Active Ad Accounts Yesterday", ind_num_ad_accounts_yesterday, f"{ind_ad_account_change}%")

    # Metric 3: Average Spend per Ad Account and change %
    # col3.metric("Avg Spend per Ad Account yesterday", f"₹{round(ind_avg_spend_per_account_yesterday, 2)}", f"{avg_ind_spend_change}%")

    # Display the current month spend as a metric
    col3.metric(label="Current Month Spend", value=f"₹{ind_current_month_spend:,.2f}")


    st.write("Yesterday spend data:")
    st.dataframe(ind_yesterday_data[['euid','ad_account_id','ad_account_name','currency_code','business_name','company_name','spend','dt']].sort_values(by='spend', ascending=False).reset_index(drop=True), use_container_width=True)

    st.write("Current Month spend data:")
    ind_grouped_data_adacclevel = ind_current_month_df.groupby([pd.to_datetime(ind_current_month_df['dt']).dt.strftime('%b %y'), 'euid','ad_account_id','ad_account_name','currency_code','business_name','company_name'])['spend'].sum().reset_index(name='spend').sort_values(by='spend', ascending=False).reset_index(drop=True)
    ind_grouped_data_adacclevel.index += 1

    # #Dataframe grouped by month and year of dt
    # ind_grouped_data_adacclevel = indian_df[indian_df['dt'].dt.month == current_month].groupby([pd.to_datetime(indian_df['dt']).dt.strftime('%b %y'), 'euid'])['spend'].sum().reset_index(name='spend').sort_values(by='spend', ascending=False)
    st.dataframe(ind_grouped_data_adacclevel, use_container_width=True)

    # Get top 10 overall spending ad accounts
    top_spending_ad_accounts = indian_df.groupby(['ad_account_id', 'ad_account_name'])['spend'].sum().reset_index(name='total_spend').sort_values(by='total_spend', ascending=False).head(10)

    st.write("Top 10 Overall Spending Ad Accounts:")
    st.dataframe(top_spending_ad_accounts, use_container_width=True)


elif selected == "Overall Stats - US" and st.session_state.status == "verified":

    st.title("Overall Stats - US")

    us_df = df[df['currency_code'].str.lower() != 'inr']
    ai_spends_df = ai_spends_df[ai_spends_df['currency_code'].str.lower() != 'inr']

    us_df = us_df[['euid', 'ad_account_name',  'ad_account_id','currency_code', 'dt','spend']]

    st.dataframe(us_df, use_container_width=True)

    # business_id,account_name,cs.ad_account_id,currency,date(date_start)dt,sum(spend)spend

    ai_spends_df = ai_spends_df[['euid', 'ad_account_name',  'ad_account_id','currency_code', 'dt','spend']]

    us_df = pd.concat([us_df,ai_spends_df], ignore_index=True)

    #remove duplicates
    us_df = us_df.drop_duplicates(subset=['ad_account_id', 'dt'], keep='last')

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
                        'AUD': 0.65
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

    # Calculate the total spend for the current month
    us_current_month_spend = us_current_month_df['spend'].sum()

    # Display the metrics
    col1, col2, col3 = st.columns(3)

    # Metric 1: Yesterday's Spend and change %
    col1.metric("Yesterday Spend", f"${us_yst_spend}", f"{us_spend_change}%")

    # Metric 2: Number of Ad Accounts and change %
    col2.metric("Ad Accounts", us_num_ad_accounts_yesterday, f"{us_ad_account_change}%")

    # Display the current month spend as a metric
    col3.metric(label="Current Month Spend (in USD)", value=f"${us_current_month_spend:,.2f}")

    st.write("Yesterday spend data:")
    st.dataframe(us_df[us_df['dt']==yesterday].sort_values(by='spend_in_usd', ascending=False).reset_index(drop=True), use_container_width=True)
    
    st.write("Current Month spend data:")
    us_grouped_data_adacclevel = us_current_month_df.groupby([pd.to_datetime(us_current_month_df['dt']).dt.strftime('%b %y'), 'euid', 'ad_account_id', 'ad_account_name', 'currency_code'])[[ 'spend','spend_in_usd']].sum().sort_values(by='spend_in_usd', ascending=False).reset_index()
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


elif selected == "Revenue-Analysis" and st.session_state.status == "verified":


    # Streamlit App
    st.title("Ad Subscription Dashboard")

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

    euid = st.number_input("Type an euid")

    filtered_list_df = list_df[list_df['euid'] == euid]
    st.dataframe(filtered_list_df, use_container_width=True)

elif selected == "Top accounts" and st.session_state.status == "verified":
    
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


    def convert_to_usd(row):
        if row['currency_code'] == 'USD':
            return row['spend']
        elif row['currency_code'] in conversion_rates:
            return row['spend'] * conversion_rates[row['currency_code']]
        return row['spend']

    usdtoinr = st.number_input("Enter conversion rates for the USD to INR:", min_value=0.0, value=84.2, step=0.01)

    # Create the 'spend_in_usd' column
    df['spend_in_usd'] = df.apply(lambda row: convert_to_usd(row), axis=1)
    df['spend_in_inr'] = df['spend_in_usd'] * usdtoinr

    st.dataframe(df, use_container_width=True)
    
    # Streamlit App
    st.title("Top 10 Businesses by Spend")

    # Currency Filter
    currency_option = st.selectbox("Select Currency", ["All", "INR", "USD"])
    if currency_option == "USD":
        filtered_df = df[df['currency'] != 'INR']
    if currency_option == "INR":
        filtered_df = df[df['currency'] == 'INR']
    if currency_option == "All":
        filtered_df = df

        # Display input boxes for each unique currency code other than 'USD'
    st.write("Enter conversion rates for the following currencies:")
   
    # Create columns dynamically based on the number of currencies
    cols = st.columns(5)  # Adjust the number of columns (3 in this case)

    # Iterate over non-USD currencies and display them in columns
    for idx, currency in enumerate(non_usd_currencies):
        default_value = default_values.get(currency, 1.0)  # Use default value if defined, otherwise 1.0
        with cols[idx % 5]:  # Rotate through the columns
            conversion_rates[currency] = st.number_input(
                f"{currency} to USD:", value=default_value, min_value=0.0, step=0.001, format="%.3f"
            )

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
    filtered_df = filtered_df[(filtered_df['dt'] >= pd.Timestamp(start_date)) & (filtered_df['dt'] <= pd.Timestamp(end_date))]

    # Aggregate spend per business and get the top 10
    top_spenders = (
        filtered_df.groupby([ "ad_account_id"])["spend"]
        .sum()
        .reset_index()
        .sort_values(by="spend", ascending=False)
        .head(n)
    )

    filtered_df = filtered_df[['euid', 'ad_account_id', 'ad_account_name','business_manager_name']]

    top_businesses = pd.merge(top_spenders, filtered_df, on="ad_account_id", how="left") \
                   .drop_duplicates(subset="ad_account_id") \
                   .sort_values(by="spend", ascending=False).reset_index(drop=True)

    top_businesses = top_businesses[['euid', 'ad_account_id', 'ad_account_name','business_manager_name', 'spend']]

    # Display top 10 businesses
    st.header("Top 10 Businesses by Spend")
    st.write(f"Showing data from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")

    st.dataframe(top_businesses, use_container_width=True)

elif selected == "AI account spends" and st.session_state.status == "verified":

    st.title("AI Spend Dashboard")

    grouping = st.selectbox('Choose Grouping', ['Year', 'Month', 'Week', 'Date'], index=1)

    ai_spends_df['dt'] = pd.to_datetime(ai_spends_df['dt'])

    col1,col2 = st.columns(2)
    with col1:
        start_date = st.date_input("Start Date", value=datetime(2024, 9, 1))
    with col2:
        end_date = st.date_input("End Date", value=datetime.now())

    ai_spends_df = ai_spends_df[(ai_spends_df['dt'] >= pd.to_datetime(start_date)) & (ai_spends_df['dt'] <= pd.to_datetime(end_date))]

    # User option to select time frame
    # time_frame = st.selectbox("Select Time Frame", ["Day", "Week", "Month", "Year"])

    # Today's and yesterday's dates for calculating metrics
    today = datetime.now().date()
    yesterday = today - timedelta(days=1)

       # Assuming your 'dt' column is already in date format (e.g., YYYY-MM-DD)
    if grouping == 'Year':
        ai_spends_df.loc[:, 'grouped_date'] = ai_spends_df['dt'].apply(lambda x: x.strftime('%Y'))  # Year format as 2024
    elif grouping == 'Month':
        ai_spends_df.loc[:, 'grouped_date'] = ai_spends_df['dt'].apply(lambda x: x.strftime('%b-%y'))  # Month format as Jan-24
    elif grouping == 'Week':
        ai_spends_df.loc[:, 'grouped_date'] = ai_spends_df['dt'].apply(lambda x: f"{x.strftime('%Y')} - week {x.isocalendar()[1]}")  # Week format as 2024 - week 24
    else:
        ai_spends_df.loc[:, 'grouped_date'] = ai_spends_df['dt']  # Just use the date as is (in date format)

    # Aggregate the spend values by the selected grouping
    grouped_df = ai_spends_df.groupby(['ad_account_name','ad_account_id','currency_code','grouped_date','name'])['spend'].sum().reset_index()
    # Metrics Calculation
    # Today's Spend
    today_spend = ai_spends_df[ai_spends_df['dt'].dt.date == today]['spend'].sum()
    # Yesterday's Spend
    yesterday_spend = ai_spends_df[ai_spends_df['dt'].dt.date == yesterday]['spend'].sum()
    # Spend Change from Yesterday
    spend_change = ((today_spend - yesterday_spend) / yesterday_spend * 100) if yesterday_spend != 0 else 0
    # Current Month Spend
    current_month_spend = ai_spends_df[ai_spends_df['dt'].dt.to_period("M") == today.strftime("%Y-%m")]['spend'].sum()
    # Last Month Spend
    last_month = (today.replace(day=1) - timedelta(days=1)).strftime("%Y-%m")
    last_month_spend = ai_spends_df[ai_spends_df['dt'].dt.to_period("M") == last_month]['spend'].sum()
    # Number of Active Ad Accounts
    active_ad_accounts = ai_spends_df['ad_account_id'].nunique()


    col1, col2 = st.columns(2)
    
    # Display Metrics
    col1.metric("Today's Spend", f"${today_spend:,.2f}")
    # st.metric("Change from Yesterday", f"{spend_change:.2f}%")
    col2.metric("Current Month Spend", f"${current_month_spend:,.2f}")
    col1.metric("Last Month Spend", f"${last_month_spend:,.2f}")
    col2.metric("Active Ad Accounts", active_ad_accounts)

    # Display grouped data
    st.header(f"Spend Data - {grouping} View")

    pivoted_df = grouped_df.pivot(index=['ad_account_name','ad_account_id','currency_code','name'], columns='grouped_date', values='spend')
    st.dataframe(pivoted_df, use_container_width=True)

    # Display full table
    st.header("Full Table")
    st.dataframe(ai_spends_df, use_container_width=True)


elif selected == "FB API Campaign spends" and st.session_state.status == "verified":

    st.title("FB API Campaign Spend Dashboard")

    ai_campaign_spends_df['spend'] = pd.to_numeric(ai_campaign_spends_df['spend'], errors='coerce')

    #Arrange key metrics in columns for better layout
    col1, col2 = st.columns(2)

    with col1:

    # Currency Filter
        currency_option = st.selectbox("Select BM", ["All", "IND BM", "US BM"], index=0)

        start_date = st.date_input("Start Date", value=datetime(2024, 9, 1))
    
    if currency_option == "IND BM":
        ai_campaign_spends_df = ai_campaign_spends_df[ai_campaign_spends_df['currency'] == "INR"]
    if currency_option == "US BM":
        ai_campaign_spends_df = ai_campaign_spends_df[ai_campaign_spends_df['currency'] != "INR"]
    else:
        ai_campaign_spends_df = ai_campaign_spends_df

    with col2:

        grouping = st.selectbox('Choose Grouping', ['Year', 'Month', 'Week', 'Date'], index=1)

        end_date = st.date_input("End Date", value=datetime.now())

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

    # Metrics Calculation
    # Today's Spend
            # if currency_option == "IND BM":
            #     today_spend = ai_campaign_spends_df[ai_campaign_spends_df['dt'].dt.date == today]['spend'].sum()
            # if currency_option == "US BM":
            #     today_spend = ai_campaign_spends_df[ai_campaign_spends_df['dt'].dt.date == today]['spend_in_usd'].sum()
            # else:
            #     today_spend = ai_campaign_spends_df[ai_campaign_spends_df['dt'].dt.date == today]['spend_in_inr'].sum() 
    today_spend = ai_campaign_spends_df[ai_campaign_spends_df['dt'].dt.date == today]['spend_in_usd'].sum() 
    Overall_spend = ai_campaign_spends_df['spend_in_usd'].sum() 

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
    col1.metric("Overall Spend (YTD)", f"${Overall_spend:,.2f}")
    # col1.metric("Today's Spend", f"${today_spend:,.2f}",f"{tdy_spend_change:,.2f}%")
    
    col2.metric("Yesterday Spend", f"${yesterday_spend:.2f}",f"{spend_change:,.2f}%")
    col1.metric("Current Month Spend", f"${current_month_spend:,.2f}")
    col2.metric("Last Month Spend", f"${last_month_spend:,.2f}")
    col1.metric("Average Spend per Account - Current Month", f"${avg_spend_current_month:,.2f}",f"{average_spend_change:.2f}%" if average_spend_change is not None else "N/A")
    col2.metric("Average Spend per Account - Last Month", f"${avg_spend_last_month:,.2f}")
    # col2.metric("Active Ad Accounts", active_ad_accounts)

    ai_campaign_spends_df = ai_campaign_spends_df[(ai_campaign_spends_df['dt'] >= pd.to_datetime(start_date)) & (ai_campaign_spends_df['dt'] <= pd.to_datetime(end_date))]

    # Aggregate the spend values by the selected grouping
    grouped_df = ai_campaign_spends_df.groupby(['ad_account_id','account_name','currency','grouped_date'])['spend'].sum().reset_index()
   
    # Display grouped data
    st.header(f"Spend Data Ad Account Level- {grouping}")
    pivot_df = grouped_df.pivot(index=['account_name','ad_account_id','currency'], columns='grouped_date', values='spend')

    # st.dataframe(grouped_df, use_container_width=True)
    st.dataframe(pivot_df, use_container_width=True)

   
    # Display grouped data
    st.header(f"Spend Data Ad Account Level - {grouping} USD View")
    usd_grouped_df = ai_campaign_spends_df.groupby(['ad_account_id','account_name','grouped_date'])['spend_in_usd'].sum().reset_index()
    pivot_df = usd_grouped_df.pivot(index=['account_name','ad_account_id'], columns='grouped_date', values='spend_in_usd')

    # st.dataframe(grouped_df, use_container_width=True)
    st.dataframe(pivot_df, use_container_width=True) 

    st.header("Campaign Level Data")

    # Aggregate the spend values by the selected grouping
    grouped_df = ai_campaign_spends_df.groupby(['ad_account_id','account_name','campaign_name','currency','grouped_date'])['spend'].sum().reset_index()
   
    # Display grouped data
    st.header(f"Spend Data Campaign Level- {grouping}")
    pivot_df = grouped_df.pivot(index=['account_name','ad_account_id','campaign_name','currency'], columns='grouped_date', values='spend')

    # st.dataframe(grouped_df, use_container_width=True)
    st.dataframe(pivot_df, use_container_width=True)

    # Display grouped data
    st.header(f"Spend Data Campaign Level - {grouping} USD View")
    usd_grouped_df = ai_campaign_spends_df.groupby(['ad_account_id','account_name','campaign_name','grouped_date'])['spend_in_usd'].sum().reset_index()
    usd_pivot_df = usd_grouped_df.pivot(index=['account_name','ad_account_id','campaign_name'], columns='grouped_date', values='spend_in_usd')

    st.dataframe(usd_pivot_df, use_container_width=True)
    
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

    st.title(f"{flag} Ad Accounts")

    if flag == "Disabled":
        disabled_account_df = disabled_account_df[disabled_account_df['flag'] == 'Disabled']
        disabled_account_df = disabled_account_df.loc[:, disabled_account_df.columns != 'reactivation_date'].reset_index(drop=True)
        disabled_account_df.index+=1
    else:
        disabled_account_df = disabled_account_df[disabled_account_df['flag'] == 'Reactivated'].reset_index(drop=True)
        disabled_account_df.index+=1

    st.dataframe(disabled_account_df, use_container_width=True)


elif selected == "Datong API VS Total Spends" and st.session_state.status == "verified":

    st.title("Datong API VS Total Spends")

    #warning message if currency contains other than inr
    if not datong_api_df['currency_code'].eq('INR').all():
        st.write("Warning: Currency column contains other than INR")

    datong_api_df['dt'] = pd.to_datetime(datong_api_df['dt'])

    st.dataframe(datong_api_df, use_container_width=True)

    #group by choosing date
    grouping = st.selectbox('Choose Grouping', ['Year', 'Month', 'Week', 'Date'], index=1)

    # Assuming your 'dt' column is already in date format (e.g., YYYY-MM-DD)
    if grouping == 'Year':
        datong_api_df.loc[:, 'grouped_date'] = datong_api_df['dt'].apply(lambda x: x.strftime('%Y'))  # Year format as 2024
    elif grouping == 'Month':
        datong_api_df.loc[:, 'grouped_date'] = datong_api_df['dt'].apply(lambda x: x.strftime('%b-%y'))  # Month format as Jan-24
    elif grouping == 'Week':
        datong_api_df.loc[:, 'grouped_date'] = datong_api_df['dt'].apply(lambda x: f"{x.strftime('%Y')} - week {x.isocalendar()[1]}")  # Week format as 2024 - week 24
    else:
        datong_api_df.loc[:, 'grouped_date'] = datong_api_df['dt']  # Just use the date as is (in date format)

    today = datetime.now().date()
    yesterday = today - timedelta(days=1)

    today_api_spend = datong_api_df[datong_api_df['dt'] == today]['spend'].sum()
    today_tot_spend = datong_api_df[datong_api_df['dt'] == today]['total_spend'].sum() 
    per_today_spend= (today_api_spend/today_tot_spend)*100

    Overall_api_spend = datong_api_df['spend'].sum()
    Overall_tot_spend = datong_api_df['total_spend'].sum() 
    per_ovr_spend = (Overall_api_spend/Overall_tot_spend)*100

    # Yesterday's api Spend
    yesterday_api_spend = datong_api_df[datong_api_df['dt'].dt.date == yesterday]['spend'].sum()
    day_before_yesterday_api_spend = datong_api_df[datong_api_df['dt'].dt.date == day_before_yst]['spend'].sum()
    

    # Yesterday's total Spend
    yesterday_total_spend = datong_api_df[datong_api_df['dt'].dt.date == yesterday]['total_spend'].sum()
    day_before_yesterday_total_spend = datong_api_df[datong_api_df['dt'].dt.date == day_before_yst]['total_spend'].sum()

    per_yst_spend = (yesterday_api_spend/yesterday_total_spend)*100
    per_day_before_yesterday_spend = (day_before_yesterday_api_spend/day_before_yesterday_total_spend)*100

    # Spend Change from Yesterday
    tdy_spend_change = ((today_api_spend - yesterday_api_spend) / yesterday_api_spend * 100) if yesterday_api_spend != 0 else 0
    spend_change = ((yesterday_api_spend - day_before_yesterday_api_spend) / day_before_yesterday_api_spend * 100) if day_before_yesterday_api_spend != 0 else 0

    # Spend Change from Yesterday
    tdy_spend_change = ((today_tot_spend - yesterday_total_spend) / yesterday_total_spend * 100) if yesterday_total_spend != 0 else 0
    spend_change = ((yesterday_total_spend - day_before_yesterday_total_spend) / day_before_yesterday_total_spend * 100) if day_before_yesterday_total_spend != 0 else 0

    # Current Month Spend
    current_month_api_spend = datong_api_df[datong_api_df['dt'].dt.to_period("M") == today.strftime("%Y-%m")]['spend'].sum()
    current_month_total_spend = datong_api_df[datong_api_df['dt'].dt.to_period("M") == today.strftime("%Y-%m")]['total_spend'].sum()
    per_current_month_spend = (current_month_api_spend/current_month_total_spend)*100

    # Last Month Spend
    last_month = (today.replace(day=1) - timedelta(days=1)).strftime("%Y-%m")
    last_month_api_spend = datong_api_df[datong_api_df['dt'].dt.to_period("M") == last_month]['spend'].sum()
    last_month_total_spend = datong_api_df[datong_api_df['dt'].dt.to_period("M") == last_month]['total_spend'].sum()
    per_last_month_spend = (last_month_api_spend/last_month_total_spend)*100

    # Number of Active Ad Accounts
    active_ad_accounts = datong_api_df['ad_account_id'].nunique()

     # Get today's date to identify the current and last month
    today = datetime.now().date()
    current_month_period = today.strftime("%Y-%m")  # e.g., "2024-10"
    last_month_period = (today.replace(day=1) - timedelta(days=1)).strftime("%Y-%m")  # Previous month in "YYYY-MM" format

    # Filter for current month
    current_month_data = datong_api_df[datong_api_df['dt'].dt.to_period("M") == current_month_period]
    # Filter for last month
    last_month_data = datong_api_df[datong_api_df['dt'].dt.to_period("M") == last_month_period]


    col1, col2, col3 = st.columns(3)

    # Display Metrics
    col1.metric("Overall API Spend (YTD)", f"₹{Overall_api_spend:,.2f}")
    col2.metric("Overall Total Spend (YTD)", f"₹{Overall_tot_spend:,.2f}")
    col3.metric("Percentage of Overall Spend (YTD)", f"{per_ovr_spend:,.2f}%")
    # col1.metric("Today's Spend", f"${today_spend:,.2f}",f"{tdy_spend_change:,.2f}%")


    col1.metric("Today API Spend", f"₹{today_api_spend:,.2f}")
    col2.metric("Today Total Spend", f"₹{today_tot_spend:,.2f}")
    col3.metric("Percentage of Today Spend", f"{per_today_spend:,.2f}%")
    
    col1.metric("Yesterday API Spend", f"₹{yesterday_api_spend:,.2f}",f"{spend_change:,.2f}%")
    col2.metric("Yesterday Total Spend", f"₹{yesterday_total_spend:,.2f}",f"{spend_change:,.2f}%")
    col3.metric("Percentage of Yesterday Spend", f"{per_yst_spend:,.2f}%")


    col1.metric("API Current Month Spend", f"₹{current_month_api_spend:,.2f}")
    col2.metric("Total Current Month Spend", f"₹{current_month_total_spend:,.2f}")
    col3.metric("Percentage of Current Month Spend", f"{per_current_month_spend:,.2f}%")


    col1.metric("Last Month Spend", f"₹{last_month_api_spend:,.2f}")
    col2.metric("Last Month Spend", f"₹{last_month_total_spend:,.2f}")
    col3.metric("Percentage of Last Month Spend", f"{per_last_month_spend:,.2f}%")

    # col2.metric("Active Ad Accounts", active_ad_accounts)

    # Aggregate the spend values by the selected grouping
    grouped_df = datong_api_df.groupby(['ad_account_id','ad_account_name','grouped_date'])[['spend','total_spend']].sum().reset_index()

    st.line_chart(grouped_df, x='grouped_date', y=['spend', 'total_spend'])
   
    # Display grouped data
    st.header(f"Spend Data Ad Account Level- {grouping}")
    pivot_df = grouped_df.pivot(index=['ad_account_name','ad_account_id'], columns='grouped_date', values=['spend','total_spend'])

    

    # st.dataframe(grouped_df, use_container_width=True)
    st.dataframe(pivot_df, use_container_width=True)
    
    # Display full table
    st.header("Full Table")
    st.dataframe(datong_api_df, use_container_width=True)

   