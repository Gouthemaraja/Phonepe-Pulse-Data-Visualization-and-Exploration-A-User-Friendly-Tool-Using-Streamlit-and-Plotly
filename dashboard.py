#importing necessary packages
from dash import Dash, html, dcc
import plotly.express as px
import pandas as pd
import streamlit as st
import numpy as np
import plotly.figure_factory as ff
import retrival as re

from urllib.request import urlopen
import json


st.set_page_config(
    page_title="phonepe pulse ",
    page_icon = "",
    layout="wide",
    initial_sidebar_state="expanded"
)

df = re.retrive_data()
year = df["qyear"].unique()
quater= df["quater"].unique()
states = df["state"].unique()
# print(year[4])
st.title("PhonePe Pulse: Data Visualization for 2018 to 2022")
st.title("Transactions and Users by transaction type")
c1,c2,c3,c4 = st.columns([1.5,1.5,2,3])

with c1:
    r1 = st.selectbox(
    "Select year:",
    (year),
    index = 4,
    
)
    
with c2:
    r2 = st.selectbox(
    "Select quater:",
    (quater),
    index=0,
    
    
)
   
with c3:
    option = ["transactions","users"]
    r3 = st.selectbox(
    "Select",
    (option),
    index=0,
    
    
)
with c4:
   
    r4 = st.selectbox("select state" ,(states),
    index=0,
    key = "select state"
)
    
#     # print(r1)
# print(r2)
if r3=="transactions":
    df1=re.retrive_trans_data(r1,r2,r4)
    df1 = df1.groupby(["state","payment_category"]).sum().reset_index()
    c1,c2 = st.columns([5,5])
    with c1:
        fig1 = px.pie(df1 ,names = "payment_category",values = "count_Cr",title = "User count")
        st.plotly_chart(fig1)
    with c2:
        fig2 = px.pie(df1 ,names = "payment_category",values = "amount_Cr",title = "Amount")
        st.plotly_chart(fig2)

else:
    df2 = re.retrive_user_data(r1,r2,r4)
    c1,c2 = st.columns([10,5])
    with c1:
        fig = px.bar(df2,x = "state", y = ["registeredUsers","appOpens"],barmode="group")
        st.plotly_chart(fig)
    with c2:
        st.table(df2)


st.title("Users by phone brand")

u1,u2 = st.columns([5,5])
with u1:
    r5 = st.selectbox(
    "Select year for brand :",
    (year),
    index = 4,
    
    )
with u2:
    r6 = st.selectbox(
    "Select quater for brand",
    (quater),
    index=0,
    
    )
  

dfbrand = re.retrive_brand_users(r5,r6)
# print(dfbrand)
# print(r6)
c1,c2,c3 = st.columns([5,2,5])
with c1:
    fig = px.bar(dfbrand,"brand","count")
    st.plotly_chart(fig)

st.markdown('<style>.stColumn { margin-right: 20px; }</style>', unsafe_allow_html=True)

with c3:
    st.dataframe(dfbrand,width = 1000)


st.title("Transactions by zones in state")
c1,c2,c3,c4 = st.columns([1.5,1.5,2,3])

with c1:
    r1 = st.selectbox(
    "Select year for district:",
    (year),
    index = 4,
    
)
    
with c2:
    r2 = st.selectbox(
    "Select quater for district:",
    (quater),
    index=0,
    
    
)
   
with c3:
    option = ["transactions","users"]
    r3 = st.selectbox(
    "Select one option ",
    (option),
    index=0,
    
    
)
with c4:
   
    r4 = st.selectbox("select state" ,(states),
    index=0,
    key = "select one state"
)
c1,s,c2 = st.columns([5,1,5])
if r3=="transactions":
    with c1:
    
        df = re.retrive_trans_dictrict(r1,r2,r4)
        fig = px.bar(df,x = "zone" , y= "amount",barmode='group',title = "Amount and Count by Zone")
        st.plotly_chart(fig)
    with c2:
        df = re.retrive_trans_dictrict(r1,r2,r4)
        fig = px.bar(df,x = "zone" , y= "count",barmode='group',title = "Amount and Count by Zone")
        st.plotly_chart(fig)


else:
    with c1:
    
        df = re.retrive_user_dictrict(r1,r2,r4)
        fig = px.bar(df,x = "zone" , y= "registeredUsers",title = "registeredUsers")
        st.plotly_chart(fig)
    with c2:
        df = re.retrive_user_dictrict(r1,r2,r4)
        fig = px.bar(df,x = "zone" , y= "appOpens",title = "appOpens")
        st.plotly_chart(fig)


st.title("Top transactions in india")


c1,c2,c3 = st.columns([3,3,3])

with c1:
    r1 = st.selectbox(
    "Select year for top user:",
    (year),
    index = 4,
    
)
    
with c2:
    r2 = st.selectbox(
    "Select quater for top user:",
    (quater),
    index=0,
    
    
)
   
with c3:
    option = ["transactions","users"]
    r3 = st.selectbox(
    "Select one ",
    (option),
    index=0,
    
    
)
c1,c2,c3 = st.columns([5,5,5])
with c1:
    t1 = st.button("District")
with c2:
    t2 = st.button("Pincode")
with c3:
    t3 = st.button("state")

if r3 == "transactions":
    c1,s,c2 = st.columns([5,1,5])
    if t1 :
        df = re.retrive_top_district_trans(r1,r2)
        with c1:
            
            st.plotly_chart(px.pie(df,"district","amount"))

        with c2:
            st.plotly_chart(px.pie(df,"district","count"))

    if t2 :
        df = re.retrive_top_pincode_trans(r1,r2)
        with c1:
            
            st.plotly_chart(px.pie(df,"pincode","amount"))

        with c2:
            st.plotly_chart(px.pie(df,"pincode","count"))

    if t3 :
        df = re.retrive_top_state_trans(r1,r2)
        with c1:
            
            st.plotly_chart(px.pie(df,"state","amount"))

        with c2:
            st.plotly_chart(px.pie(df,"state","count"))


# print(t1)
    