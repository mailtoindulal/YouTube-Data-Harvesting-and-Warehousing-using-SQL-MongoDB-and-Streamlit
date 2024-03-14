from calendar import c
from cgitb import text
import collections
import csv
from multiprocessing import connection
from xml.dom.xmlbuilder import DOMImplementationLS
from altair import Cursor
import psycopg2
from logging.config import IDENTIFIER
import pymongo
import sqlite3
import mysql.connector 
import MySQLdb
import re
import mysql.connector
from urllib import response
import streamlit as st
import requests
from pprint import pprint
import json
import json
from sqlalchemy import create_engine,text
import pandas as pd
import googleapiclient.discovery
st.title('Youtube Data Harvesting')


myclient = pymongo.MongoClient('mongodb://localhost:27017/')
db = myclient['youtube']
mydb = mysql.connector.connect(host="127.0.0.1",
                   user="root",
                   password="root",
                   database= "youtub"
                  )
mycursor = mydb.cursor(buffered=True)
response_API = "AIzaSyC2GRBYcmLQ_VHzoNlmRgAgQVN3iHbwO7Q"
youtube = googleapiclient.discovery.build("youtube", "v3",developerKey=response_API)
def channel_details(x):
  request = youtube.channels().list(
          part="snippet,contentDetails,statistics",
          id=x
      )
  response = request.execute()
  
  z = dict(title = response['items'][0]['snippet']['title'],
          channel_id = response['items'][0]['id'],
          des = response['items'][0]['snippet']['description'],
          joined = response['items'][0]['snippet']['publishedAt'],
          thumbnails = response['items'][0]['snippet']['thumbnails']['medium']['url'],
          sc = response['items'][0]['statistics']['subscriberCount'],
          vc = response['items'][0]['statistics']['videoCount'],
          views = response['items'][0]['statistics']['viewCount'] ) 
  return z 

    





col1, col2, col3 = st.columns(3)

with col1:
    
    button1 = st.button('data srtorage')
    text_input =st.text_input("enter channel id")
  
    if button1:
     
      details = channel_details(text_input)
      st.write(details)
      youtube.insert_one(channel_details(text_input))  
      
     
     
with col2:
    
       button2 = st.button('data change')
if button2:
 file_encoding = 'utf-8'
# Open and read the JSON file
 with open('datas.json', 'r',encoding=file_encoding) as file:
    

  data = json.load(file)


# Convert the parsed data to a pandas DataFrame
  df = pd.DataFrame(data)
  engine = create_engine('mysql://root:root@127.0.0.1/youtub')
# Write the DataFrame to a SQL table
  df.to_sql(name='yout', con=engine, if_exists='replace', index=False)

with col3:
    button3 = st.button('sql') 
    st.write("## :orange[Select any question to get Insights]")
    questions = st.selectbox('Questions',['1. Which channels have the most number of videos?',
                   '2. What are the top 10 most viewed videos and their respective channels?',
                    '3.What is the total number of views for each channel, and what are their corresponding channel names?'])
    engine = create_engine('mysql://root:root@127.0.0.1/youtub')
    connection=engine.connect()
    if questions == '1. Which channels have the most number of videos?':
        query=text('select title from yout order by vc')
        result=connection.execute(query)
        df = pd.DataFrame(result.fetchall())
        st.write(df)
    elif  questions == '2. What are the top 10 most viewed videos and their respective channels?' :
        query=text('select vc ,channel_id  from yout order by views')
        result=connection.execute(query)
        df = pd.DataFrame(result.fetchall())
        st.write(df) 
    elif  questions =='3.What is the total number of views for each channel, and what are their corresponding channel names?' :
        query=text('select views , channel_id from yout ')
        result=connection.execute(query)
        df = pd.DataFrame(result.fetchall())
        st.write(df)      
        

    



 
      