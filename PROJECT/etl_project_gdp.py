import pandas as pd 
from bs4 import BeautifulSoup as soup
import requests 
import sqlite3
from datetime import datetime
import numpy as np
import pprint



url="https://web.archive.org/web/20230902185326/https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29"
log_file="log_file.txt"
table_attribs= ["Country","GDP_USD_millions"]
db_name='World_Economies.db'
table_name='Countries_by_GDP'
csv_path='Countries_by_GDP.csv'
conn=sqlite3.connect('SQL.db')

def extract (url,table_attribs):
    
    html_obj= requests.get(url).text #extract the web page as text 
    data=soup(html_obj,'html.parser')#Parse the text into an HTML object.
    df=pd.DataFrame(columns=table_attribs)#Create an empty pandas DataFrame named df with columns as the table_attribs.
    tables = data.find_all('tbody')
    rows = tables[2].find_all('tr')
    print(type(rows[1]))
    for row in rows:
 
         col = row.find_all('td')
         if len(col)!=0 and (col[0].find_all('a')) and (col[2].contents[0]!='—') :
            country= col[0].text
            gdp=col[2].contents[0]
            dict={"Country":country,"GDP_USD_millions":gdp}
            df1 = pd.DataFrame(dict, index=[0])
            df=pd.concat([df,df1],ignore_index=True)
         
    return df

def transform (df):
    list=[]
    for str in df["GDP_USD_millions"]:
        list.append(round(float(''.join((str.split(','))))/1000,2))
    df["GDP_USD_millions"]=list
    df.rename(columns={"GDP_USD_millions": "GDP_USD_billions"}, inplace=True)
    return df


def load_to_csv(df, csv_path):
    df.to_csv(csv_path)

def load_to_db(df, csv_path):
    df.to_sql(table_name, conn, if_exists = 'replace', index =False)


def run_query(query_statement, conn):
    query_output = pd.read_sql(query_statement, conn)
    print(query_statement)
    print(query_output)


# def log_progress(message):



extracted_data=extract (url,table_attribs)
transformed_data=transform(extracted_data)
print(transformed_data)
load_to_csv(transformed_data,csv_path)
print('csv_saved')
load_to_db(transformed_data, csv_path)
print('Table is ready')
query_statement = f"SELECT * FROM {table_name}"
run_query(query_statement, conn)
