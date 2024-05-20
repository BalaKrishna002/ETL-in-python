from bs4 import BeautifulSoup
import requests
import pandas as pd
from datetime import datetime
import sqlite3


json_file = 'Countries_by_GDP.json'
db = 'World_Economies.db'
table_name = 'Countries_by_GDP'
log_file = 'logfile.txt'

# ETL Process

def extract():
    url = 'https://web.archive.org/web/20230902185326/https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29'
    r = requests.get(url).text
    soup = BeautifulSoup(r, 'html.parser')
    table = soup.find('table',class_="srn-white-background")
    rows = table.tbody.find_all('tr')

    dataframe = pd.DataFrame(columns=['Country','GDP_USD_billion'])

    for i,row in enumerate(rows):
        if i>=3:
            tds = row.find_all('td')
            country = tds[0].a.text
            gdp = tds[2].text
            dataframe = dataframe.append({'Country':country,'GDP_USD_billion':gdp},ignore_index=True)
            
    return dataframe

def transform(dataframe):
    dataframe['GDP_USD_billion'] = dataframe['GDP_USD_billion'].replace('—','0')
    dataframe['GDP_USD_billion'] = dataframe['GDP_USD_billion'].str.replace(',','').astype(int)
    dataframe['GDP_USD_billion'] = round(dataframe.GDP_USD_billion/1000,2)
        
    return dataframe

def load(dataframe):
    
    dataframe.to_json(json_file, orient='records', lines=True)

    conn = sqlite3.connect(db)
    dataframe.to_sql(table_name, conn, if_exists = 'replace', index = False)
    
    result = pd.read_sql('select * from '+table_name+' where GDP_USD_billion>100',conn)
    conn.close()
    return result

def logging(message):
    
    with open(log_file,'a') as f:
        f.write(datetime.now().strftime("%Y-%m-%d %H:%M:%S")+": "+message+"\n")
        print(datetime.now().strftime("%Y-%m-%d %H:%M:%S")+": "+message+"\n")
    
        
            
logging("ETL started")

logging("Extracting of data")
dataframe = extract()
logging("Extracting of data is Completed")

logging("Transforming of data")
transformed_data = transform(dataframe)
print("Transformed Data:\n")
print(transformed_data)
logging("Transforming of data is Completed")

logging("Loading of data")
result =load(transformed_data)
print("Display only the entries with more than a 100 billion USD economy:\n")
print(result)
logging("Loading of data is Completed")
