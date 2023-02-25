import pandas as pd
from datetime import date
import logging

logging.basicConfig(filename='importancia.log', filemode='w', level=logging.DEBUG)
logging.info('Started Usa')
datos= pd.read_excel('dominante.xlsx')

job_final = []
for i in datos['jobtitle']:
    try:
        y = i.replace("&", "%20")
        y = y.replace(" ", "%20")
        job_final.append(y)
    except:
        pass

lista = []
for x in job_final:
    lista.append('https://www.linkedin.com/jobs/search?keywords='+str(x)+'&location=usa&f_TPR=r604800&position=1&pageNum=0')

import nest_asyncio
nest_asyncio.apply()
import aiohttp
import asyncio
import requests
from bs4 import BeautifulSoup
import time
async def main(i,status):
    while status!=200:
        time.sleep(1)
        try:
            async with aiohttp.ClientSession() as session:
                try:
                    async with session.get(i) as response:

                        status =response.status

                        html = await response.text()
                        try:
                            soup = BeautifulSoup(html, 'html.parser')
                            a = soup.find("h1", {"class": "results-context-header__context"}).get_text()
                            datos.append(a)
                        except Exception as e:
                            pass
                except Exception as e:
                    pass
        except Exception as e:
            pass
datos = []
status=3  
count =0
for i in lista[0:10]:
    time.sleep(1)
    count +=1

    asyncio.run(main(i,status))

import re    

df = pd.DataFrame(datos)
df_1 = df.replace(to_replace=r'\n', value='', regex=True)
importancia = []
jobs = []
new = []
for i in range(len(df_1)):
    importancia.append(df_1.iloc[i][0].split(" ")[0])
    palabra = re.sub("\d+", "", df_1.iloc[i][0].split(" Jobs")[0]).replace(",+ ","")
    jobs.append(palabra.lstrip())

user_list = pd.DataFrame(zip(importancia, jobs),columns=["importancia","jobs"])
user_list['country'] = 'USA'
today = date.today()
user_list['dates'] = today
#user_list.to_excel('ultimos_usa.xlsx')


from google.cloud import bigquery
import pandas
import pytz
client = bigquery.Client()
table_id = "prosfy-12037.Importancia.tabla_importancia"
job_config = bigquery.LoadJobConfig(
 
    schema = [
        bigquery.SchemaField("importancia","STRING"),
        bigquery.SchemaField("jobs","STRING"),
        bigquery.SchemaField("country", "STRING"),
        bigquery.SchemaField("dates", "DATE"),
    ],
    autodetect=False,
    source_format=bigquery.SourceFormat.CSV
)
job = client.load_table_from_dataframe(
    user_list, table_id,job_config=job_config
) 


job.result()
#user_list.to_excel('ultimos_usa.xlsx')
logging.info('Finished Usa')