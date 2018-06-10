from __future__ import print_function
import httplib2
import argparse
import time
import datetime
import pyodbc
import json

from apiclient import discovery
from httplib2 import Http
from oauth2client import file, client, tools

SCOPES = 'https://www.googleapis.com/auth/spreadsheets'
CLIENT_SECRET_FILE = 'storage.json'
#APPLICATION_NAME = 'Google Sheets API Python Usage'

store= file.Storage('storage')
creds = store.get()
if not creds or creds.invalid:
        flags = argparse.ArgumentParser(parents=[tools.argparser]).parse_args()
        flow = client.flow_from_clientsecrets('client_secrets.json',SCOPES)
        creds = tools.run_flow(flow,store,flags)

SHEETS = discovery.build('Sheets','v4',http=creds.authorize(Http()))
data = {'properties':{'title': 'Usage_UBX [%s]' % time.ctime()}}
res = SHEETS.spreadsheets().create(body=data).execute()
SHEET_ID = res['spreadsheetId']
print('created "%s"' % res['properties']['title'])

FIELDS = ('Usage_Date','Usage','Product_Type','Usage_type')
json.dumps(FIELDS)
server = 'Instance details'
database = ''
username = ''
password = ''
driver='{ODBC Driver 13 for SQL Server}'
cnxn = pyodbc.connect('DRIVER='+driver+';SERVER='+server+';PORT=1443;DATABASE='+database+';UID='+username+';PWD='+ password)
cursor = cnxn.cursor()
#now = datetime.datetime.now()
raw=cursor.execute("select usage_date, sum(Usage_Counter) Usage,  Product_Type, Usage_type_code,1 from  [DWH].[Agg_Book_Chapter_Daily_usage] with (nolock) where Usage_Date between '2018-02-01' and '2018-$
#print (rows)
#row = cursor.fetchone()
cnxn.close()
raw.insert(0,FIELDS)
#data = {'values':[row[:2] for row in rows]}
#rangeName = 'A1:C'
DATA = {
            'values':[row[:4] for row in raw],
        }

SHEETS.spreadsheets().values().update(spreadsheetId=SHEET_ID,range='A1',body=DATA,valueInputOption='RAW').execute()
#SHEETS.spreadsheets().values().update(spreadsheetId=SHEET_ID,range='A1','valueInputOption'='RAW',body=data).execute()
print('wrote data')
rows = SHEETS.spreadsheets().values().get(spreadsheetId=SHEET_ID,range='Sheet1').execute().get('values',[])
for row in rows:
      print(row)
