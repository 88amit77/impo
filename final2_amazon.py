import datetime
import io
import os
import sys
# from io import StringIO
import csv
import requests
# import dropbox
import pandas as pd
import psycopg2
from sqlalchemy import create_engine
from django.db import connection
import json


def lambda_handler():
    # print(event)
    # body = json.loads(event['body'])
    # print("body===")
    # print(body)
    # file = body["files"]
    # print(file)
    try:
        url ='https://content.dropboxapi.com/apitl/1/Ah_JouRY1wGueX9rvadluIUbiEXb5U9n_yTN2IRKd5I3jVcqSWcAtLwCa1thOwzOJrqY8Oy0UiI1QPt5Z-_9QseO2bsEkmQBAdXg4W_VMxjXRekA9ld2NfBv88pU6ky0CaX_D8Ov9nnP4Iskuv0EewWCdNSweoPAkOvwITxBI9M_sHFX7VJCaedwcQsiHVJRyr6f6NuMakS0GCRSam_cEJCuuR5uYgyFKnX3Y52rkwGtHB_JxF2S1ESnxqWLazariW5f1rvDqD5QsuW6afTaaBr706MsvAR5UAWWGuHcTOQNKq4b90lol3inVS-r72pRbBGgbTF_T8PloZZgsA7iqIINqIhbS7Dr8APmFGCcn-td2v5ohdbMDg7LFUCpfs6wYVB0p2so2UKmg4Hp3OX20RXo'
        response = requests.get(url)

        with open('/tmp/out.csv', 'w') as f:
            writer = csv.writer(f)
            for line in response.iter_lines():
                writer.writerow(line.decode('utf-8').split(','))
        data1 = pd.read_csv('/tmp/out.csv')
        print(data1)
        # data1 = pd.read_csv('testing_data.csv')

        data1.amazon_created_at.fillna(datetime.datetime.now(), inplace=True)
        data1.amazon_updated_at.fillna(datetime.datetime.now(), inplace=True)
        data1.amazon_all_values_external_api.fillna("None", inplace=True)
        data1.amazon_break_even_sp.fillna(float(0.0), inplace=True)
        data1.amazon_min_break_even_sp.fillna(float(0.0), inplace=True)
        data1.amazon_max_break_even_sp.fillna(float(0.0), inplace=True)

        data1.to_csv('/tmp/data.csv', index=False)
        # this method is to remove duplication from user entered data and saving it to one csv file
        data2 = pd.read_csv('/tmp/data.csv')
        print("data2=====", data2)
        # data3 = data2.drop_duplicates(keep='first')
        # print(data1)
        # data3.to_csv('data4.csv', index=False)
        ####

        engine = create_engine(
            'postgresql://postgres:buymore2@buymore2.cegnfd8ehfoc.ap-south-1.rds.amazonaws.com/products')
        query = 'select product_id,amazon_unique_id from amazon_amazonproducts'
        sql = pd.read_sql(query, engine)
        print("sql2", sql)
        # sql1 = sql.to_csv('sql.csv', index=False)

        # ##main logic for data  validation between two user data and db data files
        cc = list(sql['product_id'])
        dd = data2[~data2['product_id'].isin(cc)]
        cc1 = list(sql['amazon_unique_id'])
        dd1 = dd[~dd['amazon_unique_id'].isin(cc1)]
        print("dddd1",dd1)
        # # dd.to_csv('z4.csv')

        dd1.to_sql(
            name='amazon_amazonproducts',
            con=engine,
            index=False,
            if_exists='append'
        )
        engine.dispose()
        return {
            'statusCode': 200,
            'Message': "File upload successful",
            'body': json.dumps('Hello from Import master product lambda!')
        }
    except Exception as e:
        return {
            'statusCode': 404,
            'Message': "File upload error",
            'error': {e},
            'body': json.dumps('Hello from Import master product lambda!')
        }

print(lambda_handler())