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

rds_host = "buymore2.cegnfd8ehfoc.ap-south-1.rds.amazonaws.com"
name = "postgres"
password = "buymore2"
##mod1
db_name = "products"


def lambda_handler(event, context):
    print(event)
    body = json.loads(event['body'])
    print("body===")
    print(body)
    file = body["files"]
    print(file)

    try:
        conn = psycopg2.connect(host=rds_host,
                                database=db_name,
                                user=name,
                                password=password)
        cur = conn.cursor()

        url = file
        response = requests.get(url)

        with open('/tmp/out.csv', 'w') as f:
            writer = csv.writer(f)
            for line in response.iter_lines():
                writer.writerow(line.decode('utf-8').split(','))
        data1 = pd.read_csv('/tmp/out.csv')
        ##mod2
        data1.amazon_updated_at.fillna(datetime.datetime.now(), inplace=True)

        data1.to_csv('/tmp/z10.csv')
        data4 = pd.read_csv('/tmp/z10.csv')
        print(data4)
        df = pd.DataFrame(data4)

        print("df======", df)

        # drop all rows with any NaN and NaT values
        df1 = df.dropna(axis=1)
        print("df2=======", df1)
        first_column = df1.columns[0]
        df7 = df1.drop([first_column], axis=1)
        df7.to_csv('/tmp/z15.csv', index=False)

        ###to save column name in one variable
        data5 = pd.read_csv('/tmp/z10.csv')
        d = pd.read_csv('/tmp/z15.csv', nrows=1).columns.tolist()
        print("ddddd>>>", d)
        ###full column name stored in (d1)
        d1 = d
        print("ddddd1>>>", d1)

        # now drop that unique column from main csv file(d) we will use d to remove column from sql column which is already present in user csv
        d.remove("product_id")
        print("dddd2>>>>>", d)

        engine = create_engine(
            'postgresql://postgres:buymore2@buymore2.cegnfd8ehfoc.ap-south-1.rds.amazonaws.com/products')

        query = "select product_id from amazon_amazonproducts"
        sql = pd.read_sql(query, engine)
        sql.to_csv('/tmp/z25.csv')

        cc = list(data5['product_id'])
        dd = sql[sql['product_id'].isin(cc)]
        dd.to_csv('/tmp/z22.csv', index=False)
        ddd = pd.read_csv('/tmp/z22.csv')
        ##main logic to extract full data which is matching to user's entered product_id
        a = []
        for i, j in ddd.iterrows():
            print(j['product_id'])
            k = j['product_id']
            a.append(k)
            print("k==", k)
        print("outer==a==", a)
        # to remove square brcket
        final = str(a)[1:-1]
        print(final)
        query = "select * from amazon_amazonproducts where product_id in " + "(" + str(final) + ")"
        print(query)
        sql = pd.read_sql(query, engine)
        sql.to_csv('/tmp/z25.csv', index=False)
        # first_column = dd.columns[0]
        # ddd = dd.drop([first_column], axis=1)
        # ddd.to_csv('z31.csv', index=False)

        # data from sql
        z = pd.read_csv('/tmp/z25.csv')
        print("zz", z)
        z.drop(d, axis=1, inplace=True)
        print("last", z.head())
        z.to_csv("/tmp/z32.csv", index=False)

        df11 = pd.read_csv("/tmp/z15.csv")
        df12 = pd.read_csv("/tmp/z32.csv")
        df22 = pd.merge(left=df12, right=df11, left_on='product_id', right_on='product_id')
        # df22 = pd.concat([df12, df11])
        df22.to_csv('/tmp/z33.csv')
        first_column = df22.columns[0]
        df7 = df22.drop([first_column], axis=1)
        df7.to_csv('/tmp/z34.csv', index=False)
        df10 = pd.read_csv("/tmp/z34.csv")
        # working perfectly
        iters = df10.iterrows()
        ##mod7
        for index, row in iters:
            cur.execute(
                'UPDATE "amazon_amazonproducts" SET "amazon_portal_sku" = %s , "amazon_unique_id" = %s, "amazon_listing_id" = %s, "amazon_price_rule" = %s,"amazon_break_even_sp" = %s,"amazon_min_break_even_sp" = %s,"amazon_max_break_even_sp" = %s, "amazon_vendors_price" = %s,"amazon_purchase_order_value" = %s,"amazon_current_selling_price" = %s,"amazon_upload_selling_price" = %s,"amazon_competitor_lowest_price" = %s,"amazon_account_id" = %s, "amazon_all_values_external_api" = %s,"amazon_created_at" = %s,"amazon_updated_at" = %s, "amazon_portal_category_id" = %s WHERE "amazon_amazonproducts"."product_id" = %s',
                [row['amazon_portal_sku'], row['amazon_unique_id'], row['amazon_listing_id'], row['amazon_price_rule'],
                 row['amazon_break_even_sp'],
                 row['amazon_min_break_even_sp'], row['amazon_max_break_even_sp'], row['amazon_vendors_price'],
                 row['amazon_purchase_order_value'], row['amazon_current_selling_price'],
                 row['amazon_upload_selling_price'], row['amazon_competitor_lowest_price'], row['amazon_account_id'],
                 row['amazon_all_values_external_api'], row['amazon_created_at'],
                 row['amazon_updated_at'], row['amazon_portal_category_id'],
                 row['product_id']])
            conn.commit()
            conn.close()
        ##mod8
        return {
            'statusCode': 200,
            'Message': "File upload successful",
            'body': json.dumps('Hello from partial Import amazonproducts lambda!')
        }
    except Exception as e:
        return {
            'statusCode': 404,
            'Message': "File upload error",
            'error': {e},
            'body': json.dumps('Hello from partial Import amazonproducts lambda!')
        }


