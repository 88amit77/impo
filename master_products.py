import os
import psycopg2
import dropbox
import csv
from datetime import datetime

rds_host = "buymore2.cegnfd8ehfoc.ap-south-1.rds.amazonaws.com"
name = "postgres"
password = "buymore2"
db_name = "products"
def lambda_handler():
    conn_users = psycopg2.connect(database="users", user="postgres", password="buymore2",
                                host="buymore2.cegnfd8ehfoc.ap-south-1.rds.amazonaws.com", port="5432")
    cur_users = conn_users.cursor()

    conn_products = psycopg2.connect(host=rds_host, database=db_name, user=name, password=password)
    cur_products = conn_products.cursor()
    #Status check
    # status_export_file_query = "Select * from api_export where file_type='MasterProduct' and status='Generating'"
    # cur_users.execute(status_export_file_query)
    # status_exports = cur_users.fetchall()
    #
    # if len(status_exports) > 0:
    #     return {'status1': False}

    export_file_query = "Select * from api_export where file_type='MasterProduct' and exfile_iscreated = FALSE LIMIT 1"
    cur_users.execute(export_file_query)
    exports = cur_users.fetchall()

    if len(exports) == 0:
        return {'status2': False}
    for export in exports:
        try:
            query_pruducts = 'SELECT master_masterproduct.*,master_brand.brand_name,calculation_currency.currency_name,calculation_hsncoderate.hsn_code,calculation_categoryrequirement.category_name ' \
                            'FROM master_masterproduct ' \
                            'join master_brand on master_masterproduct.brand_id_id =master_brand.brand_id ' \
                            'join calculation_currency on master_masterproduct.currency_id_id =calculation_currency.currency_id ' \
                            'join calculation_hsncoderate on master_masterproduct.hsn_code_id_id =calculation_hsncoderate.hsn_rate_id ' \
                            'join calculation_categoryrequirement on master_masterproduct.category_id_id = calculation_categoryrequirement.category_id ' \
                            'where hsn_code_id_id in(0) or brand_id_id in(63) or category_id_id in(0)'
            #if len(exports) == 0:
            # query_params
            # query_employees += ' where col_name = col_val and '

            cur_products.execute(query_pruducts)

            products = cur_products.fetchall()
            if len(products) <= 0:
                return {'status': False}

            cur_users.execute("Update api_export set status='Generating' where export_id=" + str(export[0]))
            conn_users.commit()

            data = [[
                'Product ID',
                'Product Name',
                'Buymore SKU',
                'Brand ID',
                "Brand Name",
                "Category ID",
                "Category Name",
                'Currency ID',
                "Currency Name",
                "HSN Code ID",
                'HSN Code',
                'Ean',
                "Product Mrp",
                "Product Length",
                "Product Breath",
                "Product Width",
                'Product Weight',
                "MIN Payout Value",
                "Max Payout Value",
                "Product Model No.",
                'Series Name',
                'Child Variations',
                "Description",
                "Sales Rank",
                "Image Url",
                "Key point",
                'Status',
                "Created At",
                "Updated At",

            ]]
            for product in products:
                # print(product)
                data.append([

                    product[0],
                    product[3],
                    product[1],
                    product[24],
                    product[25],
                    product[21],
                    product[28],
                    product[22],
                    product[26],
                    product[23],
                    product[27],
                    product[2],
                    product[4],
                    product[5],
                    product[6],
                    product[7],
                    product[8],
                    product[9],
                    product[10],
                    product[11],
                    product[12],
                    product[13],
                    product[14],
                    product[15],
                    product[16],
                    product[17],
                    product[18],
                    product[19],
                    product[20],
                ])
            file_name = 'exports-' + str(int(datetime.timestamp(datetime.now()))) + '.csv'
            file_from = '/tmp/' + file_name
            with open(file_from, 'w', newline='') as file:
                writer = csv.writer(file)
                writer.writerows(data)
            file_to = '/buymore2/products/' + file_name
            access_token = 'd7ElXR2Sr-AAAAAAAAAAC2HC0qc45ss1TYhRYB4Jy6__NJU1jjGiffP7LlP_2rrf'
            dbx = dropbox.Dropbox(access_token)
            file_size = os.path.getsize(file_from)
            with open(file_from, 'rb') as f:
                dbx.files_upload(f.read(), file_to, mode=dropbox.files.WriteMode.overwrite)

            cur_users.execute("Update api_export set exfile_path='" + file_to + "', exfile_iscreated=TRUE,"
                                                                                " exfile_name='" + file_name + "', exfile_size='" + str(
                file_size) + "', status='Generated' "
                             "where export_id=" + str(export[0]))
            conn_users.commit()
        except:
            file_name = 'exports_error-' + str(int(datetime.timestamp(datetime.now()))) + '.log'
            file_from = '/tmp/' + file_name
            with open(file_from, 'w', newline='') as file:
                file.write('Exception occurred')
            file_to = '/buymore2/products/logs/' + file_name
            access_token = 'd7ElXR2Sr-AAAAAAAAAAC2HC0qc45ss1TYhRYB4Jy6__NJU1jjGiffP7LlP_2rrf'
            dbx = dropbox.Dropbox(access_token)
            file_size = os.path.getsize(file_from)
            with open(file_from, 'rb') as f:
                dbx.files_upload(f.read(), file_to, mode=dropbox.files.WriteMode.overwrite)

            cur_users.execute(
                "Update api_export set exfile_errorlog='" + file_to + "',  status='Failed' where export_id=" + str(
                    export[0]))
            conn_users.commit()
    return {
        'status': True
    }

print(lambda_handler())