import pandas as pd
from sqlalchemy import create_engine
import cx_Oracle
import logging

# create mysql database commection
from Scripts.config import *

#mysql_engine = create_engine('mysql+pymysql://root:Admin%40143@localhost:3308/retaildwh')
mysql_engine = create_engine(f'mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}')
# Create Oracle engine
#oracle_engine = create_engine('oracle+cx_oracle://system:admin@localhost:1521/xe')
oracle_engine = create_engine(f'oracle+cx_oracle://{ORACLE_USER}:{ORACLE_PASSWORD}@{ORACLE_HOST}:{ORACLE_PORT}/{ORACLE_SERVICE}')



def load_csv_mysql(file_path,table_name):
    df = pd.read_csv(file_path)
    df.to_sql(table_name, mysql_engine, index=False, if_exists='replace')

def load_json_mysql(file_path,table_name):
    df = pd.read_json(file_path)
    df.to_sql(table_name, mysql_engine, index=False, if_exists='replace')

def load_xml_mysql(file_path,table_name):
    df = pd.read_xml(file_path, xpath='.//item')
    df.to_sql(table_name, mysql_engine, index=False, if_exists='replace')

def load_oracle_mysql(query,table_name):
    df = pd.read_sql(query, oracle_engine)
    df.to_sql(table_name, mysql_engine, index=False, if_exists='replace')


'''
def extract_sales_csv_mysql():
    df = pd.read_csv("data/sales_data.csv")
    df.to_sql("staging_sales",mysql_engine,index=False,if_exists='replace')

def extract_product_csv_mysql():
    df = pd.read_csv("data/product_data.csv")
    df.to_sql("staging_product", mysql_engine,index=False,if_exists='replace')

def extract_supplier_json_mysql():
    df = pd.read_json("data/supplier_data.json")
    df.to_sql("staging_supplier", mysql_engine,index=False,if_exists='replace')


def extract_inventory_xml_mysql():
    df = pd.read_xml("data/inventory_data.xml", xpath='.//item')
    df.to_sql("staging_inventory", mysql_engine,index=False,if_exists='replace')

def extract_oracle_mysql():
    df = pd.read_sql("select * from stores",oracle_engine)
    df.to_sql("staging_stores", mysql_engine,index=False,if_exists='replace')
'''

if __name__ == "__main__":
    print(" my data extraction started ...")

    '''
    extract_sales_csv_mysql()
    extract_product_csv_mysql()
    extract_supplier_json_mysql()
    extract_inventory_xml_mysql()
    extract_oracle_mysql()
    '''
    load_csv_mysql("data/sales_data.csv","staging_sales")
    load_csv_mysql("data/product_data.csv", "staging_product")
    load_json_mysql("data/supplier_data.json", "staging_supplier")
    load_xml_mysql("data/inventory_data.xml", "staging_inventory")
    load_oracle_mysql("select * from stores", "staging_stores")


    print(" my data extraction successfully completed ...")