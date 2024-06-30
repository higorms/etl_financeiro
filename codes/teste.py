from funcs import stock_data
from funcs import create_table_if_not_exists
import requests
import pandas as pd
import oracledb

arquivo = open('sensivel/key.txt', 'r')
api_key = arquivo.read()
arquivo.close()

arquivo = open('sensivel/simbolos.txt', 'r')
syms = arquivo.read()
arquivo.close()
symbols = syms.split()

data = stock_data(api_key, symbols)

df = pd.DataFrame(data)

arquivo = open('sensivel/db.txt', 'r')
db = arquivo.read()
arquivo.close()
db = db.split()
#df.to_csv('output.csv', index=False)

oracledb.init_oracle_client(lib_dir = r"C:\Users\menez\Documents\Oracle_client\instantclient_23_4")
conn = oracledb.connect(user=db[0], password=db[1], dsn=db[2], 
                        config_dir=r"C:\Users\menez\Documents\Oracle_client\config", 
                        wallet_location=r"C:\Users\menez\Documents\Oracle_client\config", wallet_password=db[1])

create_table_if_not_exists(connection = conn, table_name = 'stock_data')

for record in df.itertuples(index=False):
        cursor = conn.cursor()
        cursor.execute("""
        INSERT INTO stock_data (symbol, data, abertura, alta, baixa, fechamento)
        VALUES (:1, TO_DATE(:2, 'YYYY-MM-DD'), :3, :4, :5, :6)""", 
        (record.symbol, record.date, record.open_price, record.high_price, record.low_price, record.close_price ))

cursor.close()
conn.commit()

