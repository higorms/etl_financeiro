import oracledb
from funcs import create_table_if_not_exists

arquivo = open('sensivel/db.txt', 'r')
db = arquivo.read()
arquivo.close()
db = db.split()

oracledb.init_oracle_client(lib_dir = r"C:\Users\menez\Documents\Oracle_client\instantclient_23_4")
conn = oracledb.connect(user=db[0], password=db[1], dsn=db[2], 
                        config_dir=r"C:\Users\menez\Documents\Oracle_client\config", 
                        wallet_location=r"C:\Users\menez\Documents\Oracle_client\config", wallet_password=db[1])

print("Versão do banco de dados: ", conn.version)

create_table_if_not_exists(conn,'stock_data')