# importando funções úteis
from funcs import stock_data, dividendos, symbol_sa
from funcs import create_table_if_not_exists, create_table_if_not_exists_divid
import requests
import pandas as pd
import oracledb

# Fazendo leitura de um arquivo .txt que contem dados da chave de acesso da API
arquivo = open('sensivel/key.txt', 'r')
api_key = arquivo.read()
arquivo.close()

# Fazendo leitura de um arquivo .txt que contem dados dos simbolos das ações 
arquivo = open('sensivel/simbolos.txt', 'r')
syms = arquivo.read()
arquivo.close()
symbols = syms.split()
symb_sa = symbol_sa()

data = stock_data(api_key, symbols) # Função que acessa, extrai e armazena os dados (JSON) desejados da API da Alpha Vantage

df = pd.DataFrame(data) # Tranforma os dados aramzenados da API em um dataframe do pandas

# Fazendo leitura de um arquivo .txt que contem dados do banco de dados autônomo da Oracle
arquivo = open('sensivel/db.txt', 'r')
db = arquivo.read()
arquivo.close()
db = db.split()

# Realizando conexão com o banco de dados Oracle
oracledb.init_oracle_client(lib_dir = r"C:\Users\menez\Documents\Oracle_client\instantclient_23_4")
conn = oracledb.connect(user=db[0], password=db[1], dsn=db[2], 
                        config_dir=r"C:\Users\menez\Documents\Oracle_client\config", 
                        wallet_location=r"C:\Users\menez\Documents\Oracle_client\config", wallet_password=db[1])

create_table_if_not_exists(connection = conn, table_name = 'stock_data') # Função que cria uma tabela chamada Stock_data dentro do banco de dados da Oracle, caso ela ainda não exista
cursor = conn.cursor()
# Laço que transfere os dados do dataframe para a tabela stock_data no banco de dados. Os dados que já existem no banco são ignorado, evitando informações duplicadas dentro do banco
for record in df.itertuples(index=False):
        cursor.execute("""
        MERGE INTO stock_data dst
        USING (
            SELECT :symbol AS symbol, 
            TO_DATE(:data, 'YYYY-MM-DD') AS data, 
            :abertura AS abertura, 
            :alta AS alta, 
            :baixa AS baixa, 
            :fechamento AS fechamento 
            FROM dual 
        ) src
        ON (dst.symbol = src.symbol AND dst.data = src.data)
        WHEN MATCHED THEN 
            UPDATE SET dst.abertura = src.abertura, 
                       dst.alta = src.alta, 
                       dst.baixa = src.baixa, 
                       dst.fechamento = src.fechamento 
        WHEN NOT MATCHED THEN
            INSERT (symbol, data, abertura, alta, baixa, fechamento)
            VALUES (src.symbol, src.data, src.abertura, src.alta, src.baixa, src.fechamento)
        """, {
            'symbol': record.symbol, 
            'data': record.data, 
            'abertura': record.abertura, 
            'alta': record.alta, 
            'baixa': record.baixa, 
            'fechamento': record.fechamento
        })
        conn.commit() # confirma e grava as momdificações feitas no banco de dados.

create_table_if_not_exists_divid(connection = conn, table_name = 'dividendos') # Função que cria uma tabela chamada Dividendos dentro do banco de dados da Oracle, caso ela ainda não exista

# Extrai os dados da API de dividendos e armazena em um dataframe
data_div = dividendos(symbols)

# Laço que transfere os dados do dataframe para a tabela de dividendos no banco de dados. Os dados que já existem no banco são ignorados, evitando informações duplicadas dentro do banco
for record in data_div.itertuples(index=False):
        cursor.execute("""
        MERGE INTO dividendos dst
        USING (
            SELECT :symbol AS symbol, TO_DATE(:Data_COM, 'DD/MM/YYYY') AS Data_COM, 
            TO_DATE(:Data_pay, 'DD/MM/YYYY') AS Data_pay, 
            :Valor AS Valor FROM dual) src
        ON (dst.symbol = src.symbol AND dst.Data_COM = src.Data_COM)
        WHEN MATCHED THEN 
            UPDATE SET dst.Data_pay = src.Data_pay, 
                       dst.Valor = src.Valor 
        WHEN NOT MATCHED THEN
            INSERT (symbol, Data_COM, Data_pay, Valor)
            VALUES (src.symbol, src.Data_COM, src.Data_pay, src.Valor)
        """, {
            'symbol': record.symbol, 
            'Data_COM': record.Data_COM, 
            'Data_pay': record.Pagamento, 
            'Valor': record.Valor
        })

        conn.commit() # confirma e grava as modificações feitas no banco de dados.
cursor.close()

print("Dados gravados com sucesso na nuvem.")