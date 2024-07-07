# importando funções úteis
from funcs import stock_data, dividendos
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

data = stock_data(api_key, symbols) # Função que acessa, extrai e armazena os dados (JSON) desejados da API da Alpha Vantage

df = pd.DataFrame(data) # Tranforma os dados aramzenados da API em um dataframe do pandas
print(df)

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
data_div = dividendos(api_key, symbols)
df_div = pd.DataFrame(data_div)
print(df_div)

# Laço que transfere os dados do dataframe para a tabela de dividendos no banco de dados. Os dados que já existem no banco são ignorados, evitando informações duplicadas dentro do banco
for record in df_div.itertuples(index=False):
        cursor.execute("""
        MERGE INTO dividendos dst
        USING (
            SELECT :symbol AS symbol, TO_DATE(:ex_dividend_date, 'YYYY-MM-DD') AS ex_dividend_date, 
            TO_DATE(:declaration_date, 'YYYY-MM-DD') AS declaration_date, 
            TO_DATE(:record_date, 'YYYY-MM-DD') AS record_date, TO_DATE(:payment_date, 'YYYY-MM-DD') AS payment_date, 
            :amount AS amount FROM dual) src
        ON (dst.symbol = src.symbol AND dst.payment_date = src.payment_date)
        WHEN MATCHED THEN 
            UPDATE SET dst.ex_dividend_date = src.ex_dividend_date, 
                       dst.declaration_date = src.declaration_date, 
                       dst.record_date = src.record_date, 
                       dst.amount = src.amount 
        WHEN NOT MATCHED THEN
            INSERT (symbol, ex_dividend_date, declaration_date, record_date, payment_date, amount)
            VALUES (src.symbol, src.ex_dividend_date, src.declaration_date, src.record_date, src.payment_date, src.amount)
        """, {
            'symbol': record.symbol, 
            'ex_dividend_date': record.ex_dividend_date, 
            'declaration_date': record.declaration_date, 
            'record_date': record.record_date, 
            'payment_date': record.payment_date, 
            'amount': record.amount
        })

        conn.commit() # confirma e grava as momdificações feitas no banco de dados.
cursor.close()

print("Dados gravados com sucesso na nuvem.")