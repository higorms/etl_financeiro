import requests
import time
from bs4 import BeautifulSoup
import pandas as pd

def stock_data(api_key, symbols):
    all_data = []

    for symbol in symbols:
        url = f'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&apikey={api_key}'
        response = requests.get(url)
        stock_data = response.json()
    
        if 'Time Series (Daily)' in stock_data:
            time_series = stock_data['Time Series (Daily)']
            for date, data in time_series.items():
                all_data.append({
                    'symbol': symbol,
                    'data': date,
                    'abertura': data['1. open'],
                    'alta': data['2. high'],
                    'baixa': data['3. low'],
                    'fechamento': data['4. close'],
                })
        
        # Respeitar os limites de taxa da API
        time.sleep(12)  # Espera de 12 segundos para evitar limite de chamadas
    
    return all_data

def dividendos(symbols):
    all_divs = []

    for symbol in symbols:
        indicador = symbol[len(symbol)-2:]
        
        if indicador == '11':
            url = f'https://statusinvest.com.br/fundos-imobiliarios/{symbol}'
            HEADERS = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:98.0) Gecko/20100101 Firefox/98.0",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
            "Accept-Encoding": "gzip, deflate",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Sec-Fetch-User": "?1",
            "Cache-Control": "max-age=0",
            }
            req = requests.get(url, headers = HEADERS)
            soup = BeautifulSoup(req.content, 'html.parser')
            table = soup.find('table')

            head = ['symbol', 'Tipo', 'Data_COM', 'Pagamento', 'Valor']
                   
            rows = []
            for row in table.find_all("tr"):
                cells = []
                cells.append(symbol)
                for td in row.find_all("td"):
                    cells.append(td.text.strip())
                if cells:
                    rows.append(cells)
        
            df = pd.DataFrame(rows, columns=head)
            divs = df.query('Tipo=="Rendimento"')
            divs['Tipo'] = divs['Tipo'].str[:10]
            divs['Valor'] = divs['Valor'].str[:9]
            all_divs.append(divs)

        else:
            url = f'https://statusinvest.com.br/acoes/{symbol}'
            HEADERS = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:98.0) Gecko/20100101 Firefox/98.0",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
            "Accept-Encoding": "gzip, deflate",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Sec-Fetch-User": "?1",
            "Cache-Control": "max-age=0",
            }
            req = requests.get(url, headers = HEADERS)
            soup = BeautifulSoup(req.content, 'html.parser')
            table = soup.find('table')

            head = ['symbol', 'Tipo', 'Data_COM', 'Pagamento', 'Valor']
                   
            rows = []
            for row in table.find_all("tr"):
                cells = []
                cells.append(symbol)
                for td in row.find_all("td"):
                    cells.append(td.text.strip())
                if cells:
                    rows.append(cells)
        
            df2 = pd.DataFrame(rows, columns=head)
            divs = df2.query('Tipo=="Dividendo"')
            divs['Tipo'] = divs['Tipo'].str[:9]
            divs['Valor'] = divs['Valor'].str[:9]
            all_divs.append(divs)
            
        
    final_df = pd.concat(all_divs, ignore_index=True)
    
    full_df = final_df.replace(',', '.', regex = True)
        
    return full_df
    
def create_table_if_not_exists(connection, table_name):
    cursor = connection.cursor()

    # Verifica se a tabela já existe
    cursor.execute(f"""
    SELECT COUNT(*)
    FROM user_tables
    WHERE table_name = UPPER('{table_name}')
    """)

    exists = cursor.fetchone()[0]

    # Cria a tabela se ela não existir
    if exists == 0:
        cursor.execute(f"""
        CREATE TABLE {table_name} (
            symbol VARCHAR2(10),
            data DATE,
            abertura NUMBER,
            alta NUMBER,
            baixa NUMBER,
            fechamento NUMBER
        )
        """)
        print(f"Tabela '{table_name}' criada com sucesso.")
    else:
        print(f"Tabela '{table_name}' já existe.")

    cursor.close()
    connection.commit()

def create_table_if_not_exists_divid(connection, table_name):
    cursor = connection.cursor()

    # Verifica se a tabela já existe
    cursor.execute(f"""
    SELECT COUNT(*)
    FROM user_tables
    WHERE table_name = UPPER('{table_name}')
    """)

    exists = cursor.fetchone()[0]

    # Cria a tabela se ela não existir
    if exists == 0:
        cursor.execute(f"""
        CREATE TABLE {table_name} (
            symbol VARCHAR2(10),
            Data_COM DATE,
            Data_pay DATE,
            Valor NUMBER
        )
        """)
        print(f"Tabela '{table_name}' criada com sucesso.")
    else:
        print(f"Tabela '{table_name}' já existe.")

    cursor.close()
    connection.commit()

def symbol_sa():
    # Fazendo leitura de um arquivo .txt que contem dados dos simbolos das ações 
    arquivo = open('sensivel/simbolos.txt', 'r')
    syms = arquivo.read()
    arquivo.close()
    symbols = syms.split()

    sym_sa = []
    for symbol in symbols:
        sa = symbol + '.SA'
        sym_sa.append(sa)

    return sym_sa

