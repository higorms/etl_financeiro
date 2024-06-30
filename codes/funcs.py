import requests
import time

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
                    'date': date,
                    'open_price': data['1. open'],
                    'high_price': data['2. high'],
                    'low_price': data['3. low'],
                    'close_price': data['4. close'],
                })
        
        # Respeitar os limites de taxa da API
        time.sleep(12)  # Espera de 12 segundos para evitar limite de chamadas
    
    return all_data

def dividendos(api_key, symbols):
    divs = []

    for symbol in symbols:
        url = f'https://www.alphavantage.co/query?function=DIVIDENDS&{symbol}=IBM&apikey={api_key}'
        response = requests.get(url)
        stock_data = response.json()
    
        if 'Time Series (Daily)' in stock_data:
            time_series = stock_data['Time Series (Daily)']
            for date, data in time_series.items():
                divs.append({
                    'symbol': symbol,
                    'ex_dividend_date': data['ex_dividend_date'],
                    'declaration_date': data['declaration_date'],
                    'record_date': data['record_date'],
                    'payment_date': data['payment_date'],
                    'amount': data['amount'],
                })
        
        # Respeitar os limites de taxa da API
        time.sleep(12)  # Espera de 12 segundos para evitar limite de chamadas

        return divs
    
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
