from funcs import stock_data
import requests
import pandas as pd

arquivo = open('sensivel/key.txt', 'r')
api_key = arquivo.read()
arquivo.close()

arquivo = open('sensivel/simbolos.txt', 'r')
syms = arquivo.read()
arquivo.close()
symbols = syms.split()

print(symbols[1])

#data = stock_data(api_key, symbols)

#df = pd.DataFrame(data)

#df.to_csv('output.csv', index=False)
