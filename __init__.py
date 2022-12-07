import pandas as pd
from persist.db import persist_data

df = pd.read_csv('arquivo_exemplo.csv', sep=';')

persist_data(df)