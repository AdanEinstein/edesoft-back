import mysql.connector
import datetime
import re

mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  password="",
  database="edesoft",
)

def persist_data(data_frame):
    cursor = mydb.cursor()
    sql = f'''INSERT INTO CESSAO_FUNDO 
(ORIGINADOR,
DOC_ORIGINADOR,
CEDENTE,
DOC_CEDENTE,
CCB,
ID_EXTERNO,
CLIENTE,
CPF_CNPJ,
ENDERECO,
CEP,
CIDADE,
UF,
VALOR_DO_EMPRESTIMO,
VALOR_PARCELA,
TOTAL_PARCELAS,
PARCELA,
DATA_DE_EMISSAO,
DATA_DE_VENCIMENTO,
PRECO_DE_AQUISICAO) VALUES 
    (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    '''
    for data in data_frame.iterrows():
        val = (data['Originador'],
               data['Doc Originador'],
               data['Cedente'],
               data['Doc Cedente'],
               data['CCB'],
               data['Id'],
               data['Cliente'],
               re.sub("\D", '', data['CPF/CNPJ']),
               data['Endereço'],
               data['CEP'],
               data['Cidade'],
               data['UF'],
               data['Valor do Empréstimo'],
               data['Parcela R$'],
               data['Total Parcelas'],
               data['Parcela'],
               datetime.datetime.strptime(data['Data de Emissão'], "%d/%m/%Y").strftime("%Y-%m-%d"),
               datetime.datetime.strptime(data['Data de Vencimento'], "%d/%m/%Y").strftime("%Y-%m-%d"),
               data['Preço de Aquisição'])
        cursor.execute(sql, val)

    mydb.commit()