import pandas as pd
import csv
from simpledbf import Dbf5
import os
from iptu import Iptu


def gerar_csv_bairros(bairros, df_log, ano, file_ano):
    
    dir_ = os.path.join(os.path.abspath(os.getcwd()), ano)
    iptu = Iptu(df_log, file_ano, bairros)
    iptu.main_iptu()
    dfs = {bairro : iptu.df[iptu.df['BAIRRO'] == bairro].copy().reset_index(drop = True)
          for bairro in iptu.bairros}
    for bairro, df in dfs.items():
        df.to_csv("{ano}\\{bairro}.csv".format(ano = dir_, bairro = bairro), sep = ';', encoding = 'latin1')
        print('Pipeline para o bairro {} finalizado'.format(bairro))
    iptu.file.close()
    
def parsear_dbf(path_dbf):
    '''Parseia o arquivo dbf e gera um DataFrame'''

    dbf = Dbf5(path_dbf, codec='utf-8')
    df = dbf.to_dataframe()

    return df

def formatar_bairros(bairros):
    
    bairros_format = []
    chars_ruims = ['?', '*', '$', '"', '.']
    for char in chars_ruims:
        for bairro in bairros:
            bairro = str(bairro)
            bairro = bairro.replace(char, '')
            bairros_format.append(bairro)
    return bairros_format
    
def list_bairros_log(df_log):

    l = list(df_log['BAIRRO_E'])
    l.extend(list(df_log['BAIRRO_D']))
    bairros = set(l)
    bairros = formatar_bairros(bairros)
    
    return sorted(bairros)
                
def dividir_bairros(bairros, num_batches):
    
    result = []
    start = 0
    if num_batches > 1:
        for i, bairro in enumerate(bairros):
            if (i>0) and (i%(len(bairros)//num_batches)==0):
                stop = i
                result.append(bairros[start:stop])
                start = i
        else:
            result.append(bairros[start:]) # ultimos registros
    return result

def criar_diretorio(file_iptu):
    
    ano = ''.join([d for d in file_iptu if d.isdigit()])
    os.mkdir(os.path.join(os.path.abspath(os.getcwd()), ano))
    return ano

def inicializacao(file_iptu, num_batches):
    
    df_log = parsear_dbf(path_dbf = 'DEINFO_SEGTOS_212.dbf')
    bairros = list_bairros_log(df_log)
    bairros = dividir_bairros(bairros, num_batches)
    ano = criar_diretorio(file_iptu)
    
    return df_log, bairros, ano
    
def main(file_iptu, num_batches):
    
    df_log, bairros, ano = inicializacao(file_iptu, num_batches)
    
    for i, batch_bairros in enumerate(bairros):
        print('Batch Num : {} de {}'.format(i, num_batches))
        print('Inicializando pipeline para os bairros :')
        for bairro in batch_bairros: print(bairro)
        gerar_csv_bairros(batch_bairros, df_log, ano, file_iptu)

if __name__ == "__main__":
    
    file = 'IPTUS/EG2018.csv'
    main(file, num_batches = 10)
    
    
        
    
        
        
