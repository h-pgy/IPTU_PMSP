import pandas as pd
import csv
from simpledbf import Dbf5
import os

class Iptu():
    
    def __init__(self,df_log, file, bairros = '', encoding = 'utf-8'):
        
        self.file = open(file, 'rt')
        self.cols = self.gerar_reader(cols = False).fieldnames
        if not bairros:
            self.bairros = list_bairros_log()
        else:
            self.bairros = bairros
        self.mem_bairros = self.criar_mem_bairros(df_log)
        self.linhas = []
            
    
    def criar_mem_bairros(self, df):
        '''Criar os dicionarios para filtragem dos bairros'''

        df_d = df.copy()[['CEP_D', 'BAIRRO_D']].drop_duplicates()
        df_d = df_d[df_d['BAIRRO_D'].isin(self.bairros)]
        df_e = df.copy()[['CEP_E', 'BAIRRO_E']].drop_duplicates()
        df_e = df_e[df_e['BAIRRO_E'].isin(self.bairros)]
        
        dic_d = dict(zip(df_d['CEP_D'], df_d['BAIRRO_D']))
        dic_e = dict(zip(df_e['CEP_E'], df_e['BAIRRO_E']))
        
        return dic_d, dic_e        
    
    
    def gerar_reader(self, cols = True):
        if not cols:
            return csv.DictReader(self.file, delimiter = ';')
        else:
            return csv.DictReader(self.file, delimiter = ';', fieldnames = self.cols)
        
    def pegar_bairro(self, linha):
        
        cep = linha['CEP DO IMOVEL'].replace('-', '')
        try:
            bairro = self.mem_bairros[0][cep]
        except KeyError:
            try:
                bairro = self.mem_bairros[1][cep]
            except KeyError:
                return False
        linha['BAIRRO'] = bairro
        return linha
        
        
    def pegar_linha(self, reader):
        try:
            linha = reader.__next__()
            linha = self.pegar_bairro(linha)
            return linha, 1
        except StopIteration:
            return {}, 0
    
    def filtrar_bairros(self, chunk = 1000000):
        
        reader = self.gerar_reader()
        count = 0
        while True:
            for i in range(chunk):
                linha, flag = self.pegar_linha(reader)
                if flag:
                    if linha:
                        self.linhas.append(linha)
                else:
                    break
            else:
                count += chunk
            if not flag:
                break        
    
    def gerar_df(self):
        
        self.df = pd.DataFrame.from_dict(self.linhas)
        del self.linhas
        
    def main_iptu(self):
        
        self.filtrar_bairros()
        self.gerar_df()
        self.file.close()
        
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
    
    
        
    
        
        
