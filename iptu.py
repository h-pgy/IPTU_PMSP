import pandas as pd
import csv
from simpledbf import Dbf5

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
