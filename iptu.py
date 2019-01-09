import pandas as pd
import csv
from simpledbf import Dbf5

class Iptu():
    
    def __init__(self, bairros = '', path = 'IPTU_2018.csv', encoding = 'utf-8', path_log = 'DEINFO_SEGTOS_212.dbf'):
        
        self.file = open(path, 'rt')
        self.cols = self.gerar_reader(cols = False).fieldnames
        if not bairros:
            self.bairros = self.list_bairros_log()
        else:
            self.bairros = bairros
        self.mem_bairros = self.criar_mem_bairros(self.parsear_dbf(path_log))
        self.linhas = []
        
    def parsear_dbf(self,path_dbf):
        '''Parseia o arquivo dbf e gera um DataFrame'''

        dbf = Dbf5(path_dbf, codec='utf-8')
        df = dbf.to_dataframe()
        
        return df
        
    
    def criar_mem_bairros(self, df):
        '''Criar os dicionarios para filtragem dos bairros'''

        df_d = df.copy()[['CEP_D', 'BAIRRO_D']].drop_duplicates()
        df_d = df_d[df_d['BAIRRO_D'].isin(self.bairros)]
        df_e = df.copy()[['CEP_E', 'BAIRRO_E']].drop_duplicates()
        df_e = df_e[df_e['BAIRRO_E'].isin(self.bairros)]
        
        dic_d = dict(zip(df_d['CEP_D'], df_d['BAIRRO_D']))
        dic_e = dict(zip(df_e['CEP_E'], df_e['BAIRRO_E']))
        
        return dic_d, dic_e        
    
    def list_bairros_log(self):
        
        l = list(self.logradouros['BAIRRO_E'])
        l.extend(list(self.logradouros['BAIRRO_D']))
        return set(l)
    
    def gerar_reader(self, cols = True):
        if not cols:
            return csv.DictReader(self.file, delimiter = ';')
        else:
            return csv.DictReader(self.file, delimiter = ';', fieldnames = self.cols)
        
    def pegar_bairro(self, linha):
        
        cep = linha['CEP DO IMOVEL'].replace('-', '')
        try:
            bairro = self.mem_bairros[0]
        except KeyError:
            try:
                bairro = self.mem_bairros[1]
            except KeyError:
                bairro = 'NAO ENCONTRADO'
        linha['BAIRRO'] = bairro
        
        
    def pegar_linha(self, reader):
        try:
            linha = reader.__next__()
            self.pegar_bairro(linha)
            return linha, 1
        except StopIteration:
            return {}, 0
    
    def filtrar_bairros(self, chunk = 100000):
        
        reader = self.gerar_reader()
        count = 0
        while True:
            for i in range(chunk):
                linha, flag = self.pegar_linha(reader)
                if flag:
                    self.linhas.append(linha)
                    
                else:
                    break
            else:
                count += chunk
                print('{} Imoveis parseados'.format(count))
        self.file.close()        
        print('Todos os imoveis foram parseados')