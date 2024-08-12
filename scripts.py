from django.shortcuts import render
import json
import requests
from abc import ABC, abstractmethod
import pandas as pd
from datetime import datetime

import os
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "web.settings")
import django
from django.db import transaction
django.setup()

from api.models import (
    Producao,Processamento,Comercializacao,Importacao,Exportacao,Atualizacao)



class Pipeline(ABC):
    """Essa é a 'interface' para os Pipelines. Todas as classes que a implementão terão
      o método run"""
    @abstractmethod
    def run(self,sources:dict,atualizacao:object):
        """O método run será implementado em todas as subclasses. Ele servirá para 
        executar todo o pipeline de dados proposto.
        1) Deverá ser passado a ele um dict com os sources
        2) Deverá ser passado um callback em que 

        """
        ...

class EmbrapaPipeline(Pipeline,ABC):
    """Essa é a 'interface' para os Pipelines da Embrapa. 
    Todos deverão implemtar os métodos abaixo"""

    @abstractmethod
    def handle_producao(self,csv_file_path:str,atualizacao:int):
        ...

    @abstractmethod
    def handle_processamento_viniferas(self,csv_file_path:str,atualizacao:int):
        ...
    
    @abstractmethod
    def handle_processamento_americanas(self,csv_file_path:str,atualizacao:int):
        ...
    
    @abstractmethod
    def handle_processamento_mesa(self,csv_file_path:str,atualizacao:int):
        ...

    @abstractmethod
    def handle_processamento_sem_classificacao(self,csv_file_path:str,atualizacao:int):
        ...

    @abstractmethod
    def handle_comercializacao(self,csv_file_path:str,atualizacao:int):
        ...

    @abstractmethod
    def handle_importacao_vinhos_de_mesa(self,csv_file_path:str,atualizacao:int):
        ...

    @abstractmethod
    def handle_importacao_espumantes(self,csv_file_path:str,atualizacao:int):
        ...

    @abstractmethod
    def handle_importacao_uvas_frescas(self,csv_file_path:str,atualizacao:int):
        ...

    @abstractmethod
    def handle_importacao_uvas_passas(self,csv_file_path:str,atualizacao:int):
        ...
    
    @abstractmethod
    def handle_importacao_suco_de_uva(self,csv_file_path:str,atualizacao:int):
        ...

    @abstractmethod
    def handle_exportacao_vinhos_de_mesa(self,csv_file_path:str,atualizacao:int):
        ...
    
    @abstractmethod
    def handle_exportacao_espumantes(self,csv_file_path:str,atualizacao:int):
        ...

    @abstractmethod
    def handle_exportacao_uvas_frescas(self,csv_file_path:str,atualizacao:int):
        ...

    @abstractmethod
    def handle_exportacao_suco_de_uva(self,csv_file_path:str,atualizacao:int):
        ...

class DefaultEmbrapaPipeline(EmbrapaPipeline):
    """Esta classe é a padrão no Pipeline que implementa EmbrapaPipeline.
    Ela serve para garantir que todo o fluxo, de download até o
    armazenamento no banco de dados seja realizado com sucesso."""


    def run(self, sources: dict,atualizacao:object):
        """O método run está implementado tendo como premissas os seguintes fatores:
        1) Ele receberá um dict que representa o json sources conforme documentado em Readme.md
        2) Todos os handlers indicados começam com handler.

        Com essas garantias no arquivo de entrada, teremos uma implementação mais limpa do método run;
        """
        #Executa o download atualizado de todos os arquivos conforme as especificações de URL que constam no sources.json
        self.downloader(sources)
        
        #busca por todos os métodos handler que estão em sources.json.
        handlers = [self.__getattribute__(f"handle_{x}") for x in sources]
        
        with transaction.atomic():
            try:
                for handler in handlers:
                #busca onde está salvo o csv
                    dst_file = sources[handler.__name__.replace("handle_","")]['dst_file']
                    #executa o handler passando o diretório do CSV.
                    print(f'[handle_producao] Init. {handler.__name__.replace("handle_","")}')
                    handler(dst_file,atualizacao)
                    print(f'[handle_producao] Fim. {handler.__name__.replace("handle_","")}')
                atualizacao.status = "SUCESSO"
                atualizacao.save()
            except Exception as ex:
                print(f"Erro inesperado: {ex}")
                atualizacao.status = "ERRO"
                atualizacao.save()
                raise Exception from ex
        
        
    def handle_producao(self, csv_file_path:str,atualizacao:int):
        
        df = pd.read_csv(csv_file_path, delimiter = ";")
        df = df.melt(id_vars=["id", "control","produto"], 
                var_name="ano", 
                value_name="quantidade_litros")
        df['ano'] = df['ano'].astype(int)
        df['quantidade_litros'] = df['quantidade_litros'].astype(float)
        df.drop(columns=['control'])
        for index, row in df.iterrows():
            
            Producao.objects.create(
                atualizacao = atualizacao,
                produto = row['produto'],
                ano = row['ano'],
                quantidade_litros = row['quantidade_litros']
            )


    def handle_comercializacao(self, csv_file_path:str,atualizacao:int):
        
        df = pd.read_csv(csv_file_path, delimiter = ";")
        df = df.melt(id_vars=["id", "control","Produto"], 
                var_name="ano", 
                value_name="quantidade_litros")
        df.columns = ['id','control','produto','ano','quantidade_litros']
        df['ano'] = df['ano'].astype(int)
        df['quantidade_litros'] = df['quantidade_litros'].astype(float)
        df.drop(columns=['control'])
        for index, row in df.iterrows():
           
            Comercializacao.objects.create(
                atualizacao = atualizacao,
                produto = row['produto'],
                ano = row['ano'],
                quantidade_litros = row['quantidade_litros']
            )

    def handle_exportacao_espumantes(self, csv_file_path:str,atualizacao:int):
        df = pd.read_csv(csv_file_path, delimiter = ";")
        
        df_quantidade = df.melt(
                id_vars=["Id", "País"], 
                value_vars=[ x for x in df.columns[2:] if not ".1" in x ],
                var_name="ano", 
                value_name="quantidade")
   
        

        df_dolares = df.melt(
                id_vars=["Id", "País"], 
                value_vars=[x for x in df.columns[2:] if  ".1" in x],
                var_name="ano", 
                value_name="dolares")
        
    
        
        df_merged = pd.merge(df_quantidade, df_dolares, on = ["Id", "País","ano"],how='left')
        df_merged.rename(
            columns = 
            {
                "Id":"id",
                "País":"pais"
            },inplace=True
        )
        df_merged = df_merged.fillna(
            {
                "quantidade":0,
                "dolares":0
            }
        )
        for index, row in df_merged.iterrows():
            
            Exportacao.objects.create(
                    
                 atualizacao = atualizacao,
                 classificacao = "espumantes",
                 pais = row['pais'],
                 ano = int(row['ano'].replace(".1","") if '.1' in row['ano'] else row["ano"]),
                 valor_dolares =  float(row['dolares']),
                 quantidade = row['quantidade']
             )
            
            
    def handle_exportacao_suco_de_uva(self, csv_file_path:str,atualizacao:int):
        df = pd.read_csv(csv_file_path, delimiter = ";")
        
        df_quantidade = df.melt(
                id_vars=["Id", "País"], 
                value_vars=[ x for x in df.columns[2:] if not ".1" in x ],
                var_name="ano", 
                value_name="quantidade")
   
        

        df_dolares = df.melt(
                id_vars=["Id", "País"], 
                value_vars=[x for x in df.columns[2:] if  ".1" in x],
                var_name="ano", 
                value_name="dolares")
        
  
        
        df_merged = pd.merge(df_quantidade, df_dolares, on = ["Id", "País","ano"],how='left')
        df_merged.rename(
            columns = 
            {
                "Id":"id",
                "País":"pais"
            },inplace=True
        )
        df_merged = df_merged.fillna(
            {
                "quantidade":0,
                "dolares":0
            }
        )
        for index, row in df_merged.iterrows():
            
            Exportacao.objects.create(
                    
                 atualizacao = atualizacao,
                 classificacao = "suco_de_uva",
                 pais = row['pais'],
                 ano = int(row['ano'].replace(".1","") if '.1' in row['ano'] else row["ano"]),
                 valor_dolares =  float(row['dolares']),
                 quantidade = row['quantidade']
             )
            
    
    def handle_exportacao_uvas_frescas(self, csv_file_path:str,atualizacao:int):
        df = pd.read_csv(csv_file_path, delimiter = ";")
        
        df_quantidade = df.melt(
                id_vars=["Id", "País"], 
                value_vars=[ x for x in df.columns[2:] if not ".1" in x ],
                var_name="ano", 
                value_name="quantidade")
   
        

        df_dolares = df.melt(
                id_vars=["Id", "País"], 
                value_vars=[x for x in df.columns[2:] if  ".1" in x],
                var_name="ano", 
                value_name="dolares")
        
  
        
        df_merged = pd.merge(df_quantidade, df_dolares, on = ["Id", "País","ano"],how='left')
        df_merged.rename(
            columns = 
            {
                "Id":"id",
                "País":"pais"
            },inplace=True
        )
        df_merged = df_merged.fillna(
            {
                "quantidade":0,
                "dolares":0
            }
        )
        for index, row in df_merged.iterrows():
            
            Exportacao.objects.create(
                    
                 atualizacao = atualizacao,
                 classificacao = "uvas_frescas",
                 pais = row['pais'],
                 ano = int(row['ano'].replace(".1","") if '.1' in row['ano'] else row["ano"]),
                 valor_dolares =  float(row['dolares']),
                 quantidade = row['quantidade']
             )
            
    
    def handle_exportacao_vinhos_de_mesa(self, csv_file_path:str,atualizacao:int):
        df = pd.read_csv(csv_file_path, delimiter = ";")
        
        df_quantidade = df.melt(
                id_vars=["Id", "País"], 
                value_vars=[ x for x in df.columns[2:] if not ".1" in x ],
                var_name="ano", 
                value_name="quantidade")

        

        df_dolares = df.melt(
                id_vars=["Id", "País"], 
                value_vars=[x for x in df.columns[2:] if  ".1" in x],
                var_name="ano", 
                value_name="dolares")
        

        
        df_merged = pd.merge(df_quantidade, df_dolares, on = ["Id", "País","ano"],how='left')
        df_merged.rename(
            columns = 
            {
                "Id":"id",
                "País":"pais"
            },inplace=True
        )
        df_merged = df_merged.fillna(
            {
                "quantidade":0,
                "dolares":0
            }
        )
        for index, row in df_merged.iterrows():
            
            Exportacao.objects.create(
                    
                    atualizacao = atualizacao,
                    classificacao = "vinhos_de_mesa",
                    pais = row['pais'],
                    ano = int(row['ano'].replace(".1","") if '.1' in row['ano'] else row["ano"]),
                    valor_dolares =  float(row['dolares']),
                    quantidade = row['quantidade']
                )
            
    
    def handle_importacao_espumantes(self, csv_file_path:str,atualizacao:int):
        df = pd.read_csv(csv_file_path, delimiter = ";")
        
        df_quantidade = df.melt(
                id_vars=["Id", "País"], 
                value_vars=[ x for x in df.columns[2:] if not ".1" in x ],
                var_name="ano", 
                value_name="quantidade")

        

        df_dolares = df.melt(
                id_vars=["Id", "País"], 
                value_vars=[x for x in df.columns[2:] if  ".1" in x],
                var_name="ano", 
                value_name="dolares")
        

        
        df_merged = pd.merge(df_quantidade, df_dolares, on = ["Id", "País","ano"],how='left')
        df_merged.rename(
            columns = 
            {
                "Id":"id",
                "País":"pais"
            },inplace=True
        )
        df_merged = df_merged.fillna(
            {
                "quantidade":0,
                "dolares":0
            }
        )
        for index, row in df_merged.iterrows():
            
            Importacao.objects.create(
                    
                    atualizacao = atualizacao,
                    classificacao = "espumantes",
                    pais = row['pais'],
                    ano = int(row['ano'].replace(".1","") if '.1' in row['ano'] else row["ano"]),
                    valor_dolares =  float(row['dolares']),
                    quantidade = row['quantidade']
                )
    
    def handle_importacao_suco_de_uva(self, csv_file_path:str,atualizacao:int):
        df = pd.read_csv(csv_file_path, delimiter = ";")
        
        df_quantidade = df.melt(
                id_vars=["Id", "País"], 
                value_vars=[ x for x in df.columns[2:] if not ".1" in x ],
                var_name="ano", 
                value_name="quantidade")

        

        df_dolares = df.melt(
                id_vars=["Id", "País"], 
                value_vars=[x for x in df.columns[2:] if  ".1" in x],
                var_name="ano", 
                value_name="dolares")
        

        
        df_merged = pd.merge(df_quantidade, df_dolares, on = ["Id", "País","ano"],how='left')
        df_merged.rename(
            columns = 
            {
                "Id":"id",
                "País":"pais"
            },inplace=True
        )
        df_merged = df_merged.fillna(
            {
                "quantidade":0,
                "dolares":0
            }
        )
        for index, row in df_merged.iterrows():
            
            Importacao.objects.create(
                    
                    atualizacao = atualizacao,
                    classificacao = "suco_de_uva",
                    pais = row['pais'],
                    ano = int(row['ano'].replace(".1","") if '.1' in row['ano'] else row["ano"]),
                    valor_dolares =  float(row['dolares']),
                    quantidade = row['quantidade']
                )
    
    def handle_importacao_uvas_passas(self, csv_file_path:str,atualizacao:int):
        df = pd.read_csv(csv_file_path, delimiter = ";")
        
        df_quantidade = df.melt(
                id_vars=["Id", "País"], 
                value_vars=[ x for x in df.columns[2:] if not ".1" in x ],
                var_name="ano", 
                value_name="quantidade")

        

        df_dolares = df.melt(
                id_vars=["Id", "País"], 
                value_vars=[x for x in df.columns[2:] if  ".1" in x],
                var_name="ano", 
                value_name="dolares")
        

        
        df_merged = pd.merge(df_quantidade, df_dolares, on = ["Id", "País","ano"],how='left')
        df_merged.rename(
            columns = 
            {
                "Id":"id",
                "País":"pais"
            },inplace=True
        )
        df_merged = df_merged.fillna(
            {
                "quantidade":0,
                "dolares":0
            }
        )
        for index, row in df_merged.iterrows():
            
            Importacao.objects.create(
                    
                    atualizacao = atualizacao,
                    classificacao = "uvas_passas",
                    pais = row['pais'],
                    ano = int(row['ano'].replace(".1","") if '.1' in row['ano'] else row["ano"]),
                    valor_dolares =  float(row['dolares']),
                    quantidade = row['quantidade']
                )
    
    def handle_importacao_uvas_frescas(self, csv_file_path:str,atualizacao:int):
        df = pd.read_csv(csv_file_path, delimiter = ";")
        
        df_quantidade = df.melt(
                id_vars=["Id", "País"], 
                value_vars=[ x for x in df.columns[2:] if not ".1" in x ],
                var_name="ano", 
                value_name="quantidade")

        

        df_dolares = df.melt(
                id_vars=["Id", "País"], 
                value_vars=[x for x in df.columns[2:] if  ".1" in x],
                var_name="ano", 
                value_name="dolares")
        

        
        df_merged = pd.merge(df_quantidade, df_dolares, on = ["Id", "País","ano"],how='left')
        df_merged.rename(
            columns = 
            {
                "Id":"id",
                "País":"pais"
            },inplace=True
        )
        df_merged = df_merged.fillna(
            {
                "quantidade":0,
                "dolares":0
            }
        )
        for index, row in df_merged.iterrows():
            
            Importacao.objects.create(
                    
                    atualizacao = atualizacao,
                    classificacao = "uvas_frescas",
                    pais = row['pais'],
                    ano = int(row['ano'].replace(".1","") if '.1' in row['ano'] else row["ano"]),
                    valor_dolares =  float(row['dolares']),
                    quantidade = row['quantidade']
                )
    
    def handle_importacao_vinhos_de_mesa(self, csv_file_path:str,atualizacao:int):
        df = pd.read_csv(csv_file_path, delimiter = ";")
        
        df_quantidade = df.melt(
                id_vars=["Id", "País"], 
                value_vars=[ x for x in df.columns[2:] if not ".1" in x ],
                var_name="ano", 
                value_name="quantidade")

        

        df_dolares = df.melt(
                id_vars=["Id", "País"], 
                value_vars=[x for x in df.columns[2:] if  ".1" in x],
                var_name="ano", 
                value_name="dolares")
        
        
        
        df_merged = pd.merge(df_quantidade, df_dolares, on = ["Id", "País","ano"],how='left')
        df_merged.rename(
            columns = 
            {
                "Id":"id",
                "País":"pais"
            },inplace=True
        )
        df_merged = df_merged.fillna(
            {
                "quantidade":0,
                "dolares":0
            }
        )
        for index, row in df_merged.iterrows():
            Importacao.objects.create(
                    
                    atualizacao = atualizacao,
                    classificacao = "vinhos_de_mesa",
                    pais = row['pais'],
                    ano = int(row['ano'].replace(".1","") if '.1' in row['ano'] else row["ano"]),
                    valor_dolares =  float(row['dolares']),
                    quantidade = row['quantidade']
                )

    
    def handle_processamento_americanas(self, csv_file_path:str,atualizacao:int):
        df = pd.read_csv(csv_file_path, delimiter = "\t")
        df = df.melt(id_vars=["id", "control","cultivar"], 
                var_name="ano", 
                value_name="quantidade_kg")
        df['quantidade_kg'] = df['quantidade_kg'].astype(str)
        df['quantidade_kg'] = df['quantidade_kg'].apply(lambda x : 0 if not x.isdigit() else x)
        
        df.fillna({
            "quantidade_kg":0
        })
        
        
        df['ano'] = df['ano'].astype(int)
        df['quantidade_kg'] = df['quantidade_kg'].astype(float)
        df.drop(columns=['control'])
        
        
        for index, row in df.iterrows():
            
            Processamento.objects.create(
                atualizacao = atualizacao,
                classificacao = "americanas",
                cultivar = row['cultivar'],
                ano = row['ano'],
                quantidade_kg = row['quantidade_kg']
            )
    
    def handle_processamento_mesa(self, csv_file_path:str,atualizacao:int):
        df = pd.read_csv(csv_file_path, delimiter = "\t")
        df = df.melt(id_vars=["id", "control","cultivar"], 
                var_name="ano", 
                value_name="quantidade_kg")
        df['quantidade_kg'] = df['quantidade_kg'].astype(str)
        df['quantidade_kg'] = df['quantidade_kg'].apply(lambda x : 0 if not x.isdigit() else x)
        
        df.fillna({
            "quantidade_kg":0
        })
        
        
        df['ano'] = df['ano'].astype(int)
        df['quantidade_kg'] = df['quantidade_kg'].astype(float)
        df.drop(columns=['control'])
        
        
        for index, row in df.iterrows():
            
            Processamento.objects.create(
                atualizacao = atualizacao,
                classificacao = "mesa",
                cultivar = row['cultivar'],
                ano = row['ano'],
                quantidade_kg = row['quantidade_kg']
            )
    
    def handle_processamento_sem_classificacao(self, csv_file_path:str,atualizacao:int):
        df = pd.read_csv(csv_file_path, delimiter = "\t")
        df = df.melt(id_vars=["id", "control","cultivar"], 
                var_name="ano", 
                value_name="quantidade_kg")
        df['quantidade_kg'] = df['quantidade_kg'].astype(str)
        df['quantidade_kg'] = df['quantidade_kg'].apply(lambda x : 0 if not x.isdigit() else x)
        
        df.fillna({
            "quantidade_kg":0
        })
        
        
        df['ano'] = df['ano'].astype(int)
        df['quantidade_kg'] = df['quantidade_kg'].astype(float)
        df.drop(columns=['control'])
        
        
        for index, row in df.iterrows():
            
            Processamento.objects.create(
                atualizacao = atualizacao,
                classificacao = "sem_classificacao",
                cultivar = row['cultivar'],
                ano = row['ano'],
                quantidade_kg = row['quantidade_kg']
            )
    
    def handle_processamento_viniferas(self, csv_file_path:str,atualizacao:int):
        df = pd.read_csv(csv_file_path, delimiter = ";")
        df = df.melt(id_vars=["id", "control","cultivar"], 
                var_name="ano", 
                value_name="quantidade_kg")
        df['quantidade_kg'] = df['quantidade_kg'].astype(str)
        df['quantidade_kg'] = df['quantidade_kg'].apply(lambda x : 0 if not x.isdigit() else x)
        
        df.fillna({
            "quantidade_kg":0
        })
        
        
        df['ano'] = df['ano'].astype(int)
        df['quantidade_kg'] = df['quantidade_kg'].astype(float)
        df.drop(columns=['control'])
        
        
        for index, row in df.iterrows():
            
            Processamento.objects.create(
                atualizacao = atualizacao,
                classificacao = "viniferas",
                cultivar = row['cultivar'],
                ano = row['ano'],
                quantidade_kg = row['quantidade_kg']
            )

    def downloader(self,sources:dict):
        """O objetivo deste método é realizar o download das fontes de dados do Emprapa
        para o cache."""
        
        if not os.path.exists("./cache"):
            os.makedirs("./cache")
        
        #percorre todas as chaves do json cuja a chave é sources
        for source in sources.keys():
            try:
                #O url onde está localizado o arquivo csv
                url = sources[source]['url']
                #prod_file define onde será salvo o arquivo csv
                prod_file = sources[source]['dst_file']

                print(f"File {prod_file} download started")
                #Utiliza a lib requests para buscar o conteudo csv
                content = requests.get(url).content
                print(f"File {prod_file} download finished")

                #salva o arquivo no diretorio informado por prod_file
                with open(prod_file,"wb") as csv_file:
                    csv_file.write(content)
                    print(f"File {prod_file} successfully saved")

            #Caso ocorra algum erro ao salvar o arquivo, este é informado.
            except Exception as ex:
                print(f"Unexpected Error: {ex}")

def run(atualizacao):
    with open("sources.json") as f:
        sources = json.load(f)['sources']
        DefaultEmbrapaPipeline().run(sources,atualizacao)


