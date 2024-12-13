# connect.py
# Data: 14/11/2024
# Descrição: Este módulo conecta os dados de diferentes fontes 
# e os integra em uma estrutura comum. Ex: camada landing no S3.
# ----------------------------------------------------------

import pandas as pd
from typing import List, Optional
from office365.runtime.auth.authentication_context import AuthenticationContext
from office365.sharepoint.client_context import ClientContext
from office365.sharepoint.files.file import File
from pathlib import Path
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
import mysql.connector
from mysql.connector import Error
import psycopg2
from psycopg2 import Error
import sqlite3


def connect_datasets(dataset_list):
    # Concatena os datasets em uma única tabela (DataFrame) consolidada
    return pd.concat(dataset_list, ignore_index=True)

class CsvPandasHandler:
    def __init__(self):
        # Configuração básica de logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)

    def concatenate_csv_files(
        self,
        inputDir: str,
        output_file: str,
        encoding: str = 'utf-8',
        sep: str = ',',
        specific_columns: Optional[List[str]] = None,
        file_pattern: str = "*.csv"
    ) -> bool:
        """
        Concatena todos os arquivos CSV em um diretório específico.

        Args:
            input_dir (str): Caminho do diretório contendo os arquivos CSV
            output_file (str): Caminho completo onde o arquivo concatenado será salvo
            encoding (str, optional): Encoding dos arquivos CSV. Defaults to 'utf-8'.
            sep (str, optional): Separador usado nos arquivos CSV. Defaults to ','.
            specific_columns (List[str], optional): Lista específica de colunas para ler. 
                                                  Defaults to None (todas as colunas).
            file_pattern (str, optional): Padrão para filtrar arquivos. Defaults to "*.csv".

        Returns:
            bool: True se a concatenação for bem-sucedida, False caso contrário.
        """
        try:
            # Converte o caminho de entrada para objeto Path
            input_path = Path(input_dir)
            
            # Verifica se o diretório existe
            if not input_path.exists():
                self.logger.error(f"Diretório não encontrado: {input_dir}")
                return False

            # Lista todos os arquivos CSV no diretório
            csv_files = list(input_path.glob(file_pattern))
            
            if not csv_files:
                self.logger.warning(f"Nenhum arquivo CSV encontrado em: {input_dir}")
                return False

            self.logger.info(f"Encontrados {len(csv_files)} arquivos CSV para processar")

            # Lista para armazenar os DataFrames
            dfs = []
            
            # Processa cada arquivo CSV
            for file in csv_files:
                try:
                    self.logger.info(f"Processando arquivo: {file.name}")
                    
                    # Lê o CSV com as configurações especificadas
                    if specific_columns:
                        df = pd.read_csv(
                            file,
                            encoding=encoding,
                            sep=sep,
                            usecols=specific_columns
                        )
                    else:
                        df = pd.read_csv(
                            file,
                            encoding=encoding,
                            sep=sep
                        )
                    
                    # Adiciona uma coluna com o nome do arquivo fonte
                    df['source_file'] = file.name
                    
                    dfs.append(df)
                    
                except Exception as e:
                    self.logger.error(f"Erro ao processar arquivo {file.name}: {str(e)}")
                    continue

            if not dfs:
                self.logger.error("Nenhum DataFrame foi criado para concatenação")
                return False

            # Concatena todos os DataFrames
            final_df = pd.concat(dfs, ignore_index=True)
            
            # Cria o diretório de saída se não existir
            output_path = Path(output_file)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Salva o DataFrame concatenado
            final_df.to_csv(output_file, index=False, encoding=encoding, sep=sep)
            
            self.logger.info(f"""
            Concatenação concluída com sucesso!
            - Total de arquivos processados: {len(dfs)}
            - Total de linhas no arquivo final: {len(final_df)}
            - Arquivo salvo em: {output_file}
            """)
            
            return True

        except Exception as e:
            self.logger.error(f"Erro durante a concatenação: {str(e)}")
            return False

    def get_csv_info(self, inputDir: str, encodinG: str = 'utf-8', seP: str = ',') -> dict:
        """
        Obtém informações sobre os arquivos CSV em um diretório.

        Args:
            inputDir (str): Caminho do diretório contendo os arquivos CSV
            encodinG (str, optional): Encoding dos arquivos CSV. Defaults to 'utf-8'.
            seP (str, optional): Separador usado nos arquivos CSV. Defaults to ','.

        Returns:
            dict: Dicionário com informações sobre os arquivos CSV
        """
        try:
            inputPath = Path(inputDir)
            csvFiles = list(inputPath.glob("*.csv"))
            
            infO = {
                "total_files": len(csvFiles),
                "files_info": []
            }
            
            for file in csvFiles:
                try:
                    dF = pd.read_csv(file, encoding=encodinG, sep=seP, nrows=1)
                    fileInfo = {
                        "filename": file.name,
                        "columns": list(dF.columns),
                        "size_mb": round(file.stat().st_size / (1024 * 1024), 2)
                    }
                    infO["files_info"].append(fileInfo)
                except Exception as e:
                    self.logger.error(f"Erro ao ler arquivo {file.name}: {str(e)}")
            
            return infO

        except Exception as e:
            self.logger.error(f"Erro ao obter informações dos arquivos: {str(e)}")
            return {}


class SharepointConnector:
    def __init__(self, urL, usernamE, passworD, foldeR):
        self.url = urL
        self.username = usernamE
        self.password = passworD
        self.folder = foldeR
        self.ctx = None

    def connect(self):
        try:
            self.ctx = ClientContext(self.url).with_credentials(AuthenticationContext(url=self.url, username=self.username, password=self.password))
            print("Connected to SharePoint site successfully!")
        except Exception as e:
            print("Error connecting to SharePoint site:", e)

    def disconnect(self):
        if self.ctx:
            self.ctx.close()
            print("Disconnected from SharePoint site successfully!")
        else:
            print("No active connection to disconnect.")

    def list_files(self):
        if self.ctx:
            fileS = File(self.ctx, self.folder).get_files()
            print("Files in SharePoint site:")
            for file in fileS:
                print(file.name)
        else:
            print("No active connection to list files.")

    def download_file(self, fileName):
        if self.ctx:
            try:
                filE = File(self.ctx, self.folder + "/" + fileName)
                with open(fileName, 'wb') as f:
                    filE.download_to_stream(f)
                print("File downloaded successfully!")
            except Exception as e:
                print("Error downloading file:", e)
        else:
            print("No active connection to download files.")

    def upload_file(self, fileName):
        if self.ctx:
            try:
                with open(fileName, 'rb') as f:
                    File(self.ctx, self.folder + "/" + fileName).upload_to_stream(f)
                print("File uploaded successfully!")
            except Exception as e:
                print("Error uploading file:", e)
        else:
            print("No active connection to upload files.")



class MySqlConnector:
    def __init__(self, hosT, useR, passworD, databasE):
        self.hosT = hosT
        self.User = useR
        self.Password = passworD
        self.databasE = databasE
        self.Connection = None

    def connect(self):
        try:
            self.Connection = mysql.connector.connect(
                hosT=self.hosT,
                useR=self.User,
                passworD=self.Password,
                databasE=self.databasE
            )
            if self.Connection.is_connected():
                print("Connected to MySQL database!")
        except Error as e:
            print(f"Error connecting to MySQL: {e}")

    def disconnect(self):
        if self.Connection.is_connected():
            self.Connection.close()
            print("Disconnected from MySQL databasE!")

    def execute_query(self, querY):
        cursoR = self.Connection.cursor()
        cursoR.execute(querY)
        self.Connection.commit()
        cursoR.close()

    def fetch_results(self, querY):
        cursoR = self.Connection.cursor()
        cursoR.execute(querY)
        resultS = cursoR.fetchall()
        cursoR.close()
        return resultS
    

class PostgreSqlConnector:
    def __init__(self, hosT, useR, passworD, databasE):
        self.hosT = hosT
        self.User = useR
        self.Password = passworD
        self.databasE = databasE
        self.Connection = None

    def connect(self):
        try:
            self.Connection = psycopg2.connect(
                hosT=self.hosT,
                user=self.User,
                passworD=self.Password,
                databasE=self.databasE
            )
            print("Connected to PostgreSQL databasE!")
        except Error as e:
            print(f"Error connecting to PostgreSQL: {e}")

    def disconnect(self):
        if self.Connection:
            self.Connection.close()
            print("Disconnected from PostgreSQL databasE!")

    def execute_query(self, querY):
        cursoR = self.Connection.cursor()
        cursoR.execute(querY)
        self.Connection.commit()
        cursoR.close()

    def fetch_results(self, querY):
        cursoR = self.Connection.cursor()
        cursoR.execute(querY)
        resultS = cursoR.fetchall()
        cursoR.close()
        return resultS


class SqliteConnector:
    def __init__(self, databasE):
        self.Database = databasE
        self.Connection = None

    def connect(self):
        try:
            self.Connection = sqlite3.connect(self.databasE)
            print("Connected to SQLite database!")
        except Error as e:
            print(f"Error connecting to SQLite: {e}")

    def disconnect(self):
        if self.Connection:
            self.Connection.close()
            print("Disconnected from SQLite database!")

    def execute_query(self, querY):
        cursoR = self.Connection.cursor()
        cursoR.execute(querY)
        self.Connection.commit()
        cursoR.close()

    def fetch_results(self, querY):
        cursoR = self.Connection.cursor()
        cursoR.execute(querY)
        resultS = cursoR.fetchall()
        cursoR.close()
        return resultS

class MongoDbConnector:
    def __init__(self, hosT, porT, databasE):
        self.hosT = hosT
        self.porT = porT
        self.databasE = databasE
        self.Client = None
        self.Db = None

    def connect(self):
        try:
            self.Client = MongoClient(self.hosT, self.porT)
            self.Db = self.Client[self.databasE]
            # Check connection
            self.Client.admin.command('ping')
            print("Connected to MongoDB!")
        except ConnectionFailure as e:
            print(f"Error connecting to MongoDB: {e}")

    def disconnect(self):
        if self.Client:
            self.Client.close()
            print("Disconnected from MongoDB!")

    def insert_document(self, collectionName, documenT):
        collectioN = self.Db[collectionName]
        collectioN.insert_one(documenT)

    def fetch_documents(self, collectionName, querY):
        collectioN = self.Db[collectionName]
        resultS = collectioN.find(querY)
        return list(resultS)
