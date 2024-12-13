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
import logging
from ruamel.yaml import YAML


def connect_datasets(datasetList):
    # Concatena os datasets em uma única tabela (DataFrame) consolidada
    return pd.concat(datasetList, ignore_index=True)

class CsvPandasUnion:
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
        outputFile: str,
        encodinG: str = 'utf-8',
        seP: str = ',',
        specificColumns: Optional[List[str]] = None,
        filePattern: str = "*.csv"
    ) -> bool:
        """
        Concatena todos os arquivos CSV em um diretório específico.

        Args:
            inputDir (str): Caminho do diretório contendo os arquivos CSV
            outputFile (str): Caminho completo onde o arquivo concatenado será salvo
            encodinG (str, optional): Encoding dos arquivos CSV. Defaults to 'utf-8'.
            seP (str, optional): Separador usado nos arquivos CSV. Defaults to ','.
            specificColumns (List[str], optional): Lista específica de colunas para ler. 
                                                  Defaults to None (todas as colunas).
            filePattern (str, optional): Padrão para filtrar arquivos. Defaults to "*.csv".

        Returns:
            bool: True se a concatenação for bem-sucedida, False caso contrário.
        """
        try:
            # Converte o caminho de entrada para objeto Path
            input_path = Path(inputDir)
            
            # Verifica se o diretório existe
            if not input_path.exists():
                self.logger.error(f"Diretório não encontrado: {inputDir}")
                return False

            # Lista todos os arquivos CSV no diretório
            csvFiles = list(input_path.glob(filePattern))
            
            if not csvFiles:
                self.logger.warning(f"Nenhum arquivo CSV encontrado em: {inputDir}")
                return False

            self.logger.info(f"Encontrados {len(csvFiles)} arquivos CSV para processar")

            # Lista para armazenar os DataFrames
            dfS = []
            
            # Processa cada arquivo CSV
            for file in csvFiles:
                try:
                    self.logger.info(f"Processando arquivo: {file.name}")
                    
                    # Lê o CSV com as configurações especificadas
                    if specificColumns:
                        dF = pd.read_csv(
                            file,
                            encoding=encodinG,
                            sep=seP,
                            usecols=specificColumns
                        )
                    else:
                        dF = pd.read_csv(
                            file,
                            encoding=encodinG,
                            sep=seP
                        )
                    
                    # Adiciona uma coluna com o nome do arquivo fonte
                    dF['source_file'] = file.name
                    
                    dfS.append(dF)
                    
                except Exception as e:
                    self.logger.error(f"Erro ao processar arquivo {file.name}: {str(e)}")
                    continue

            if not dfS:
                self.logger.error("Nenhum DataFrame foi criado para concatenação")
                return False

            # Concatena todos os DataFrames
            finalDf = pd.concat(dfS, ignore_index=True)
            
            # Cria o diretório de saída se não existir
            outputFile = Path(outputFile)
            outputFile.parent.mkdir(parents=True, exist_ok=True)
            
            # Salva o DataFrame concatenado
            finalDf.to_csv(outputFile, index=False, encoding=encodinG, sep=seP)
            
            self.logger.info(f"""
            Concatenação concluída com sucesso!
            - Total de arquivos processados: {len(dfS)}
            - Total de linhas no arquivo final: {len(finalDf)}
            - Arquivo salvo em: {outputFile}
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


import mysql.connector
from mysql.connector import Error
from ruamel.yaml import YAML

class MySqlConnector:
    def __init__(self, dataContractPath):
        """
        Inicializa o conector MySQL com um Data Contract.
        
        :param dataContractPath: Caminho para o arquivo YAML do Data Contract
        """
        self.dataContract = self.loadDataContract(dataContractPath)
        self.connection = None

    def loadDataContract(self, path):
        """
        Carrega o Data Contract a partir de um arquivo YAML.
        
        :param path: Caminho para o arquivo YAML
        :return: Dicionário contendo o Data Contract
        """
        yaml = YAML(typ='safe')
        with open(path, 'r') as file:
            return yaml.load(file)

    def connect(self):
        """
        Estabelece uma conexão com o banco de dados MySQL usando as informações do Data Contract.
        """
        try:
            self.connection = mysql.connector.connect(
                host=self.dataContract['source']['details']['server'],
                user=self.dataContract['source']['connection']['username'],
                password=self.dataContract['source']['connection']['password'],
                database=self.dataContract['source']['details']['database']
            )
            if self.connection.is_connected():
                print(f"Connected to MySQL database: {self.dataContract['source']['details']['database']}")
        except Error as e:
            print(f"Error connecting to MySQL: {e}")

    def disconnect(self):
        """
        Encerra a conexão com o banco de dados MySQL.
        """
        if self.connection and self.connection.is_connected():
            self.connection.close()
            print(f"Disconnected from MySQL database: {self.dataContract['source']['details']['database']}")

    def executeQuery(self, query):
        """
        Executa uma query SQL no banco de dados.
        
        :param query: String contendo a query SQL a ser executada
        """
        if not self.connection or not self.connection.is_connected():
            raise ConnectionError("Not connected to the database")
        
        cursor = self.connection.cursor()
        try:
            cursor.execute(query)
            self.connection.commit()
        except Error as e:
            print(f"Error executing query: {e}")
        finally:
            cursor.close()

    def fetchResults(self, query):
        """
        Executa uma query SQL e retorna os resultados.
        
        :param query: String contendo a query SQL a ser executada
        :return: Lista de dicionários, onde cada dicionário representa uma linha do resultado
        """
        if not self.connection or not self.connection.is_connected():
            raise ConnectionError("Not connected to the database")
        
        cursor = self.connection.cursor(dictionary=True)
        try:
            cursor.execute(query)
            results = cursor.fetchall()
            return results
        except Error as e:
            print(f"Error fetching results: {e}")
            return None
        finally:
            cursor.close()


import psycopg2
import sqlite3
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
from ruamel.yaml import YAML

class PostgreSqlConnector:
    def __init__(self, dataContractPath):
        self.dataContract = self._load_data_contract(dataContractPath)
        self.Connection = None

    def _load_data_contract(self, path):
        yaml = YAML(typ='safe')
        with open(path, 'r') as file:
            return yaml.load(file)

    def connect(self):
        try:
            self.Connection = psycopg2.connect(
                host=self.dataContract['source']['details']['server'],
                user=self.dataContract['source']['connection']['username'],
                password=self.dataContract['source']['connection']['password'],
                database=self.dataContract['source']['details']['database']
            )
            print(f"Connected to PostgreSQL database: {self.dataContract['source']['details']['database']}")
        except Exception as e:
            print(f"Error connecting to PostgreSQL: {e}")

    def disconnect(self):
        if self.Connection:
            self.Connection.close()
            print(f"Disconnected from PostgreSQL database: {self.dataContract['source']['details']['database']}")

    def execute_query(self, query):
        cursor = self.Connection.cursor()
        cursor.execute(query)
        self.Connection.commit()
        cursor.close()

    def fetch_results(self, query):
        cursor = self.Connection.cursor()
        cursor.execute(query)
        results = cursor.fetchall()
        cursor.close()
        return results


class SqliteConnector:
    def __init__(self, dataContractPath):
        self.dataContract = self._load_data_contract(dataContractPath)
        self.Connection = None

    def _load_data_contract(self, path):
        yaml = YAML(typ='safe')
        with open(path, 'r') as file:
            return yaml.load(file)

    def connect(self):
        try:
            self.Connection = sqlite3.connect(self.dataContract['source']['details']['database'])
            print(f"Connected to SQLite database: {self.dataContract['source']['details']['database']}")
        except Exception as e:
            print(f"Error connecting to SQLite: {e}")

    def disconnect(self):
        if self.Connection:
            self.Connection.close()
            print(f"Disconnected from SQLite database: {self.dataContract['source']['details']['database']}")

    def execute_query(self, query):
        cursor = self.Connection.cursor()
        cursor.execute(query)
        self.Connection.commit()
        cursor.close()

    def fetch_results(self, query):
        cursor = self.Connection.cursor()
        cursor.execute(query)
        results = cursor.fetchall()
        cursor.close()
        return results


class MongoDbConnector:
    def __init__(self, dataContractPath):
        self.dataContract = self._load_data_contract(dataContractPath)
        self.Client = None
        self.Db = None

    def _load_data_contract(self, path):
        yaml = YAML(typ='safe')
        with open(path, 'r') as file:
            return yaml.load(file)

    def connect(self):
        try:
            self.Client = MongoClient(
                self.dataContract['source']['details']['server'],
                self.dataContract['source']['details']['port']
            )
            self.Db = self.Client[self.dataContract['source']['details']['database']]
            # Check connection
            self.Client.admin.command('ping')
            print(f"Connected to MongoDB: {self.dataContract['source']['details']['database']}")
        except ConnectionFailure as e:
            print(f"Error connecting to MongoDB: {e}")

    def disconnect(self):
        if self.Client:
            self.Client.close()
            print(f"Disconnected from MongoDB: {self.dataContract['source']['details']['database']}")

    def insert_document(self, collectionName, document):
        collection = self.Db[collectionName]
        collection.insert_one(document)

    def fetch_documents(self, collectionName, query):
        collection = self.Db[collectionName]
        results = collection.find(query)
        return list(results)
