#  Ingestão de Dados
#  Este módulo é responsável pela extração dos dados de diferentes fontes. 
# Ele conecta-se às APIs, bases de dados, ou lê arquivos CSV, JSON, etc.
# O módulo é responsável por coletar os dados e retorná-los em um formato
# tabular, geralmente um DataFrame do Pandas ou pyspark.

import requests
import pandas as pd
import sqlalchemy
import boto3
import json
import yaml
from datetime import datetime
from pathlib import Path
from typing import Dict, Union
from deepdiff import DeepDiff

class PandasCollect:
    def __init__(self, api_base_url: str = None):
        self.api_base_url = api_base_url

    def collect_from_api(self, api_url: str) -> pd.DataFrame:
        '''
        Coleta dados de uma API e retorna um DataFrame.
        Args:
            api_url (str): URL da API.
        Returns:
            pd.DataFrame: DataFrame com os dados da API.
        '''
        response = requests.get(api_url)
        data = response.json()
        return pd.DataFrame(data)

    def collect_from_database(self, connection_string: str, query: str) -> pd.DataFrame:
        '''
        Coleta dados de uma base de dados e retorna um DataFrame.
        Args:
            connection_string (str): String de conexão com a base de dados.
            query (str): Query SQL para coleta de dados.
        Returns:
            pd.DataFrame: DataFrame com os dados da base de dados.
        '''
        engine = sqlalchemy.create_engine(connection_string)
        data = pd.read_sql(query, engine)
        return data

    def collect_from_file(self, file_path: str) -> pd.DataFrame:
        '''
        Coleta dados de um arquivo e retorna um DataFrame.
        Args:
            file_path (str): Caminho do arquivo.
        Returns:
            pd.DataFrame: DataFrame com os dados do arquivo.
        '''
        data = pd.read_csv(file_path)
        return data

    def collect_aws_s3(self, bucket: str, file_key: str) -> pd.DataFrame:
        '''
        Coleta dados de um arquivo no AWS S3 e retorna um DataFrame.
        Args:
            bucket (str): Nome do bucket no S3.
            file_key (str): Caminho do arquivo no bucket.
        Returns:
            pd.DataFrame: DataFrame com os dados do arquivo no S3.
        '''
        s3 = boto3.client('s3')
        obj = s3.get_object(Bucket=bucket, Key=file_key)
        data = pd.read_csv(obj['Body'])
        return data

    def fetch_api_data(self, dataset_id: str) -> Union[Dict, None]:
        """Busca dados da API com tratamento de erros.
        Args:
            dataset_id (str): ID do dataset.
        Returns:
            Union[Dict, None]: Dados em formato JSON se bem-sucedido, None se falhar.
        """
        try:
            response = requests.get(f"{self.api_base_url}{dataset_id}")
            response.raise_for_status()  # Lança um erro para respostas ruins
            return response.json()  # Retorna a resposta JSON
        except requests.exceptions.RequestException as e:
            print(f"Erro ao buscar dados para {dataset_id}: {str(e)}")
            return None  # Retorna None se a requisição falhar

    def load_json_file(self, file_path: Path) -> Union[Dict, None]:
        """Carrega um arquivo JSON com tratamento de erros.
        Args:
            file_path (Path): Caminho do arquivo JSON.
        Returns:
            Union[Dict, None]: Representação em dicionário do JSON, ou None se falhar.
        """
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                return json.load(f)  # Retorna o conteúdo do arquivo JSON
        except Exception as e:
            print(f"Erro ao carregar o arquivo JSON {file_path}: {str(e)}")
            return None  # Retorna None em caso de erro

    def load_yaml_file(self, file_path: Path) -> Union[Dict, None]:
        """Carrega um arquivo YAML com tratamento de erros.
        Args:
            file_path (Path): Caminho do arquivo YAML.
        Returns:
            Union[Dict, None]: Representação em dicionário do YAML, ou None se falhar.
        """
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                return yaml.safe_load(f)  # Usa safe_load por motivos de segurança
        except Exception as e:
            print(f"Erro ao carregar o arquivo YAML {file_path}: {str(e)}")
            return None  # Retorna None em caso de erro

    def save_yaml_file(self, data: Dict, file_path: Path) -> bool:
        """Salva um dicionário como um arquivo YAML com tratamento de erros.
        Args:
            data (Dict): Dados a serem salvos como YAML.
            file_path (Path): Caminho para salvar o arquivo YAML.
        Returns:
            bool: True se salvo com sucesso, False em caso de erro.
        """
        try:
            with open(file_path, 'w', encoding='utf-8') as f:
                yaml.dump(data, f, default_flow_style=False, allow_unicode=True, sort_keys=False)
            return True  # Indica sucesso
        except Exception as e:
            print(f"Erro ao salvar o arquivo YAML {file_path}: {str(e)}")
            return False  # Retorna False em caso de erro

    def compare_json_data(self, old_data: Dict, new_data: Dict) -> bool:
        """Compara dados JSON usando DeepDiff.
        Args:
            old_data (Dict): Dados JSON originais.
            new_data (Dict): Novos dados JSON para comparação.
        Returns:
            bool: True se mudanças forem encontradas, False se idênticos.
        """
        diff = DeepDiff(old_data, new_data, ignore_order=True)
        return bool(diff)  # Retorna True se houver diferenças

    def update_yaml_metadata(self, yaml_data: Dict, new_source_data: Dict) -> Dict:
        """Atualiza metadados YAML com informações de versão e fonte.
        Args:
            yaml_data (Dict): Metadados atuais como um dicionário.
            new_source_data (Dict): Novos dados de fonte a serem incorporados.
        Returns:
            Dict: Metadados YAML atualizados.
        """
        current_version = max(int(v.get('id_1', '0').split('_')[1]) 
                            for v in yaml_data.get('version', [{'id_1': 'id_0'}]))

        new_version = {'id_' + str(current_version + 1): datetime.now().isoformat()}
        yaml_data['version'].append(new_version)  # Adiciona nova versão

        new_source = {
            'created': new_source_data.get('created'),
            'id': new_source_data.get('id'),
            'name': new_source_data.get('name')
        }

        source_exists = any(
            source.get('id') == new_source['id']
            for source in yaml_data.get('source', [])
        )

        if not source_exists:
            yaml_data['source'].append(new_source)  # Adiciona nova fonte se não estiver presente

        yaml_data['last_update'] = datetime.now().isoformat()  # Atualiza o timestamp da última atualização

        return yaml_data  # Retorna metadados atualizados
