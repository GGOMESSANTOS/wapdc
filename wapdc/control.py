# Monitoramento e Orquestração
# Data: 17/08/2021
# Descrição: Script de controle do pipeline de dados
# -------------------------------------------------------

# control.py
import logging
import time
import great_expectations as ge
from collect import *
from connect import *
from compose import *
from consume import *
from ruamel.yaml import YAML
from pathlib import Path
from typing import Dict, Union
from datetime import datetime
import json, os, requests
from pyspark.sql.types import *
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import to_timestamp, to_date
import pandas as pd

class GreatExpectations:
    def __init__(self, dataframe, expectations_config=None):
        """
        Inicializa o verificador de qualidade de dados com um DataFrame e configurações de expectativas.
        
        :param dataframe: pd.DataFrame - Dados a serem verificados.
        :param expectations_config: dict - Configurações para expectativas de qualidade de dados.
        """
        self.df = ge.from_pandas(dataframe)
        self.expectations_config = expectations_config or {}

    def set_expectations(self):
        """
        Define expectativas com base nas configurações fornecidas.
        """
        for field, expectations in self.expectations_config.get('fields', {}).items():
            if expectations.get('completeness', False):
                self.df.expect_column_values_to_not_be_null(field)

            if expectations.get('uniqueness', False):
                self.df.expect_column_values_to_be_unique(field)

            if 'consistency' in expectations:
                expected_values = expectations['consistency']
                self.df.expect_column_values_to_be_in_set(field, expected_values)

            if 'accuracy' in expectations:
                min_value = expectations['accuracy'].get('min', None)
                max_value = expectations['accuracy'].get('max', None)
                if min_value is not None:
                    self.df.expect_column_values_to_be_greater_than_or_equal_to(field, min_value)
                if max_value is not None:
                    self.df.expect_column_values_to_be_less_than_or_equal_to(field, max_value)

            if 'integrity' in expectations:
                foreign_key = expectations['integrity'].get('foreign_key')
                reference_values = expectations['integrity'].get('reference_values')
                if foreign_key and reference_values:
                    self.df.expect_column_values_to_be_in_set(foreign_key, reference_values)

    def run_quality_checks(self):
        """
        Executa as verificações de qualidade de dados com base nas expectativas definidas e retorna o resultado.
        """
        self.set_expectations()
        results = self.df.validate()
        if not results['success']:
            logging.warning("Uma ou mais verificações de qualidade falharam.")
        else:
            logging.info("Todas as verificações de qualidade foram bem-sucedidas.")
        return results

    def generate_report(self, report_path='data_quality_report.html'):
        """
        Gera um relatório HTML das verificações de qualidade de dados.
        
        :param report_path: Caminho onde o relatório será salvo.
        """
        results = self.run_quality_checks()
        validation_docs = ge.data_context.DataContext()
        validation_docs.build_data_docs()
        validation_docs.open_data_docs(results['run_id'])


class YamlHandler:

    def __init__(self):
        self.yaml = YAML()
        self.yaml.preserve_quotes = True
        self.yaml.indent(mapping=2, sequence=4, offset=2)
        self.yaml.allow_unicode = True
        self.yaml.default_flow_style = False

    def load_yaml_file(self, file_path: Path) -> Union[Dict, None]:
        """
        Carrega um arquivo YAML e retorna seu conteúdo como um dicionário.
        
        """
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                return self.yaml.load(f)
        except Exception as e:
            print(f"Erro ao carregar o arquivo YAML {file_path}: {str(e)}")
            return None

    def save_yaml_file(self, data: Dict, file_path: Path) -> bool:
        """Salva um dicionário em um arquivo YAML."""
        try:
            with open(file_path, 'w', encoding='utf-8') as f:
                self.yaml.dump(data, f)
            return True
        except Exception as e:
            print(f"Erro ao salvar o arquivo YAML {file_path}: {str(e)}")
            return False


    def download_json_metadata_ccee(self, output_dir: str) -> None:
        """
        Faz o download dos metadados dos pacotes da API da CCEE e os salva em arquivos JSON.

        Args:
            output_dir (str): Diretório onde os arquivos JSON serão salvos.
        """
        try:
            # URLs fixas da API CCEE
            API_BASE_URL = "https://dadosabertos.ccee.org.br/api/3/action/package_show?id="
            PACKAGE_LIST_URL = "https://dadosabertos.ccee.org.br/api/3/action/package_list"

            # Cria o diretório se não existir
            os.makedirs(output_dir, exist_ok=True)

            # Passo 1: Faz uma solicitação GET para buscar a lista de IDs dos pacotes
            response = requests.get(PACKAGE_LIST_URL)
            response.raise_for_status()

            # Obtém os dados da resposta
            response_data = response.json()

            # Verifica se a chave 'result' existe e é iterável
            if 'result' not in response_data or not isinstance(response_data['result'], list):
                raise ValueError("No results found in the package list or invalid format")

            # Passo 2: Itera sobre cada ID de pacote no resultado
            for package_id in response_data['result']:
                # Constrói a URL completa da API para o pacote atual
                api_url = f"{API_BASE_URL}{package_id}"

                # Faz uma solicitação GET para buscar os metadados do pacote
                package_response = requests.get(api_url)
                package_response.raise_for_status()

                # Obtém os dados da resposta dos metadados
                package_metadata = package_response.json()

                # Passo 3: Salva os metadados em um arquivo JSON nomeado pelo ID do pacote
                output_file = os.path.join(output_dir, f'{package_id}.json')

                with open(output_file, 'w', encoding='utf-8') as json_file:
                    json.dump(package_metadata, json_file, ensure_ascii=False, indent=4)

                print(f"Response for package ID '{package_id}' has been saved to {output_file}")

        except requests.RequestException as e:
            print(f"Network error occurred: {str(e)}")
        except json.JSONDecodeError as e:
            print(f"Error decoding JSON response: {str(e)}")
        except Exception as e:
            print(f"An unexpected error occurred: {str(e)}")

    def first_process_metadata_ccee_to_yaml_contract(self, json_data: Dict) -> Dict:
        """
        Primeira parte do data contract CCEE: Processa os dados em JSON e os converte em um formato estruturado.

        Args:
            json_data (Dict): Dados JSON do pacote CCEE.

        Returns:
            Dict: Metadados processados no formato estruturado.

        Raises:
            ValueError: Se houver erro no processamento dos dados.
        """
        try:
            # URL base fixa para consulta de dados
            DATASTORE_URL = "https://dadosabertos.ccee.org.br/api/3/action/datastore_search"
            
            result = json_data['result']
            
            # Estrutura básica dos metadados
            metadata = {
                'name': result['name'] + "_ccee_api",
                'description': result['notes'],
                'owner': result['author'],
                'version': [
                    {'1': datetime.now().isoformat()}
                ],
                'source': [],
                'last_update': datetime.now().isoformat(),
            }

            # Processamento dos recursos
            for resource in result['resources']:
                source_item = {
                    'created': resource['created'],
                    'id': resource['id'],
                    'name': resource['name'],
                    'format': resource['format'],
                    'url': resource['url']
                }
                metadata['source'].append(source_item)

            # Processamento das tags
            tags = [tag['display_name'] for tag in result['tags']]
            metadata['tags'] = sorted(list(set(tags)))
            metadata['sync'] = 's3://xxxxxxxxxxxxxxxx/xxxxxxxxxxx'

            # Processamento específico baseado no nome do dataset
            special_datasets = ["consumo_horario_perfil_agente_ccee_api", "geracao_horaria_usina_ccee_api"]
            
            if metadata['name'] not in special_datasets:
                try:
                    id_schema_source = metadata['source'][0]['id']
                    url_dataset_id = f"{DATASTORE_URL}?resource_id={id_schema_source}&limit=0"
                    
                    response = requests.get(url_dataset_id)
                    response.raise_for_status()
                    json_response = response.json()

                    # Processamento dos campos
                    fields = {}
                    for field in json_response['result']['fields'][1:]:
                        field_id = field.get('id', '')
                        info = field.get('info', {})

                        fields[field_id] = {
                            'type': field.get('type', ''),
                            'description': info.get('notes', ''),
                            'label': info.get('label', ''),
                            'business_rule': ''
                        }

                    metadata['fields'] = fields

                    # Extração dos anos dos dados (formato padrão)
                    years = set()
                    for source in metadata['source']:
                        name = source.get('name', '')
                        if name and len(name) >= 4:
                            year = name[-4:]
                            if year.isdigit():
                                years.add(year)

                except (requests.RequestException, KeyError, IndexError) as e:
                    raise ValueError(f"Error processing dataset fields: {str(e)}")

            else:
                # Processamento especial para datasets específicos
                years = set()
                for source in metadata['source']:
                    name = source.get('name', '')
                    if name and len(name) >= 6:
                        year = name[-6:-2]
                        if year.isdigit():
                            years.add(year)

            metadata['years_of_data'] = sorted(list(years))
            return metadata

        except KeyError as e:
            raise ValueError(f"Missing required field in JSON data: {str(e)}")
        except Exception as e:
            raise ValueError(f"Error processing metadata: {str(e)}")

    def update_sources_ccee_data_contract(self, source_dir, yaml_base_dir):
            """
            Atualiza os contratos YAML com base nos arquivos JSON na pasta source_metadata.
            """
            failed_metadata = []

            for json_file in Path(source_dir).glob('*.json'):
                with open(json_file, 'r', encoding='utf-8') as file:
                    json_data = json.load(file)

                try:
                    yaml_data = self.first_process_metadata_ccee_to_yaml_contract(json_data)
                except ValueError as e:
                    failed_metadata.append(json_data['result']['name'])
                    print(f"Error processing '{json_data['result']['name']}': {e}")
                    continue

                table_name = json_data['result']['name'] + "_ccee_api"
                current_date = datetime.now().strftime('%Y%m%d')

                yaml_dir = Path(yaml_base_dir) / table_name / current_date
                yaml_dir.mkdir(parents=True, exist_ok=True)

                yaml_filename = f"{table_name}.yaml"
                yaml_path = yaml_dir / yaml_filename

                self.save_yaml_file(yaml_data, yaml_path)

                print(f"YAML contract for '{json_data['result']['name']}' has been updated at {yaml_path}")

            if failed_metadata:
                print("The following metadata could not be processed:")
                for name in failed_metadata:
                    print(f"- {name}")

class SparkDataMonitor:

    def __init__(self, monitor_path: str, spark: SparkSession):
        """
        Parameters:
            monitor_path: path from monitor table
            spark: spark session
        """

        self.monitor_path = monitor_path
        self.spark = spark

        try:
            self.monitor_table = self.read_monitor_table()
        except:
            self.monitor_table = self.create_monitor_table()
            self.save_monitor_table()

    def create_monitor_table(self) -> DataFrame:
        """
        Function to create a table to monitoring process. It's initialize a schema and create a empty spark dataframe

        Returns:
            monitor: Empty spark dataframe with an schema
        """

        self.schema = StructType([
            StructField("process_id", StringType(), True), # process unique id
            StructField("table_name", StringType(), True), # processing table name
            StructField("source", StringType(), True), # table product name
            StructField("layer", StringType(), True), # layer table name
            StructField("process_name", StringType(), True), # .py filename
            StructField("event_type", StringType(), True), # event type (start, end, processed_lines)
            StructField("values", FloatType(), True), # Any value from an event
            StructField("event_date", DateType(), True), # event start date
            StructField("event_timestamp", TimestampType(), True), # event timestamp
            StructField("log", StringType(), True) # event error log
        ])

        # Crie um DataFrame vazio com o esquema especificado
        monitor_table = self.spark.createDataFrame([], self.schema)

        return monitor_table
    
    def read_monitor_table(self) -> DataFrame:
        """
        Function to read a delta table from path

        Returns:
            monitor_table: spark dataframe
        """
        monitor_table = self.spark.read.format("delta").load(self.monitor_path)
        return monitor_table
    
    def save_monitor_table(self,
        write_format: str = "delta",
        write_mode: str = "overwrite",
        partition_field: str = "event_date",
        options: dict = {},
        ):
        """
        Generic function to write a file on s3, specifying some parameters and writing on s3

        Parameters:
            write_format: format to save df (parquet,csv,delta,...)
            write_mode: mode to save df (overwrite,...)
            partition_field: column on df to make a partition
            options: aditional configurations
        
        """
        if partition_field:
            (
                self.monitor_table.write.format(write_format)
                .mode(write_mode)
                .partitionBy(partition_field)
                .options(**options)
                .save(self.monitor_path)
            )
        else:
            (self.monitor_table.write.format(write_format).mode(write_mode).options(**options).save(self.monitor_path))
    
    def write_monitor_table(self, monitor_dict: dict):
        """
        Function to add a line on monitor_table and change columns type

        Parameters:
            monitor_dict: monitor dictionary filled
        """
        new_row = self.spark.createDataFrame([list(monitor_dict.values())], list(monitor_dict.keys()))
        self.monitor_table = self.monitor_table.union(new_row)
        self.monitor_table = self.monitor_table.withColumn("values", self.monitor_table["values"].cast("float")) \
                        .withColumn("event_date", to_date("event_date", "yyyy-MM-dd")) \
                        .withColumn("event_timestamp", to_timestamp("event_timestamp", "yyyy-MM-dd HH:mm:ss.SSS")) 
                        
    
    def get_monitor_dict(self,process_id: str,table_name: str,source: str,process_name: str,layer: str) -> dict:
        """
        Function do return a dictionary with some default values

        Parameters:
            process_id: process unique id
            table_name: processing table name
            source: table product name
            process_name: .py filename
            layer: layer table name
        
        Returns:
            monitor_dict: dictonary with default values from process
        """
        monitor_dict = {
            "process_id":process_id,
            "table_name":table_name,
            "source":source,
            "layer":layer,
            "process_name":process_name,
            "event_type":"",
            "values":"",
            "event_date":"",
            "event_timestamp":"",
            "log":""
        }
        return monitor_dict

    def fill_monitor_dict(self,monitor_dict: dict,values: dict) -> dict:
        """
        Function to fill a dictionary with values

        Parameters:
            monitor_dict: monitor dictionary
            values: values to be filled
        
        Returns:
            monitor_dict: monitor dictionary filled
        """
        for key,value in values.items():
            monitor_dict[key] = value
        return monitor_dict

class PandasDataMonitor:

    def __init__(self, monitor_path: str):
        """
        Parâmetros:
            monitor_path: caminho do arquivo CSV para monitoramento
        """
        self.monitor_path = monitor_path

        try:
            self.monitor_table = self.read_monitor_table()
        except FileNotFoundError:
            self.monitor_table = self.create_monitor_table()
            self.save_monitor_table()

    def create_monitor_table(self) -> pd.DataFrame:
        """
        Função para criar uma tabela para monitorar o processo. Inicializa a estrutura do DataFrame e cria um DataFrame vazio.

        Retorna:
            monitor: DataFrame vazio com as colunas especificadas.
        """
        self.columns = [
            "process_id",       # ID único do processo
            "table_name",       # Nome da tabela em processamento
            "source",           # Nome do produto da tabela
            "layer",            # Nome da camada
            "process_name",     # Nome do arquivo .py
            "event_type",       # Tipo de evento (início, fim, linhas processadas)
            "values",           # Qualquer valor de um evento
            "event_date",       # Data de início do evento
            "event_timestamp",   # Timestamp do evento
            "log"               # Log de erro do evento
        ]

        # Cria um DataFrame vazio com as colunas especificadas
        monitor_table = pd.DataFrame(columns=self.columns)
        return monitor_table
    
    def read_monitor_table(self) -> pd.DataFrame:
        """
        Função para ler a tabela do CSV.

        Retorna:
            monitor_table: DataFrame do Pandas
        """
        monitor_table = pd.read_csv(self.monitor_path)
        monitor_table['event_date'] = pd.to_datetime(monitor_table['event_date'], errors='coerce')
        monitor_table['event_timestamp'] = pd.to_datetime(monitor_table['event_timestamp'], errors='coerce')
        return monitor_table
    
    def save_monitor_table(self):
        """
        Função para salvar a tabela de monitoramento em CSV.
        """
        self.monitor_table.to_csv(self.monitor_path, index=False)
    
    def write_monitor_table(self, monitor_dict: dict):
        """
        Função para adicionar uma linha na monitor_table.

        Parâmetros:
            monitor_dict: dicionário contendo os dados a serem adicionados
        """
        new_row = pd.DataFrame([monitor_dict])
        self.monitor_table = pd.concat([self.monitor_table, new_row], ignore_index=True)
        
        # Converte os tipos de coluna conforme necessário
        self.monitor_table['values'] = self.monitor_table['values'].astype(float)
        self.monitor_table['event_date'] = pd.to_datetime(self.monitor_table['event_date'], errors='coerce')
        self.monitor_table['event_timestamp'] = pd.to_datetime(self.monitor_table['event_timestamp'], errors='coerce')

    def get_monitor_dict(self, process_id: str, table_name: str, source: str, process_name: str, layer: str) -> dict:
        """
        Função para retornar um dicionário com alguns valores padrão.

        Parâmetros:
            process_id: ID único do processo
            table_name: Nome da tabela em processamento
            source: Nome do produto da tabela
            process_name: Nome do arquivo .py
            layer: Nome da camada

        Retorna:
            monitor_dict: dicionário com valores padrão do processo
        """
        monitor_dict = {
            "process_id": process_id,
            "table_name": table_name,
            "source": source,
            "layer": layer,
            "process_name": process_name,
            "event_type": "",
            "values": "",
            "event_date": None,
            "event_timestamp": None,
            "log": ""
        }
        return monitor_dict

    def fill_monitor_dict(self, monitor_dict: dict, values: dict) -> dict:
        """
        Função para preencher um dicionário com valores.

        Parâmetros:
            monitor_dict: dicionário de monitoramento
            values: valores a serem preenchidos

        Retorna:
            monitor_dict: dicionário de monitoramento preenchido
        """
        for key, value in values.items():
            monitor_dict[key] = value
        return monitor_dict


