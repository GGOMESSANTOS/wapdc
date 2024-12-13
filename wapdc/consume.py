# consume.py
# A
# Data: 2020-12-20
# Descrição: Funções para consumir dados de diferentes fontes. 
# Democratização dos Dados
# ----------------------------------------------------------

import os
import pandas as pd
from datetime import datetime
from typing import List, Optional, Union
import boto3  # For AWS S3
from azure.storage.blob import BlobServiceClient  # For Azure Blob Storage

class DataSaver:
    """
    Classe abrangente para salvar dados em diferentes formatos e tecnologias.
    Suporta Pandas, PySpark, Delta, Iceberg, S3 e Azure.
    """

    @staticmethod
    def save_to_database(
        df: Union[pd.DataFrame, 'spark.DataFrame'], 
        connection_string: str, 
        table_name: str, 
        mode: str = 'replace', 
        engine: str = 'pandas'
    ):
        """
        Salva DataFrame em banco de dados usando SQLAlchemy ou Spark.
        
        Args:
            df: DataFrame para salvar (Pandas ou Spark)
            connection_string: String de conexão com o banco
            table_name: Nome da tabela
            mode: Modo de gravação (replace/append)
            engine: Tipo de engine (pandas/spark)
        """
        if engine == 'pandas':
            import sqlalchemy
            sql_engine = sqlalchemy.create_engine(connection_string)
            df.to_sql(table_name, sql_engine, if_exists=mode, index=False)
        elif engine == 'spark':
            df.write \
                .format("jdbc") \
                .option("url", connection_string) \
                .option("dbtable", table_name) \
                .mode(mode) \
                .save()
        else:
            raise ValueError("Engine deve ser 'pandas' ou 'spark'")
        
    @classmethod        
    def save_json_file(self, data: Dict, file_path: Path) -> bool:
        """Salva um arquivo JSON com tratamento de erros.

        Este método recebe um dicionário e o salva no caminho especificado como um arquivo JSON.
        Se a operação falhar (por exemplo, problemas de permissão ou espaço em disco),
        uma mensagem de erro será exibida e False será retornado.
        """
        try:
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=4)  # Salva o dicionário como JSON formatado
            return True  # Retorna True se o arquivo foi salvo com sucesso
        except Exception as e:
            print(f"Erro ao salvar o arquivo JSON {file_path}: {str(e)}")
            return False  # Retorna False em caso de erro

    @classmethod
    def save_dataframe(
        df: Union[pd.DataFrame, 'spark.DataFrame'], 
        file_path: str, 
        format: str = 'csv', 
        mode: str = 'w', 
        engine: str = 'pandas',
        **kwargs
    ):
        """
        Salva DataFrame em diferentes formatos de arquivo, incluindo JSON e XLSX.
        
        Args:
            df: DataFrame para salvar
            file_path: Caminho do arquivo
            format: Formato de salvamento (csv/json/xlsx/parquet/delta/iceberg)
            mode: Modo de gravação
            engine: Tipo de engine (pandas/spark)
        """
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        
        if engine == 'pandas':
            if format == 'csv':
                df.to_csv(file_path, index=False, mode=mode)
            elif format == 'parquet':
                df.to_parquet(file_path, index=False)
            elif format == 'json':
                df.to_json(file_path, orient='records', lines=True)
            elif format == 'xlsx':
                with pd.ExcelWriter(file_path, engine='xlsxwriter') as writer:
                    df.to_excel(writer, index=False, sheet_name='Sheet1')
            else:
                raise ValueError("Formato não suportado para Pandas")
        
        elif engine == 'spark':
            if format == 'csv':
                df.write.mode(mode).csv(file_path)
            elif format == 'parquet':
                df.write.mode(mode).parquet(file_path)
            elif format == 'delta':
                df.write \
                    .format("delta") \
                    .mode(mode) \
                    .save(file_path)
            elif format == 'iceberg':
                df.writeTo(file_path) \
                    .using("iceberg") \
                    .createOrReplace() if mode == 'overwrite' else \
                    df.writeTo(file_path) \
                    .using("iceberg") \
                    .append()
            else:
                raise ValueError("Formato não suportado para Spark")
        else:
            raise ValueError("Engine deve ser 'pandas' ou 'spark'")

    @classmethod
    def save_incremental(
        cls, 
        df: Union[pd.DataFrame, 'spark.DataFrame'], 
        base_path: str, 
        prefix: str = 'data', 
        format: str = 'csv', 
        engine: str = 'pandas'
    ):
        """
        Salva dados incrementalmente com timestamp no nome do arquivo.
        
        Args:
            df: DataFrame para salvar
            base_path: Diretório base para salvamento
            prefix: Prefixo do nome do arquivo
            format: Formato de salvamento
            engine: Tipo de engine
        """
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{prefix}_{timestamp}"
        full_path = os.path.join(base_path, filename)
        
        cls.save_to_file(df, full_path, format=format, engine=engine)

    @staticmethod
    def merge_table(
        spark: 'SparkSession', 
        target_table: str, 
        source_df: 'spark.DataFrame', 
        merge_condition: str,
        update_cols: Optional[List[str]] = None,
        insert_cols: Optional[List[str]] = None,
        table_type: str = 'delta'
    ):
        """
        Realiza merge em tabelas Delta ou Iceberg.
        
        Args:
            spark: Sessão Spark
            target_table: Caminho/identificador da tabela de destino
            source_df: DataFrame fonte para merge
            merge_condition: Condição de merge
            update_cols: Colunas para atualizar
            insert_cols: Colunas para inserir
            table_type: Tipo de tabela (delta/iceberg)
        """
        source_df.createOrReplaceTempView("source_table")
        
        if table_type == 'delta':
            from delta.tables import DeltaTable
            deltaTable = DeltaTable.forPath(spark, target_table)
            
            mergeOperation = deltaTable.alias("target").merge(
                source_df.alias("source"), 
                merge_condition
            )
            
            if update_cols:
                update_dict = {f"target.{col}": f"source.{col}" for col in update_cols}
                merge_operation = merge_operation.whenMatchedUpdate(set=update_dict)
            
            if insert_cols:
                insert_dict = {col: f"source.{col}" for col in insert_cols}
                merge_operation = merge_operation.whenNotMatchedInsert(values=insert_dict)
            
            merge_operation.execute()
        
        elif table_type == 'iceberg':
            merge_sql = f"""
            MERGE INTO {target_table} AS target
            USING (SELECT * FROM source_table) AS source
            ON {merge_condition}
            """
            
            if update_cols:
                update_clause = "WHEN MATCHED THEN UPDATE SET " + \
                    ", ".join([f"target.{col} = source.{col}" for col in update_cols])
                merge_sql += " " + update_clause
            
            if insert_cols:
                insert_clause = "WHEN NOT MATCHED THEN INSERT " + \
                    "(" + ", ".join(insert_cols) + ") " + \
                    "VALUES (" + ", ".join([f"source.{col}" for col in insert_cols]) + ")"
                merge_sql += " " + insert_clause
            
            spark.sql(merge_sql)
        
        else:
            raise ValueError("Tipo de tabela deve ser 'delta' ou 'iceberg'")

    @staticmethod
    def vacuum_table(
        spark: 'SparkSession', 
        table_path: str, 
        table_type: str = 'delta', 
        retention_hours: int = 168
    ):
        """
        Realiza vacuum em tabelas Delta ou limpeza de snapshots em Iceberg.
        
        Args:
            spark: Sessão Spark
            table_path: Caminho da tabela
            table_type: Tipo de tabela
            retention_hours: Horas para retenção
        """
        if table_type == 'delta':
            from delta.tables import DeltaTable
            delta_table = DeltaTable.forPath(spark, table_path)
            delta_table.vacuum(retention_hours)
        
        elif table_type == 'iceberg':
            # Para Iceberg, usa SQL para remover snapshots antigos
            spark.sql(f"""
                DELETE FROM {table_path}.snapshots 
                WHERE committed_at < current_timestamp() - INTERVAL {retention_hours} HOURS
            """)
        
        else:
            raise ValueError("Tipo de tabela deve ser 'delta' ou 'iceberg'")

    @classmethod
    def save_to_s3(
        cls, 
        df: pd.DataFrame, 
        bucket_name: str, 
        file_name: str, 
        format: str = 'csv', 
        credentials: Optional[dict] = None
    ):
        """
        Salva um DataFrame em um bucket S3.
        
        Args:
            df: DataFrame para salvar
            bucket_name: Nome do bucket S3
            file_name: Nome do arquivo a ser salvo
            format: Formato de salvamento (csv/json/xlsx)
            credentials: Dicionário de credenciais AWS (opcional)
        """
        if credentials:
            boto3.setup_default_session(
                aws_access_key_id=credentials['aws_access_key_id'],
                aws_secret_access_key=credentials['aws_secret_access_key'],
                region_name=credentials.get('region', 'us-west-2')
            )

        if format == 'csv':
            csv_buffer = df.to_csv(index=False, encoding='utf-8')
            s3_resource = boto3.resource('s3')
            s3_resource.Object(bucket_name, file_name).put(Body=csv_buffer)
        elif format == 'json':
            json_buffer = df.to_json(orient='records')
            s3_resource = boto3.resource('s3')
            s3_resource.Object(bucket_name, file_name).put(Body=json_buffer)
        elif format == 'xlsx':
            import io
            excel_buffer = io.BytesIO()
            with pd.ExcelWriter(excel_buffer, engine='xlsxwriter') as writer:
                df.to_excel(writer, index=False, sheet_name='Sheet1')
            s3_resource = boto3.resource('s3')
            s3_resource.Object(bucket_name, file_name).put(Body=excel_buffer.getvalue())
        else:
            raise ValueError("Formato não suportado para S3")

    @classmethod
    def save_to_azure_blob(
        cls, 
        df: pd.DataFrame, 
        container_name: str, 
        blob_name: str, 
        format: str = 'csv', 
        connection_string: str = None
    ):
        """
        Salva um DataFrame em um container Azure Blob Storage.
        
        Args:
            df: DataFrame para salvar
            container_name: Nome do container
            blob_name: Nome do blob a ser salvo
            format: Formato de salvamento (csv/json/xlsx)
            connection_string: String de conexão com Azure Blob Storage
        """
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)

        if format == 'csv':
            csv_buffer = df.to_csv(index=False)
            blob_client.upload_blob(csv_buffer, overwrite=True)
        elif format == 'json':
            json_buffer = df.to_json(orient='records')
            blob_client.upload_blob(json_buffer, overwrite=True)
        elif format == 'xlsx':
            import io
            excel_buffer = io.BytesIO()
            with pd.ExcelWriter(excel_buffer, engine='xlsxwriter') as writer:
                df.to_excel(writer, index=False, sheet_name='Sheet1')
            blob_client.upload_blob(excel_buffer.getvalue(), overwrite=True)
        else:
            raise ValueError("Formato não suportado para Azure Blob Storage")



