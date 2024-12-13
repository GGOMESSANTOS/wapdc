import os
import pandas as pd
import json
from datetime import datetime
from typing import Dict, List, Optional, Union
from pathlib import Path
import boto3
from azure.storage.blob import BlobServiceClient
from delta.tables import DeltaTable

class PandasDataSaver:


    @staticmethod
    def pandas_save_json_file(data: Dict, filePath: Path) -> bool:
        try:
            with open(filePath, 'w', encoding='utf-8') as file:
                json.dump(data, file, ensure_ascii=False, indent=4)
            return True
        except Exception as e:
            print(f"Error saving JSON file {filePath}: {str(e)}")
            return False

    @staticmethod
    def pandas_save_dataframe_csv(dataFrame: pd.DataFrame, filePath: str, mode: str = 'w', **kwargs):
        os.makedirs(os.path.dirname(filePath), exist_ok=True)
        dataFrame.to_csv(filePath, index=False, mode=mode, **kwargs)

    @staticmethod
    def pandas_save_dataframe_parquet(dataFrame: pd.DataFrame, filePath: str, **kwargs):
        os.makedirs(os.path.dirname(filePath), exist_ok=True)
        dataFrame.to_parquet(filePath, index=False, **kwargs)

    @staticmethod
    def pandas_save_dataframe_json(dataFrame: pd.DataFrame, filePath: str, **kwargs):
        os.makedirs(os.path.dirname(filePath), exist_ok=True)
        dataFrame.to_json(filePath, orient='records', lines=True, **kwargs)

    @staticmethod
    def pandas_save_dataframe_xlsx(dataFrame: pd.DataFrame, filePath: str, **kwargs):
        os.makedirs(os.path.dirname(filePath), exist_ok=True)
        with pd.ExcelWriter(filePath, engine='xlsxwriter') as writer:
            dataFrame.to_excel(writer, index=False, sheet_name='Sheet1', **kwargs)

    @classmethod
    def pandas_save_incremental(cls, dataFrame: pd.DataFrame, basePath: str, prefix: str = 'data', fileFormat: str = 'csv', **kwargs):
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        fileName = f"{prefix}_{timestamp}.{fileFormat}"
        fullPath = os.path.join(basePath, fileName)
        
        saveMethod = getattr(cls, f"pandas_save_dataframe_{fileFormat}")
        saveMethod(dataFrame, fullPath, **kwargs)

    @staticmethod
    def pandas_save_to_s3_csv(dataFrame: pd.DataFrame, bucketName: str, fileName: str, credentials: Optional[dict] = None, **kwargs):
        if credentials:
            boto3.setup_default_session(**credentials)
        s3Resource = boto3.resource('s3')
        csvBuffer = dataFrame.to_csv(index=False, encoding='utf-8', **kwargs)
        s3Resource.Object(bucketName, fileName).put(Body=csvBuffer)

    @staticmethod
    def pandas_save_to_s3_json(dataFrame: pd.DataFrame, bucketName: str, fileName: str, credentials: Optional[dict] = None, **kwargs):
        if credentials:
            boto3.setup_default_session(**credentials)
        s3Resource = boto3.resource('s3')
        jsonBuffer = dataFrame.to_json(orient='records', **kwargs)
        s3Resource.Object(bucketName, fileName).put(Body=jsonBuffer)

    @staticmethod
    def pandas_save_to_s3_xlsx(dataFrame: pd.DataFrame, bucketName: str, fileName: str, credentials: Optional[dict] = None, **kwargs):
        if credentials:
            boto3.setup_default_session(**credentials)
        s3Resource = boto3.resource('s3')
        import io
        excelBuffer = io.BytesIO()
        with pd.ExcelWriter(excelBuffer, engine='xlsxwriter') as writer:
            dataFrame.to_excel(writer, index=False, sheet_name='Sheet1', **kwargs)
        s3Resource.Object(bucketName, fileName).put(Body=excelBuffer.getvalue())

    @staticmethod
    def pandas_save_to_azure_blob_csv(dataFrame: pd.DataFrame, containerName: str, blobName: str, connectionString: str, **kwargs):
        blobServiceClient = BlobServiceClient.from_connection_string(connectionString)
        blobClient = blobServiceClient.get_blob_client(container=containerName, blob=blobName)
        csvBuffer = dataFrame.to_csv(index=False, **kwargs)
        blobClient.upload_blob(csvBuffer, overwrite=True)

    @staticmethod
    def pandas_save_to_azure_blob_json(dataFrame: pd.DataFrame, containerName: str, blobName: str, connectionString: str, **kwargs):
        blobServiceClient = BlobServiceClient.from_connection_string(connectionString)
        blobClient = blobServiceClient.get_blob_client(container=containerName, blob=blobName)
        jsonBuffer = dataFrame.to_json(orient='records', **kwargs)
        blobClient.upload_blob(jsonBuffer, overwrite=True)

    @staticmethod
    def pandas_save_to_azure_blob_xlsx(dataFrame: pd.DataFrame, containerName: str, blobName: str, connectionString: str, **kwargs):
        blobServiceClient = BlobServiceClient.from_connection_string(connectionString)
        blobClient = blobServiceClient.get_blob_client(container=containerName, blob=blobName)
        import io
        excelBuffer = io.BytesIO()
        with pd.ExcelWriter(excelBuffer, engine='xlsxwriter') as writer:
            dataFrame.to_excel(writer, index=False, sheet_name='Sheet1', **kwargs)
        blobClient.upload_blob(excelBuffer.getvalue(), overwrite=True)

class SparkDataSaver:
    @staticmethod
    def spark_save_to_database(dataFrame: 'spark.DataFrame', connectionString: str, tableName: str, mode: str = 'overwrite'):
        dataFrame.write \
            .format("jdbc") \
            .option("url", connectionString) \
            .option("dbtable", tableName) \
            .mode(mode) \
            .save()

    @staticmethod
    def spark_save_dataframe_csv(dataFrame: 'spark.DataFrame', filePath: str, mode: str = 'overwrite', **kwargs):
        dataFrame.write.mode(mode).csv(filePath, **kwargs)

    @staticmethod
    def spark_save_dataframe_parquet(dataFrame: 'spark.DataFrame', filePath: str, mode: str = 'overwrite', **kwargs):
        dataFrame.write.mode(mode).parquet(filePath, **kwargs)

    @staticmethod
    def spark_save_dataframe_delta(dataFrame: 'spark.DataFrame', filePath: str, mode: str = 'overwrite', **kwargs):
        dataFrame.write.format("delta").mode(mode).save(filePath, **kwargs)

    @staticmethod
    def spark_save_dataframe_iceberg(dataFrame: 'spark.DataFrame', filePath: str, mode: str = 'overwrite', **kwargs):
        if mode == 'overwrite':
            dataFrame.writeTo(filePath).using("iceberg").createOrReplace()
        else:
            dataFrame.writeTo(filePath).using("iceberg").append()

    @classmethod
    def spark_save_incremental(cls, dataFrame: 'spark.DataFrame', basePath: str, prefix: str = 'data', fileFormat: str = 'csv', **kwargs):
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        fileName = f"{prefix}_{timestamp}"
        fullPath = os.path.join(basePath, fileName)
        
        saveMethod = getattr(cls, f"spark_save_dataframe_{fileFormat}")
        saveMethod(dataFrame, fullPath, **kwargs)

    @staticmethod
    def spark_merge_table(spark: 'SparkSession', targetTable: str, sourceDataFrame: 'spark.DataFrame', mergeCondition: str,
                          updateCols: Optional[List[str]] = None, insertCols: Optional[List[str]] = None, tableType: str = 'delta'):
        sourceDataFrame.createOrReplaceTempView("source_table")
        
        if tableType == 'delta':
            from delta.tables import DeltaTable
            deltaTable = DeltaTable.forPath(spark, targetTable)
            
            mergeOperation = deltaTable.alias("target").merge(
                sourceDataFrame.alias("source"), 
                mergeCondition
            )
            
            if updateCols:
                updateDict = {f"target.{col}": f"source.{col}" for col in updateCols}
                mergeOperation = mergeOperation.whenMatchedUpdate(set=updateDict)
            
            if insertCols:
                insertDict = {col: f"source.{col}" for col in insertCols}
                mergeOperation = mergeOperation.whenNotMatchedInsert(values=insertDict)
            
            mergeOperation.execute()
        
        elif tableType == 'iceberg':
            mergeSql = f"""
            MERGE INTO {targetTable} AS target
            USING (SELECT * FROM source_table) AS source
            ON {mergeCondition}
            """
            
            if updateCols:
                updateClause = "WHEN MATCHED THEN UPDATE SET " + \
                    ", ".join([f"target.{col} = source.{col}" for col in updateCols])
                mergeSql += " " + updateClause
            
            if insertCols:
                insertClause = "WHEN NOT MATCHED THEN INSERT " + \
                    "(" + ", ".join(insertCols) + ") " + \
                    "VALUES (" + ", ".join([f"source.{col}" for col in insertCols]) + ")"
                mergeSql += " " + insertClause
            
            spark.sql(mergeSql)
        
        else:
            raise ValueError("Table type must be 'delta' or 'iceberg'")

    @staticmethod
    def spark_vacuum_table(spark: 'SparkSession', tablePath: str, tableType: str = 'delta', retentionHours: int = 168):
        if tableType == 'delta':
           
            deltaTable = DeltaTable.forPath(spark, tablePath)
            deltaTable.vacuum(retentionHours)
        
        elif tableType == 'iceberg':
            spark.sql(f"""
                DELETE FROM {tablePath}.snapshots 
                WHERE committed_at < current_timestamp() - INTERVAL {retentionHours} HOURS
            """)
        
        else:
            raise ValueError("Table type must be 'delta' or 'iceberg'")
