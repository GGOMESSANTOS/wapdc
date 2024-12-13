# compose.py
# Data: 14/11/2024
# Descrição: Este módulo realiza a limpeza e transformação dos dados
# coletados. Ele é responsável por preparar os dados para análise.
# ----------------------------------------------------------
import pandas as pd
from pyspark.sql import DataFrame as PySparkDataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *

class Transformation:
    """
    Classe que reúne funções de transformação de dados, compatível com 
    Pandas e PySpark, focada em tabelas do setor de energia e CCEE.
    """

    @staticmethod
    def clean_data(df):
        """
        Limpa os dados removendo valores nulos e outliers.

        Args:
            df (pd.DataFrame ou pyspark.sql.DataFrame): DataFrame de entrada.

        Returns:
            pd.DataFrame ou pyspark.sql.DataFrame: DataFrame limpo.
        """
        if isinstance(df, pd.DataFrame):
            # Para Pandas, remove valores nulos
            return df.dropna()
        elif isinstance(df, PySparkDataFrame):
            # Para PySpark, remove valores nulos
            return df.na.drop()
        else:
            raise TypeError("O objeto fornecido não é um DataFrame Pandas ou PySpark.")

    @staticmethod
    def columns_to_lower(df):
        """
        Converte todos os nomes de colunas de um DataFrame para letras minúsculas, 
        substituindo espaços por underscores ("_").

        Args:
            df (pd.DataFrame ou pyspark.sql.DataFrame): O DataFrame cujas colunas serão convertidas.

        Returns:
            pd.DataFrame ou pyspark.sql.DataFrame: O DataFrame com os nomes de colunas transformados.
        """
        if isinstance(df, pd.DataFrame):
            df.columns = [col.lower().replace(" ", "_") for col in df.columns]
            return df
        elif isinstance(df, PySparkDataFrame):
            for column in df.columns:
                df = df.withColumnRenamed(column, column.lower().replace(" ", "_"))
            return df
        else:
            raise TypeError("O objeto fornecido não é um DataFrame Pandas ou PySpark.")

    @staticmethod
    def convert_date_column(df, column_name, format="%Y-%m-%d"):
        """
        Converte uma coluna em formato de string para o tipo de data.

        Args:
            df (pd.DataFrame ou pyspark.sql.DataFrame): DataFrame de entrada.
            column_name (str): Nome da coluna a ser convertida.
            format (str): Formato esperado da data (apenas para Pandas).

        Returns:
            pd.DataFrame ou pyspark.sql.DataFrame: DataFrame com a coluna convertida.
        """
        if isinstance(df, pd.DataFrame):
            # Pandas: usa pd.to_datetime para converter coluna
            df[column_name] = pd.to_datetime(df[column_name], format=format, errors='coerce')
            return df
        elif isinstance(df, PySparkDataFrame):
            # PySpark: usa to_date para conversão
            df = df.withColumn(column_name, to_date(col(column_name), format))
            return df
        else:
            raise TypeError("O objeto fornecido não é um DataFrame Pandas ou PySpark.")

    @staticmethod
    def filter_by_range(df, column_name, min_value, max_value):
        """
        Filtra valores de uma coluna dentro de um intervalo numérico determinado.

        Args:
            df (pd.DataFrame ou pyspark.sql.DataFrame): DataFrame de entrada.
            column_name (str): Nome da coluna para filtrar.
            min_value (float): Valor mínimo permitido.
            max_value (float): Valor máximo permitido.

        Returns:
            pd.DataFrame ou pyspark.sql.DataFrame: DataFrame filtrado.
        """
        if isinstance(df, pd.DataFrame):
            return df[(df[column_name] >= min_value) & (df[column_name] <= max_value)]
        elif isinstance(df, PySparkDataFrame):
            return df.filter((col(column_name) >= min_value) & (col(column_name) <= max_value))
        else:
            raise TypeError("O objeto fornecido não é um DataFrame Pandas ou PySpark.")

