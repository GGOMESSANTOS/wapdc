

# correct.py
import pandas as pd
import logging

def handle_error(error, context=""):
    # Log do erro e contexto (etapa do pipeline onde ocorreu)
    logging.error(f"Erro em {context}: {error}")

def retry_step(step_function, max_retries=3):
    '''
    Tenta executar uma função até um número máximo de tentativas.
    Args:
        step_function: função a ser executada
        max_retries: número máximo de tentativas
    Returns:
        Resultado da função ou None em caso de falha
    '''
    retries = 0
    while retries < max_retries:
        try:
            return step_function()
        except Exception as e:
            retries += 1
            handle_error(e, context=step_function.__name__)
    logging.error(f"Falha após {max_retries} tentativas.")
    return None


class DataQuality:
    def __init__(self, dataframe):
        self.df = dataframe

    def check_completeness(self, columns):
        """
        Verifica se as colunas especificadas estão completas (sem valores nulos).
        Args:
            columns: lista de colunas a serem verificadas
        Returns:
            bool: True se todas as colunas estiverem completas, False caso contrário
        """
        missing_data = self.df[columns].isnull().sum()
        incomplete_columns = missing_data[missing_data > 0]
        if not incomplete_columns.empty:
            logging.warning(f"Incompletude detectada nas colunas: {incomplete_columns.to_dict()}")
            return False
        return True

    def check_consistency(self, column, expected_values):
        """Verifica se os valores em uma coluna estão dentro do conjunto esperado."""
        inconsistent_values = self.df[~self.df[column].isin(expected_values)]
        if not inconsistent_values.empty:
            logging.warning(f"Inconsistência detectada na coluna '{column}'. Valores inesperados encontrados: {inconsistent_values[column].unique()}")
            return False
        return True

    def check_accuracy(self, column, min_value=None, max_value=None):
        """Verifica se os valores em uma coluna estão dentro de um intervalo aceitável."""
        if min_value is not None and any(self.df[column] < min_value):
            logging.warning(f"Imprecisão detectada na coluna '{column}': valores abaixo de {min_value}")
            return False
        if max_value is not None and any(self.df[column] > max_value):
            logging.warning(f"Imprecisão detectada na coluna '{column}': valores acima de {max_value}")
            return False
        return True

    def check_integrity(self, foreign_key, reference_df, reference_key):
        """Verifica se a chave estrangeira em 'foreign_key' possui correspondência na tabela de referência."""
        missing_references = self.df[~self.df[foreign_key].isin(reference_df[reference_key])]
        if not missing_references.empty:
            logging.warning(f"Integridade violada: valores de '{foreign_key}' sem correspondência em '{reference_key}'")
            return False
        return True

    def run_quality_checks(self, checks):
        """
        Executa uma lista de verificações de qualidade de dados.
        Args:
            checks: lista de dicionários contendo as verificações a serem realizadas
        Returns:
            dict: resultados das verificações
        """
        results = {}
        for check in checks:
            method = getattr(self, check['method'])
            result = method(*check['args'])
            results[check['method']] = result
        return results
