# WAPDC - Write Audit Publish Data Contract

WAPDC é um framework robusto para construção de fábricas de pipeline de dados, oferecendo uma solução completa para processamento de dados de ponta a ponta, desde a ingestão até o armazenamento e consumo final. O framework é projetado para ser automatizado, escalável e replicável para diferentes fluxos de dados.

## Índice

1. [Visão Geral](#visão-geral)
2. [Estrutura](#estrutura)
3. [Instalação](#instalação)
4. [Uso](#uso)
5. [Configuração](#configuração)
6. [Monitoramento](#monitoramento)
7. [Testes](#testes)
8. [Contribuição](#contribuição)
9. [Licença](#licença)

## Visão Geral

WAPDC fornece uma estrutura modular para criar pipelines de dados eficientes e escaláveis. O framework é dividido em seis componentes principais, cada um responsável por uma parte específica do processo de dados.

## Estrutura

O framework WAPDC está dividido em 6 módulos principais:

1. **connect.py**: Conecta dados de diferentes fontes e os integra em uma estrutura comum (ex: camada landing no S3).

2. **collect.py**: Responsável pela extração dos dados de diferentes fontes (APIs, bases de dados, arquivos CSV, JSON, etc.). Retorna os dados em formato tabular, geralmente um DataFrame do Pandas ou PySpark.

3. **consume.py**: Responsável por salvar os dados nas fontes de destino.

4. **correct.py**: Realiza a aferição da qualidade dos dados através de producers de data quality.

5. **control.py**: Gerencia o controle dos pipelines, incluindo monitoramento e gerenciamento de configurações YAML.

6. **compose.py**: Realiza a limpeza e transformação dos dados coletados, preparando-os para análise.


