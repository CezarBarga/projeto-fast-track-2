# Databricks notebook source
# MAGIC %md
# MAGIC # 00 - Configuracoes Globais
# MAGIC Ambiente: Databricks Free Tier -- Serverless Warehouse
# MAGIC
# MAGIC Este notebook define todos os parametros globais utilizados pelos demais notebooks da solucao.
# MAGIC Deve ser executado primeiro, antes de qualquer outro notebook do projeto.
# MAGIC Execute com Run All.

# COMMAND ----------

# Importacoes de modulos PySpark e bibliotecas Python utilizadas em toda a solucao
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql import types as T
import hashlib
from datetime import datetime

# COMMAND ----------

# DBTITLE 1,Parametros Globais

# URL base da API publica de Dados Abertos da Camara dos Deputados
API_BASE_URL = "https://dadosabertos.camara.leg.br/api/v2"

# Nomes dos schemas Delta Lake por camada da arquitetura medalha
# Bronze: dados brutos ingeridos da API sem transformacao
# Prata: dados normalizados, tipados e modelados dimensionalmente
# Ouro: tabelas analiticas finais — os relatorios entregaveis do projeto
SCHEMA_BRONZE = "bronze_camara"
SCHEMA_PRATA  = "prata_camara"
SCHEMA_OURO   = "ouro_camara"

# Tamanho de pagina para chamadas paginadas a API (maximo permitido pela API da Camara)
PAGE_SIZE = 100

# Limite de paginas por execucao — evita timeout no ambiente Serverless (2h max)
# Para conjuntos maiores, usar carga incremental em execucoes subsequentes
MAX_PAGES = 8

# Limite de IDs buscados individualmente por execucao (endpoints de detalhe por ID)
MAX_IDS_DETALHE = 150

# Periodo de ingestao dos dados — ajuste conforme necessidade
ANO_INICIO = 2024
ANO_FIM    = 2025

# Legislatura atual da Camara dos Deputados (57a: 2023-2027)
LEGISLATURA_ATUAL = 57

# Threshold do Z-Score para classificacao de anomalias nas despesas CEAP
# Valores com |z-score| acima deste limite sao sinalizados como anomalia
ZSCORE_THRESHOLD = 3.0

# COMMAND ----------

# DBTITLE 1,Inicializar Schemas

# Cria os schemas no catalogo workspace caso ainda nao existam (operacao idempotente)
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_BRONZE}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_PRATA}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_OURO}")

print(f"Schemas prontos: {SCHEMA_BRONZE} | {SCHEMA_PRATA} | {SCHEMA_OURO}")
print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} -- Periodo configurado: {ANO_INICIO} a {ANO_FIM}")
