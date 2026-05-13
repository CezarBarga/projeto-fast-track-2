# Databricks notebook source
# MAGIC %md
# MAGIC # 06 - Bronze: Proposicoes, Tramitacoes e CPIs
# MAGIC Carga incremental por ID e hash via PySpark.
# MAGIC
# MAGIC Etapas:
# MAGIC - Controle incremental de proposicoes por ID
# MAGIC - Ingestao de tramitacoes por proposicao em lotes (base para SCD Type 2 na Prata)
# MAGIC - Ingestao de CPIs com eventos e membros em lotes
# MAGIC - Tabelas de eventos e membros de CPIs criadas mesmo sem dados disponivos na API
# MAGIC
# MAGIC Endpoints: /proposicoes | /proposicoes/{id}/tramitacoes | /orgaos

# COMMAND ----------

# Carrega configuracoes globais e funcoes utilitarias
# MAGIC %run ../utils/00_api_utils

# COMMAND ----------

# MAGIC %md ### Proposicoes

# COMMAND ----------

# DBTITLE 1,Controle Incremental — Proposicoes

# Busca o maior ID de proposicao ja ingerido para usar como ponto de corte
# Garante que apenas proposicoes novas sejam processadas em execucoes subsequentes
try:
    ultimo_id = int(
        read_bronze("proposicoes_lista")
        .agg(F.max(F.col("id").cast("long"))).collect()[0][0] or 0
    )
except Exception:
    # Primeira execucao: tabela ainda nao existe
    ultimo_id = 0

print(f"Ultimo ID ingerido: {ultimo_id}")

# COMMAND ----------

# DBTITLE 1,Ingestao — Proposicoes

# Busca proposicoes do ano configurado ordenadas por ID
df_prop_raw = fetch_to_spark("proposicoes", params={"ano": ANO_INICIO, "ordem": "ASC", "ordenarPor": "id"})

# Achata o campo statusProposicao (struct aninhado) em colunas escalares
# Esse campo contem o status atual da proposicao no processo legislativo
if "statusProposicao" in df_prop_raw.columns:
    df_prop_raw = df_prop_raw \
        .withColumn("status_sigla_orgao", F.col("statusProposicao")["siglaOrgao"]) \
        .withColumn("status_situacao",    F.col("statusProposicao")["descricaoSituacao"]) \
        .withColumn("status_data_hora",   F.col("statusProposicao")["dataHora"]) \
        .drop("statusProposicao")

# Filtra apenas proposicoes com ID maior que o ultimo ja ingerido
df_novas_prop = df_prop_raw.filter(F.col("id").cast("long") > F.lit(ultimo_id))
print(f"Novas proposicoes: {df_novas_prop.count()}")

if df_novas_prop.count() > 0:
    df_novas_prop = add_audit_cols(df_novas_prop, "proposicoes")
    save_bronze(df_novas_prop, "proposicoes_lista", merge_keys=["id"])

# COMMAND ----------

# DBTITLE 1,Ingestao — Tramitacoes (base para SCD2 na Prata)

import urllib.request
from pyspark.sql.types import StructType, StructField, StringType

# Limita a 150 proposicoes por execucao para evitar timeout no Serverless
# Em execucoes subsequentes, novas proposicoes serao processadas
ids_prop  = [r["id"] for r in df_novas_prop.select("id").limit(150).collect()]
erros     = 0
lote      = []

# Tamanho do lote: persiste a cada 30 proposicoes processadas
LOTE_SIZE = 30

# Schema explicito com todos os campos relevantes da tramitacao
schema_tram = StructType([
    StructField("_proposicao_id",      StringType(), True),
    StructField("_payload_hash",       StringType(), True),
    StructField("sequencia",           StringType(), True),
    StructField("dataHora",            StringType(), True),
    StructField("descricaoSituacao",   StringType(), True),
    StructField("descricaoTramitacao", StringType(), True),
    StructField("siglaOrgao",          StringType(), True),
    StructField("despacho",            StringType(), True),
])

def salvar_lote_tram(lote):
    """
    Persiste um lote de tramitacoes na Bronze.
    Usa hash MD5 do payload como chave para garantir idempotencia (base para CDC/SCD2).
    O payload e composto por: id da proposicao, data/hora e numero de sequencia.
    """
    if lote:
        df_l = spark.createDataFrame(lote, schema=schema_tram)
        df_l = add_audit_cols(df_l, "proposicoes/{id}/tramitacoes")
        save_bronze(df_l, "proposicoes_tramitacoes", merge_keys=["_payload_hash"])
        print(f"  {len(lote)} tramitacoes salvas")

print(f"Buscando tramitacoes de {len(ids_prop)} proposicoes...")

for idx, prop_id in enumerate(ids_prop):
    url = f"{API_BASE_URL}/proposicoes/{prop_id}/tramitacoes"
    try:
        with urllib.request.urlopen(url, timeout=30) as r:
            dados = json.loads(r.read().decode()).get("dados", [])

        for d in dados:
            # Gera hash do payload para identificar unicamente cada tramitacao
            payload = f"{prop_id}|{d.get('dataHora','')}|{d.get('sequencia','')}"
            lote.append((
                str(prop_id),
                hashlib.md5(payload.encode()).hexdigest(),
                str(d.get("sequencia")           or ""),
                str(d.get("dataHora")            or ""),
                str(d.get("descricaoSituacao")   or ""),
                str(d.get("descricaoTramitacao") or ""),
                str(d.get("siglaOrgao")          or ""),
                str(d.get("despacho")            or ""),
            ))
    except Exception as e:
        erros += 1
        print(f"  Proposicao {prop_id}: {e}")

    # Persiste o lote ao atingir o tamanho configurado
    if (idx + 1) % LOTE_SIZE == 0:
        salvar_lote_tram(lote)
        lote = []
        print(f"  {idx + 1}/{len(ids_prop)} proposicoes processadas")

    time.sleep(0.15)

# Persiste o lote restante apos o ultimo ciclo
salvar_lote_tram(lote)
print(f"Tramitacoes concluidas | Erros: {erros}")

# COMMAND ----------

# MAGIC %md ### CPIs

# COMMAND ----------

# DBTITLE 1,Ingestao — Lista de CPIs

import urllib.request
from pyspark.sql.types import StructType, StructField, StringType, LongType

# Busca orgaos com sigla contendo CPI (Comissao Parlamentar de Inquerito)
print("Buscando CPIs...")
df_cpis_raw = fetch_to_spark("orgaos", params={"sigla": "CPI"}, max_pages=5)
df_cpis     = df_cpis_raw.filter(F.upper(F.col("sigla")).contains("CPI"))
df_cpis     = add_audit_cols(df_cpis, "orgaos?sigla=CPI")
save_bronze(df_cpis, "cpis_lista", merge_keys=["id"])
print(f"{df_cpis.count()} CPIs encontradas")

ids_cpis = [r["id"] for r in df_cpis.select("id").collect()]

# COMMAND ----------

# DBTITLE 1,Eventos das CPIs — salvamento em lotes

# Schema explicito para eventos de CPI
schema_ev = StructType([
    StructField("_cpi_id",        StringType(), True),
    StructField("id",             StringType(), True),
    StructField("dataHoraInicio", StringType(), True),
    StructField("dataHoraFim",    StringType(), True),
    StructField("descricao",      StringType(), True),
    StructField("descricaoTipo",  StringType(), True),
    StructField("situacao",       StringType(), True),
    StructField("urlRegistro",    StringType(), True),
])

# Cria a tabela vazia com o schema correto antes de iniciar a ingestao
# Garante que a tabela sempre exista, mesmo que a API nao retorne eventos
df_ev_vazio = spark.createDataFrame([], schema_ev)
save_delta(df_ev_vazio, SCHEMA_BRONZE, "cpis_eventos", mode="overwrite")

lote_ev   = []
LOTE_SIZE = 20

def salvar_lote_ev(lote):
    """Persiste um lote de eventos de CPI na Bronze usando append."""
    if lote:
        df_l = spark.createDataFrame(lote, schema=schema_ev)
        df_l = add_audit_cols(df_l, "orgaos/{id}/eventos")
        save_delta(df_l, SCHEMA_BRONZE, "cpis_eventos", mode="append")
        print(f"  {len(lote)} eventos CPI salvos")

print(f"Buscando eventos de {len(ids_cpis)} CPIs...")

for idx, cpi_id in enumerate(ids_cpis):
    url = f"{API_BASE_URL}/orgaos/{cpi_id}/eventos"
    try:
        with urllib.request.urlopen(url, timeout=30) as r:
            dados = json.loads(r.read().decode()).get("dados", [])
        for e in dados:
            lote_ev.append((
                str(cpi_id),
                str(e.get("id")             or ""),
                str(e.get("dataHoraInicio") or ""),
                str(e.get("dataHoraFim")    or ""),
                str(e.get("descricao")      or ""),
                str(e.get("descricaoTipo")  or ""),
                str(e.get("situacao")       or ""),
                str(e.get("urlRegistro")    or ""),
            ))
    except Exception:
        pass

    if (idx + 1) % LOTE_SIZE == 0:
        salvar_lote_ev(lote_ev)
        lote_ev = []

    time.sleep(0.2)

# Persiste o lote restante
salvar_lote_ev(lote_ev)
total_ev = spark.table("workspace.bronze_camara.cpis_eventos").count()
print(f"Eventos CPI: {total_ev} registros")

# COMMAND ----------

# DBTITLE 1,Membros das CPIs — salvamento em lotes

# Schema explicito para membros de CPI
schema_mb = StructType([
    StructField("_cpi_id",      StringType(), True),
    StructField("id",           StringType(), True),
    StructField("nome",         StringType(), True),
    StructField("siglaPartido", StringType(), True),
    StructField("siglaUf",      StringType(), True),
    StructField("titulo",       StringType(), True),
    StructField("codTitulo",    StringType(), True),
])

# Cria a tabela vazia com o schema correto antes de iniciar a ingestao
# Garante que a tabela sempre exista, mesmo que a API nao retorne membros
df_mb_vazio = spark.createDataFrame([], schema_mb)
save_delta(df_mb_vazio, SCHEMA_BRONZE, "cpis_membros", mode="overwrite")

lote_mb = []

def salvar_lote_mb(lote):
    """Persiste um lote de membros de CPI na Bronze usando append."""
    if lote:
        df_l = spark.createDataFrame(lote, schema=schema_mb)
        df_l = add_audit_cols(df_l, "orgaos/{id}/membros")
        save_delta(df_l, SCHEMA_BRONZE, "cpis_membros", mode="append")
        print(f"  {len(lote)} membros CPI salvos")

print(f"Buscando membros de {len(ids_cpis)} CPIs...")

for idx, cpi_id in enumerate(ids_cpis):
    url = f"{API_BASE_URL}/orgaos/{cpi_id}/membros"
    try:
        with urllib.request.urlopen(url, timeout=30) as r:
            dados = json.loads(r.read().decode()).get("dados", [])
        for m in dados:
            lote_mb.append((
                str(cpi_id),
                str(m.get("id")           or ""),
                str(m.get("nome")         or ""),
                str(m.get("siglaPartido") or ""),
                str(m.get("siglaUf")      or ""),
                str(m.get("titulo")       or ""),
                str(m.get("codTitulo")    or ""),
            ))
    except Exception:
        pass

    if (idx + 1) % LOTE_SIZE == 0:
        salvar_lote_mb(lote_mb)
        lote_mb = []

    time.sleep(0.2)

# Persiste o lote restante
salvar_lote_mb(lote_mb)
total_mb = spark.table("workspace.bronze_camara.cpis_membros").count()
print(f"Membros CPI: {total_mb} registros")

# COMMAND ----------

# DBTITLE 1,Validacao Final

# Lista todas as tabelas do projeto para confirmar o estado da carga
list_tables()
