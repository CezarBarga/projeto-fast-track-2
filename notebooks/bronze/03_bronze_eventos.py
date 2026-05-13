# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - Bronze: Eventos Legislativos
# MAGIC Carga incremental por data via PySpark.
# MAGIC
# MAGIC Etapas:
# MAGIC - Controle incremental: busca apenas eventos posteriores ao ultimo ja ingerido
# MAGIC - Achatamento dos campos struct orgaos e localCamara retornados pela API
# MAGIC - Ingestao da presenca de deputados nos eventos mais recentes
# MAGIC
# MAGIC Endpoint: GET /eventos | GET /eventos/{id}/deputados

# COMMAND ----------

# Carrega configuracoes globais e funcoes utilitarias
# MAGIC %run ../utils/00_api_utils

# COMMAND ----------

# DBTITLE 1,Controle Incremental por Data

# Busca a data maxima ja ingerida para determinar o ponto de partida da carga
# Se a tabela nao existir (primeira execucao), inicia do ANO_INICIO configurado
try:
    max_dt      = read_bronze("eventos_lista") \
                    .agg(F.max("dataHoraInicio")).collect()[0][0]
    data_inicio = str(max_dt)[:10] if max_dt else f"{ANO_INICIO}-01-01"
except Exception:
    data_inicio = f"{ANO_INICIO}-01-01"

data_fim = datetime.now().strftime("%Y-%m-%d")
print(f"Periodo incremental: {data_inicio} ate {data_fim}")

# COMMAND ----------

# DBTITLE 1,Ingestao — Eventos

# Busca eventos legislativos no periodo determinado pelo controle incremental
df_ev = fetch_to_spark("eventos", params={
    "dataInicio": data_inicio,
    "dataFim":    data_fim,
    "ordem":      "ASC",
    "ordenarPor": "dataHoraInicio",
})

# Achata o campo orgaos (array de structs) extraindo apenas o primeiro orgao
# A API retorna um array de orgaos por evento; na pratica ha sempre um principal
if "orgaos" in df_ev.columns:
    df_ev = df_ev \
        .withColumn("orgao_id",      F.col("orgaos")[0]["id"].cast("long")) \
        .withColumn("orgao_nome",    F.col("orgaos")[0]["nome"]) \
        .withColumn("orgao_sigla",   F.col("orgaos")[0]["sigla"]) \
        .withColumn("orgao_apelido", F.col("orgaos")[0]["apelido"]) \
        .drop("orgaos")

# Achata o campo localCamara (struct) extraindo o nome do local
if "localCamara" in df_ev.columns:
    df_ev = df_ev \
        .withColumn("local_nome", F.col("localCamara")["nome"]) \
        .drop("localCamara")

df_ev = add_audit_cols(df_ev, "eventos")
save_bronze(df_ev, "eventos_lista", merge_keys=["id"])
print(f"{df_ev.count()} eventos ingeridos")

# COMMAND ----------

# DBTITLE 1,Ingestao — Presenca de Deputados

# Busca a presenca de deputados nos 300 eventos mais recentes
# Limitado para evitar timeout no Serverless — eventos mais antigos ja foram processados
ids_ev       = [r["id"] for r in df_ev.orderBy(F.desc("dataHoraInicio")).limit(300).collect()]
dfs_presenca = []

print(f"Buscando presenca em {len(ids_ev)} eventos...")

for ev_id in ids_ev:
    import urllib.request
    url = f"{API_BASE_URL}/eventos/{ev_id}/deputados"
    try:
        with urllib.request.urlopen(url, timeout=30) as r:
            dados = json.loads(r.read().decode()).get("dados", [])
        if dados:
            # Adiciona o ID do evento como chave de relacionamento
            df_p = spark.createDataFrame(dados) \
                        .withColumn("_evento_id", F.lit(ev_id))
            dfs_presenca.append(df_p)
    except Exception:
        pass

    # Throttle entre requisicoes para nao sobrecarregar a API publica
    time.sleep(0.15)

if dfs_presenca:
    # Empilha todos os DataFrames de presenca via unionByName
    # allowMissingColumns=True tolera schemas ligeiramente diferentes entre eventos
    from functools import reduce
    df_pres = reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), dfs_presenca)
    df_pres = add_audit_cols(df_pres, "eventos/{id}/deputados")
    save_bronze(df_pres, "eventos_presenca", merge_keys=["_evento_id", "id"])
    print(f"{df_pres.count()} registros de presenca salvos")

# COMMAND ----------

# Exibe amostra dos eventos ingeridos para validacao visual
display(read_bronze("eventos_lista").limit(5))
