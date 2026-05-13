# Databricks notebook source
# MAGIC %md
# MAGIC # 04 - Bronze: Votacoes e Votos
# MAGIC Carga incremental por ID via PySpark.
# MAGIC
# MAGIC Etapas:
# MAGIC - Controle incremental: busca apenas votacoes com ID maior que o ultimo ingerido
# MAGIC - Achatamento do campo struct proposicao_ retornado pela API
# MAGIC - Ingestao dos votos individuais de cada deputado por votacao
# MAGIC - Achatamento do campo struct deputado_ nos registros de voto
# MAGIC
# MAGIC Endpoints: GET /votacoes | GET /votacoes/{id}/votos

# COMMAND ----------

# Carrega configuracoes globais e funcoes utilitarias
# MAGIC %run ../utils/00_api_utils

# COMMAND ----------

# DBTITLE 1,Controle Incremental por ID

# Busca o maior ID ja ingerido para usar como ponto de corte da carga incremental
# O ID das votacoes e uma string composta (ex: 1234567-89) e nao um inteiro simples
try:
    ultimo_id = read_bronze("votacoes_lista") \
                    .agg(F.max(F.col("id").cast("long"))).collect()[0][0] or 0
    ultimo_id = int(ultimo_id)
except Exception:
    # Primeira execucao: tabela ainda nao existe
    ultimo_id = 0

print(f"Ultimo ID ingerido: {ultimo_id}")

# COMMAND ----------

# DBTITLE 1,Ingestao — Votacoes

# Busca todas as votacoes ordenadas por ID para facilitar o controle incremental
df_vot_raw = fetch_to_spark("votacoes", params={"ordem": "ASC", "ordenarPor": "id"})

# Achata o campo proposicao_ (struct aninhado) em colunas escalares
# Esse campo contem informacoes sobre a proposicao votada
if "proposicao_" in df_vot_raw.columns:
    df_vot_raw = df_vot_raw \
        .withColumn("prop_id",     F.col("proposicao_")["id"].cast("long")) \
        .withColumn("prop_sigla",  F.col("proposicao_")["siglaTipo"]) \
        .withColumn("prop_numero", F.col("proposicao_")["numero"]) \
        .withColumn("prop_ano",    F.col("proposicao_")["ano"]) \
        .withColumn("prop_ementa", F.col("proposicao_")["ementa"]) \
        .drop("proposicao_")

# Filtra apenas as votacoes com ID maior que o ultimo ja ingerido
# Nota: o ID pode conter hifen (ex: 1234567-89), por isso o cast para long pode retornar null
# Nesse caso, o filtro usa anti-join por string no proximo passo
try:
    ids_existentes = set(
        read_bronze("votacoes_lista")
        .select("id")
        .rdd.flatMap(lambda r: [r[0]])
        .collect()
    )
except Exception:
    ids_existentes = set()

df_novas = df_vot_raw.filter(~F.col("id").isin(ids_existentes))
print(f"Novas votacoes: {df_novas.count()}")

if df_novas.count() > 0:
    df_novas = add_audit_cols(df_novas, "votacoes")
    save_bronze(df_novas, "votacoes_lista", merge_keys=["id"])

# COMMAND ----------

# DBTITLE 1,Ingestao — Votos por Votacao

# Para cada votacao nova, busca os votos individuais de cada deputado
ids_vot   = [r["id"] for r in df_novas.select("id").collect()]
dfs_votos = []

print(f"Buscando votos de {len(ids_vot)} votacoes...")

for vot_id in ids_vot:
    import urllib.request
    url = f"{API_BASE_URL}/votacoes/{vot_id}/votos"
    try:
        with urllib.request.urlopen(url, timeout=30) as r:
            dados = json.loads(r.read().decode()).get("dados", [])
        if dados:
            df_v = spark.createDataFrame(dados) \
                        .withColumn("_votacao_id", F.lit(vot_id))

            # Achata o campo deputado_ (struct) em colunas escalares
            # Esse campo contem identificacao e partido do deputado que votou
            if "deputado_" in df_v.columns:
                df_v = df_v \
                    .withColumn("id_deputado",   F.col("deputado_")["id"].cast("long")) \
                    .withColumn("nome_deputado", F.col("deputado_")["nome"]) \
                    .withColumn("sigla_partido", F.col("deputado_")["siglaPartido"]) \
                    .withColumn("sigla_uf",      F.col("deputado_")["siglaUf"]) \
                    .drop("deputado_")
            dfs_votos.append(df_v)
    except Exception as e:
        print(f"  Votacao {vot_id}: {e}")

    # Throttle entre requisicoes para nao sobrecarregar a API publica
    time.sleep(0.15)

if dfs_votos:
    # Empilha todos os DataFrames de votos via unionByName
    # allowMissingColumns=True tolera schemas ligeiramente diferentes entre votacoes
    from functools import reduce
    df_votos = reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), dfs_votos)
    df_votos = add_audit_cols(df_votos, "votacoes/{id}/votos")
    save_bronze(df_votos, "votacoes_votos", merge_keys=["_votacao_id", "id_deputado"])
    print(f"{df_votos.count()} votos salvos")

# COMMAND ----------

# Exibe amostra das votacoes ingeridas para validacao visual
display(read_bronze("votacoes_lista").limit(5))
