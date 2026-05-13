# Databricks notebook source
# MAGIC %md
# MAGIC # 11 - Prata: dim_deputado, dim_frente, fato_frente_membro
# MAGIC Normalizacao com PySpark nativo. Deduplicacao via Window Functions.
# MAGIC
# MAGIC Etapas:
# MAGIC - Criacao de dim_deputado a partir da Bronze deputados_lista
# MAGIC - Criacao de dim_frente a partir da Bronze frentes_lista
# MAGIC - Criacao de fato_frente_membro a partir da Bronze frentes_membros
# MAGIC   Salvamento em lotes de 200 frentes para evitar timeout no Serverless
# MAGIC
# MAGIC Decisao de projeto: campos de partido e UF sao detectados dinamicamente
# MAGIC pois podem variar de nome dependendo da versao da API ingerida.

# COMMAND ----------

# Carrega configuracoes globais e funcoes utilitarias
# MAGIC %run ../utils/00_api_utils

# COMMAND ----------

# MAGIC %md ## dim_deputado

# COMMAND ----------

# DBTITLE 1,Limpeza e normalizacao — dim_deputado

df_lista = read_bronze("deputados_lista")

# Detecta dinamicamente os nomes das colunas pois podem variar entre versoes da API
col_nome    = "nome"          if "nome"          in df_lista.columns else "nomeEleitoral"
col_partido = "siglaPartido"  if "siglaPartido"  in df_lista.columns else "sigla_partido"
col_uf      = "siglaUf"       if "siglaUf"       in df_lista.columns else "sigla_uf"
col_leg     = "idLegislatura" if "idLegislatura" in df_lista.columns else "id_legislatura"

df_dim_dep = (
    df_lista
    .select(
        F.col("id").cast("long").alias("id_deputado"),
        F.trim(F.col(col_nome)).alias("nome"),
        # Normaliza sigla do partido em maiusculas para padronizacao
        F.upper(F.trim(F.col(col_partido))).alias("sigla_partido"),
        # Normaliza sigla da UF em maiusculas para padronizacao
        F.upper(F.trim(F.col(col_uf))).alias("sigla_uf"),
        F.col(col_leg).cast("int").alias("id_legislatura"),
        # Campos opcionais: retorna null se nao existirem na tabela Bronze
        F.col("urlFoto").alias("url_foto") if "urlFoto" in df_lista.columns else F.lit(None).alias("url_foto"),
        F.col("email") if "email" in df_lista.columns else F.lit(None).alias("email"),
        F.col("_ingest_timestamp"),
    )
    # Deduplicacao: mantém apenas o registro mais recente por deputado
    # Necessario pois a Bronze pode conter multiplas versoes do mesmo registro
    .withColumn("_rn", F.row_number().over(
        Window.partitionBy("id_deputado").orderBy(F.desc("_ingest_timestamp"))
    ))
    .filter(F.col("_rn") == 1).drop("_rn")
    # Flag que indica se o deputado esta na legislatura atual configurada
    .withColumn("is_ativo",    F.col("id_legislatura") == F.lit(LEGISLATURA_ATUAL))
    .withColumn("_updated_at", F.current_timestamp())
)

# Validacao de qualidade: verifica registros sem partido ou UF
total       = df_dim_dep.count()
sem_partido = df_dim_dep.filter(F.col("sigla_partido").isNull()).count()
sem_uf      = df_dim_dep.filter(F.col("sigla_uf").isNull()).count()
print(f"dim_deputado: {total} registros | Sem partido: {sem_partido} | Sem UF: {sem_uf}")

save_prata(df_dim_dep, "dim_deputado")

# COMMAND ----------

# MAGIC %md ## dim_frente

# COMMAND ----------

# DBTITLE 1,dim_frente

df_fr = read_bronze("frentes_lista")

df_dim_frente = (
    df_fr
    .select(
        F.col("id").cast("long").alias("id_frente"),
        F.trim(F.col("titulo")).alias("titulo"),
        F.col("idLegislatura").cast("int").alias("id_legislatura"),
        # Campo opcional: retorna null se nao existir na tabela Bronze
        F.col("urlWebsite").alias("url_website") if "urlWebsite" in df_fr.columns else F.lit(None).alias("url_website"),
        F.col("_ingest_timestamp"),
    )
    # Deduplicacao: mantém apenas o registro mais recente por frente
    .withColumn("_rn", F.row_number().over(
        Window.partitionBy("id_frente").orderBy(F.desc("_ingest_timestamp"))
    ))
    .filter(F.col("_rn") == 1).drop("_rn")
    # Resumo do tema da frente extraido dos primeiros 60 caracteres do titulo
    .withColumn("tema_resumo", F.substring("titulo", 1, 60))
    .withColumn("_updated_at", F.current_timestamp())
)

save_prata(df_dim_frente, "dim_frente")
print(f"dim_frente: {df_dim_frente.count()} registros")

# COMMAND ----------

# MAGIC %md ## fato_frente_membro

# COMMAND ----------

# DBTITLE 1,fato_frente_membro — salvamento em lotes

df_mb = read_bronze("frentes_membros")

# Detecta dinamicamente os nomes das colunas de partido, UF e titulo
col_partido_mb = "siglaPartido" if "siglaPartido" in df_mb.columns else "sigla_partido"
col_uf_mb      = "siglaUf"      if "siglaUf"      in df_mb.columns else "sigla_uf"
col_titulo_mb  = "titulo"       if "titulo"       in df_mb.columns else "titulo_membro"

# Prepara o DataFrame completo com tipagem e normalizacao
# try_cast e usado nos IDs para tolerar valores vazios ou malformados vindos da Bronze
df_fato_mb = (
    df_mb
    .select(
        F.expr("try_cast(_frente_id as long)").alias("id_frente"),
        F.expr("try_cast(id as long)").alias("id_deputado"),
        F.trim(F.col("nome")).alias("nome_deputado"),
        F.upper(F.trim(F.col(col_partido_mb))).alias("sigla_partido"),
        F.upper(F.trim(F.col(col_uf_mb))).alias("sigla_uf"),
        F.trim(F.col(col_titulo_mb)).alias("titulo_membro"),
        F.col("_ingest_timestamp"),
    )
    # Remove registros com IDs nulos (valores vazios que viraram null apos try_cast)
    .filter(F.col("id_frente").isNotNull() & F.col("id_deputado").isNotNull())
    # Remove duplicatas mantendo uma linha por combinacao frente/deputado
    .dropDuplicates(["id_frente", "id_deputado"])
    .withColumn("_updated_at", F.current_timestamp())
)

total_registros = df_fato_mb.count()
print(f"Total fato_frente_membro: {total_registros} registros")

# Salva em lotes de 200 frentes por vez para evitar timeout no Serverless
# O primeiro lote usa overwrite (cria a tabela), os subsequentes usam append
LOTE_SIZE   = 200
ids_frentes = [r["id_frente"] for r in df_fato_mb.select("id_frente").distinct().collect()]
print(f"Total frentes a processar: {len(ids_frentes)}")

total_salvo = 0
for i in range(0, len(ids_frentes), LOTE_SIZE):
    lote_ids = ids_frentes[i:i + LOTE_SIZE]
    df_lote  = df_fato_mb.filter(F.col("id_frente").isin(lote_ids))
    save_prata(
        df_lote,
        "fato_frente_membro",
        merge_keys=["id_frente", "id_deputado"],
        mode="append" if i > 0 else "overwrite"
    )
    total_salvo += df_lote.count()
    print(f"  Lote {i//LOTE_SIZE + 1} -- {total_salvo}/{total_registros} registros salvos")

print(f"fato_frente_membro: {total_salvo} registros")

# Exibe amostra da dim_deputado para validacao visual
display(df_dim_dep.limit(5))
