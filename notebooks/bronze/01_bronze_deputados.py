# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Bronze: Deputados
# MAGIC Ingestao via PySpark nativo com carga incremental por ID.
# MAGIC
# MAGIC Etapas:
# MAGIC - Ingestao da lista completa de deputados via endpoint paginado
# MAGIC - Carga incremental dos detalhes individuais por deputado
# MAGIC - Achatamento do campo struct ultimoStatus retornado pela API
# MAGIC
# MAGIC Endpoint: GET /deputados

# COMMAND ----------

# Carrega configuracoes globais e funcoes utilitarias
# MAGIC %run ../utils/00_api_utils

# COMMAND ----------

# DBTITLE 1,Ingestao — Lista de Deputados

# Busca a lista completa de deputados ordenada por nome
# O parametro max_pages=15 garante cobertura de todos os deputados ativos
print("Buscando lista de deputados...")

df_lista = fetch_to_spark(
    "deputados",
    params={"ordem": "ASC", "ordenarPor": "nome"},
    max_pages=15
)

# Adiciona colunas de auditoria antes de persistir
df_lista = add_audit_cols(df_lista, "deputados")

# Persiste na Bronze usando MERGE com chave id para garantir idempotencia
save_bronze(df_lista, "deputados_lista", merge_keys=["id"])

# COMMAND ----------

# DBTITLE 1,Carga Incremental — Detalhes por Deputado

# Controle incremental: identifica quais IDs ja foram detalhados em execucoes anteriores
# Evita rebuscar detalhes de deputados ja existentes na tabela
try:
    ids_existentes = set(
        read_bronze("deputados_detalhes")
        .select(F.col("id").cast("string"))
        .rdd.flatMap(lambda r: [r[0]])
        .collect()
    )
except Exception:
    # Primeira execucao: tabela ainda nao existe
    ids_existentes = set()

# Determina quais IDs ainda precisam ser buscados individualmente
ids_todos = [str(r["id"]) for r in df_lista.select("id").collect()]
ids_novos = [i for i in ids_todos if i not in ids_existentes]

print(f"Total: {len(ids_todos)} | Ja detalhados: {len(ids_existentes)} | Novos: {len(ids_novos)}")

if ids_novos:
    # Busca detalhes individuais para cada ID novo
    df_det = fetch_detail_to_spark("deputados", ids_novos)

    # Seleciona apenas colunas escalares basicas presentes no retorno
    # O endpoint de detalhe pode retornar campos variados dependendo da legislatura
    cols_base = ["id", "cpf", "nomeCivil", "sexo", "urlWebsite"]
    cols_pres = [c for c in cols_base if c in df_det.columns]
    df_det_flat = df_det.select(*cols_pres)

    # Achata o campo struct ultimoStatus em colunas escalares individuais
    # Esse campo contem informacoes do mandato mais recente do deputado
    if "ultimoStatus" in df_det.columns:
        df_det_flat = df_det_flat \
            .withColumn("nome_urna",      F.col("ultimoStatus.nomeEleitoral")) \
            .withColumn("sigla_partido",  F.col("ultimoStatus.siglaPartido")) \
            .withColumn("sigla_uf",       F.col("ultimoStatus.siglaUf")) \
            .withColumn("id_legislatura", F.col("ultimoStatus.idLegislatura")) \
            .withColumn("situacao",       F.col("ultimoStatus.situacao")) \
            .withColumn("email",          F.col("ultimoStatus.email")) \
            .withColumn("url_foto",       F.col("ultimoStatus.urlFoto")) \
            .drop("ultimoStatus")

    # Adiciona auditoria e persiste na Bronze
    df_det_flat = add_audit_cols(df_det_flat, "deputados/{id}")
    save_bronze(df_det_flat, "deputados_detalhes", merge_keys=["id"])
    print(f"{df_det_flat.count()} detalhes salvos")
else:
    print("Nenhum deputado novo para detalhar.")

# COMMAND ----------

# DBTITLE 1,Validacao

# Confirma o total de registros persistidos na camada Bronze
print(f"Bronze deputados_lista: {read_bronze('deputados_lista').count()} registros")
display(read_bronze("deputados_lista").limit(5))
