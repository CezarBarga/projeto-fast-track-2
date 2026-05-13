# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Bronze: Frentes Parlamentares
# MAGIC Ingestao via PySpark nativo com carga incremental.
# MAGIC
# MAGIC Etapas:
# MAGIC - Ingestao da lista de frentes parlamentares ativas
# MAGIC - Controle incremental: identifica frentes cujos membros ainda nao foram ingeridos
# MAGIC - Ingestao dos membros por frente em lotes para evitar timeout no Serverless
# MAGIC
# MAGIC Endpoints: GET /frentes | GET /frentes/{id}/membros

# COMMAND ----------

# Carrega configuracoes globais e funcoes utilitarias
# MAGIC %run ../utils/00_api_utils

# COMMAND ----------

# DBTITLE 1,Ingestao — Lista de Frentes

# Busca todas as frentes parlamentares cadastradas na Camara
print("Buscando frentes parlamentares...")

df_frentes = fetch_to_spark("frentes", max_pages=15)
df_frentes = add_audit_cols(df_frentes, "frentes")

# Persiste na Bronze com MERGE para garantir idempotencia
save_bronze(df_frentes, "frentes_lista", merge_keys=["id"])
print(f"{df_frentes.count()} frentes encontradas")

# COMMAND ----------

# DBTITLE 1,Controle Incremental — Frentes ja processadas

# Verifica quais frentes ja tiveram seus membros ingeridos em execucoes anteriores
# Considera validos apenas registros onde o campo id do deputado esta preenchido
try:
    df_existentes  = read_bronze("frentes_membros")
    ids_frentes_ok = set(
        df_existentes
        .filter(F.col("id") != "")
        .select("_frente_id")
        .distinct()
        .rdd.flatMap(lambda r: [str(r["_frente_id"])])
        .collect()
    )
    print(f"Frentes ja processadas com membros validos: {len(ids_frentes_ok)}")
except Exception:
    # Primeira execucao: tabela frentes_membros ainda nao existe
    ids_frentes_ok = set()
    print("Primeira carga -- nenhuma frente processada ainda")

# Filtra apenas as frentes que ainda nao foram processadas
ids_todas    = [r["id"] for r in df_frentes.select("id").collect()]
ids_faltando = [i for i in ids_todas if str(i) not in ids_frentes_ok]
print(f"Total frentes: {len(ids_todas)} | Faltando processar: {len(ids_faltando)}")

# COMMAND ----------

# DBTITLE 1,Ingestao — Membros por Frente (salvamento em lotes)

import urllib.request
from pyspark.sql.types import StructType, StructField, StringType

# Schema explicito para evitar erros de inferencia automatica de tipos
# Todos os campos sao String para compatibilidade com a resposta da API
schema_mb = StructType([
    StructField("_frente_id",    StringType(), True),
    StructField("id",            StringType(), True),
    StructField("nome",          StringType(), True),
    StructField("siglaPartido",  StringType(), True),
    StructField("siglaUf",       StringType(), True),
    StructField("idLegislatura", StringType(), True),
    StructField("urlFoto",       StringType(), True),
    StructField("email",         StringType(), True),
    StructField("titulo",        StringType(), True),
    StructField("codTitulo",     StringType(), True),
    StructField("dataInicio",    StringType(), True),
    StructField("dataFim",       StringType(), True),
])

# Tamanho do lote: a cada 50 frentes processadas os membros sao persistidos
# Evita acumulo excessivo de dados em memoria no ambiente Serverless
LOTE_SIZE    = 50
lote         = []
erros        = 0
total_salvo  = 0
primeira_vez = not table_exists(SCHEMA_BRONZE, "frentes_membros")

def salvar_lote_membros(lote, primeiro=False):
    """
    Persiste um lote de membros na tabela Bronze frentes_membros.
    Usa overwrite no primeiro lote (cria a tabela) e append nos subsequentes.
    Nao usa MERGE para evitar conflito de chave quando id do membro esta vazio.
    """
    if lote:
        df_l = spark.createDataFrame(lote, schema=schema_mb)
        df_l = add_audit_cols(df_l, "frentes/{id}/membros")
        mode = "overwrite" if primeiro else "append"
        save_delta(df_l, SCHEMA_BRONZE, "frentes_membros", mode=mode)
        print(f"  {len(lote)} membros salvos")

if not ids_faltando:
    print("Todas as frentes ja foram processadas.")
else:
    print(f"Processando {len(ids_faltando)} frentes...")
    primeiro_lote = primeira_vez

    for idx, frente_id in enumerate(ids_faltando):
        # Busca membros da frente via requisicao HTTP direta
        url = f"{API_BASE_URL}/frentes/{frente_id}/membros"
        try:
            with urllib.request.urlopen(url, timeout=30) as r:
                dados = json.loads(r.read().decode()).get("dados", [])

            # Monta tuplas com todos os campos do schema — garantindo string vazia para nulos
            for m in dados:
                lote.append((
                    str(frente_id),
                    str(m.get("id")            or ""),
                    str(m.get("nome")          or ""),
                    str(m.get("siglaPartido")  or ""),
                    str(m.get("siglaUf")       or ""),
                    str(m.get("idLegislatura") or ""),
                    str(m.get("urlFoto")       or ""),
                    str(m.get("email")         or ""),
                    str(m.get("titulo")        or ""),
                    str(m.get("codTitulo")     or ""),
                    str(m.get("dataInicio")    or ""),
                    str(m.get("dataFim")       or ""),
                ))
        except Exception as e:
            erros += 1

        # Persiste o lote ao atingir o tamanho configurado
        if (idx + 1) % LOTE_SIZE == 0:
            salvar_lote_membros(lote, primeiro=primeiro_lote)
            total_salvo  += len(lote)
            primeiro_lote = False
            lote          = []
            print(f"  {idx + 1}/{len(ids_faltando)} frentes processadas | {total_salvo} membros salvos")

        # Throttle entre requisicoes para nao sobrecarregar a API publica
        time.sleep(0.2)

    # Persiste o lote restante apos o ultimo ciclo
    salvar_lote_membros(lote, primeiro=primeiro_lote)
    total_salvo += len(lote)
    print(f"Total membros salvos nesta execucao: {total_salvo} | Erros: {erros}")

# COMMAND ----------

# DBTITLE 1,Validacao

# Verifica integridade dos dados: conta registros com e sem ID de deputado preenchido
df_check   = read_bronze("frentes_membros")
total      = df_check.count()
ids_ok     = df_check.filter(F.col("id") != "").count()
ids_vazios = df_check.filter(F.col("id") == "").count()

print(f"Bronze frentes_membros: {total} registros")
print(f"  IDs preenchidos: {ids_ok}")
print(f"  IDs vazios:      {ids_vazios}")
display(df_check.limit(5))
