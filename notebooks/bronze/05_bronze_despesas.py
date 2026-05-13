# Databricks notebook source
# MAGIC %md
# MAGIC # 05 - Bronze: Despesas CEAP
# MAGIC Carga incremental por hash MD5 via PySpark.
# MAGIC
# MAGIC Etapas:
# MAGIC - Carrega IDs de todos os deputados ja ingeridos na Bronze
# MAGIC - Controle incremental via hash MD5: evita reingerir registros ja existentes
# MAGIC - Busca paginada das despesas por deputado e por ano
# MAGIC - Persistencia em lotes de 500 registros para evitar estouro de memoria no Serverless
# MAGIC
# MAGIC Decisao de projeto: a API de despesas nao possui ID nativo por registro.
# MAGIC O hash MD5 e gerado a partir de (deputado, cnpj, data, valor) como chave unica.
# MAGIC
# MAGIC Endpoint: GET /deputados/{id}/despesas

# COMMAND ----------

# Carrega configuracoes globais e funcoes utilitarias
# MAGIC %run ../utils/00_api_utils

# COMMAND ----------

# DBTITLE 1,IDs de Deputados

# Carrega os IDs de todos os deputados para iterar sobre cada um buscando suas despesas
ids_dep = [r["id"] for r in read_bronze("deputados_lista").select("id").collect()]
print(f"{len(ids_dep)} deputados para buscar despesas")

# COMMAND ----------

# DBTITLE 1,Hashes ja ingeridos (controle incremental)

# Carrega todos os hashes ja persistidos para evitar duplicatas na carga incremental
# Em execucoes subsequentes, apenas registros com hash novo serao ingeridos
try:
    hashes_ok = set(
        read_bronze("despesas_ceap")
        .select("_record_hash")
        .rdd.flatMap(lambda r: [r[0]])
        .collect()
    )
    print(f"{len(hashes_ok)} registros existentes (serao ignorados nesta carga)")
except Exception:
    # Primeira execucao: tabela ainda nao existe
    hashes_ok = set()

# COMMAND ----------

# DBTITLE 1,Ingestao incremental — Despesas por Deputado/Ano

import urllib.request, urllib.parse
from pyspark.sql.types import StructType, StructField, StringType

# Schema explicito para todos os campos de despesa
# Todos os valores sao tratados como String para evitar erros de inferencia de tipo
schema_desp = StructType([
    StructField("_record_hash",      StringType(), True),
    StructField("_deputado_id",      StringType(), True),
    StructField("_ano",              StringType(), True),
    StructField("mes",               StringType(), True),
    StructField("tipoDespesa",       StringType(), True),
    StructField("cnpjCpfFornecedor", StringType(), True),
    StructField("nomeFornecedor",    StringType(), True),
    StructField("dataDocumento",     StringType(), True),
    StructField("numDocumento",      StringType(), True),
    StructField("valorBruto",        StringType(), True),
    StructField("valorLiquido",      StringType(), True),
    StructField("valorGlosa",        StringType(), True),
    StructField("urlDocumento",      StringType(), True),
])

erros       = []
lote_rows   = []

# Tamanho do lote: a cada 500 registros novos os dados sao persistidos na Bronze
# Evita acumulo excessivo de dados em memoria no ambiente Serverless
LOTE_SIZE   = 500
total_salvos = 0

def salvar_lote_despesas(rows):
    """
    Persiste um lote de despesas na tabela Bronze despesas_ceap.
    Usa schema explicito e tuplas para garantir compatibilidade de tipos.
    O MERGE usa o hash MD5 como chave para garantir idempotencia.
    """
    if rows:
        df_l = spark.createDataFrame(rows, schema=schema_desp)
        df_l = add_audit_cols(df_l, "deputados/{id}/despesas")
        save_bronze(df_l, "despesas_ceap", merge_keys=["_record_hash"])
        print(f"  {len(rows)} registros salvos")

for idx_dep, dep_id in enumerate(ids_dep):
    for ano in range(ANO_INICIO, ANO_FIM + 1):
        pagina = 1

        # Limita a 5 paginas por deputado/ano para evitar timeout no Serverless
        while pagina <= 5:
            params = {
                "ano":       ano,
                "itens":     PAGE_SIZE,
                "pagina":    pagina,
                "ordem":     "ASC",
                "ordenarPor":"mes"
            }
            url = f"{API_BASE_URL}/deputados/{dep_id}/despesas?" + urllib.parse.urlencode(params)

            try:
                with urllib.request.urlopen(url, timeout=30) as r:
                    data  = json.loads(r.read().decode())
                    dados = data.get("dados", [])
                    links = data.get("links", [])

                if not dados:
                    break

                for rec in dados:
                    # Gera hash MD5 como chave unica do registro
                    # Composto por: id do deputado, cnpj/cpf do fornecedor, data e valor liquido
                    chave = f"{dep_id}|{rec.get('cnpjCpfFornecedor','')}|{rec.get('dataDocumento','')}|{rec.get('valorLiquido','')}"
                    h     = hashlib.md5(chave.encode()).hexdigest()

                    # Inclui apenas registros com hash novo (nao ingeridos anteriormente)
                    if h not in hashes_ok:
                        lote_rows.append((
                            h,
                            str(dep_id),
                            str(ano),
                            str(rec.get("mes")               or ""),
                            str(rec.get("tipoDespesa")       or ""),
                            str(rec.get("cnpjCpfFornecedor") or ""),
                            str(rec.get("nomeFornecedor")    or ""),
                            str(rec.get("dataDocumento")     or ""),
                            str(rec.get("numDocumento")      or ""),
                            str(rec.get("valorBruto")        or ""),
                            str(rec.get("valorLiquido")      or ""),
                            str(rec.get("valorGlosa")        or ""),
                            str(rec.get("urlDocumento")      or ""),
                        ))

                # Persiste o lote quando atinge o tamanho configurado
                if len(lote_rows) >= LOTE_SIZE:
                    salvar_lote_despesas(lote_rows)
                    total_salvos += len(lote_rows)
                    lote_rows     = []

                # Verifica se ha proxima pagina via link HATEOAS
                has_next = any(lk.get("rel") == "next" for lk in links)
                if not has_next:
                    break
                pagina += 1
                time.sleep(0.2)

            except Exception as e:
                erros.append({"dep": dep_id, "ano": ano, "erro": str(e)})
                break

        # Throttle entre deputados para nao sobrecarregar a API publica
        time.sleep(0.1)

    # Exibe progresso a cada 50 deputados processados
    if (idx_dep + 1) % 50 == 0:
        print(f"  {idx_dep + 1}/{len(ids_dep)} deputados processados")

# Persiste o lote restante apos o ultimo ciclo
salvar_lote_despesas(lote_rows)
total_salvos += len(lote_rows)
print(f"Total salvo: {total_salvos} | Erros: {len(erros)}")

# COMMAND ----------

# DBTITLE 1,Validacao

# Confirma o total de registros persistidos na camada Bronze
total = spark.table("workspace.bronze_camara.despesas_ceap").count()
print(f"Total Bronze despesas_ceap: {total}")
