# Databricks notebook source
# MAGIC %md
# MAGIC # 00 - API Utils
# MAGIC Modulo utilitario de ingestao da API da Camara dos Deputados usando PySpark nativo.
# MAGIC Persistencia em tabelas Delta no catalogo workspace do Databricks Free Tier.
# MAGIC
# MAGIC Funcoes disponiveis:
# MAGIC - fetch_to_spark: busca paginada de endpoints da API
# MAGIC - fetch_detail_to_spark: busca de detalhes por ID
# MAGIC - add_audit_cols: adiciona colunas de auditoria ao DataFrame
# MAGIC - save_bronze / save_prata / save_ouro: persistencia por camada
# MAGIC - read_bronze / read_prata / read_ouro: leitura por camada
# MAGIC - list_tables: lista todas as tabelas do projeto

# COMMAND ----------

# Carrega as configuracoes globais definidas em 00_config
# MAGIC %run ./00_config

# COMMAND ----------

# Importacoes necessarias para chamadas HTTP, controle de tempo e hash
import json, time, hashlib
from typing import List, Optional

# COMMAND ----------

# DBTITLE 1,Ingestao via Spark — fetch paginado

def _fetch_page(endpoint: str, pagina: int, params: dict = {}) -> list:
    """
    Busca uma unica pagina de um endpoint da API da Camara.
    Realiza ate 3 tentativas com espera exponencial em caso de falha.
    Retorna tupla (registros, links) onde links contem informacoes de paginacao HATEOAS.
    """
    import urllib.request, urllib.parse

    # Monta os parametros de paginacao e tamanho de pagina
    _params = {**params, "itens": PAGE_SIZE, "pagina": pagina}
    url     = f"{API_BASE_URL}/{endpoint}?" + urllib.parse.urlencode(_params)

    # Tentativas com backoff exponencial: 1s, 2s, 4s
    for attempt in range(3):
        try:
            with urllib.request.urlopen(url, timeout=30) as r:
                data = json.loads(r.read().decode())
            return data.get("dados", []), data.get("links", [])
        except Exception as e:
            if attempt == 2:
                # Esgotou as tentativas — retorna vazio sem lancar excecao
                print(f"  Falha permanente: {url} -- {e}")
                return [], []
            time.sleep(2 ** attempt)


def fetch_to_spark(endpoint: str, params: dict = {}, max_pages: int = None):
    """
    Busca todas as paginas de um endpoint da API e retorna um Spark DataFrame.
    Respeita o limite MAX_PAGES definido em 00_config para evitar timeout no Serverless.
    Utiliza os links HATEOAS retornados pela API para detectar se ha proxima pagina.
    """
    _max        = max_pages or MAX_PAGES
    all_records = []

    for pagina in range(1, _max + 1):
        records, links = _fetch_page(endpoint, pagina, params)

        # Para a paginacao se nao houver registros na pagina atual
        if not records:
            break

        all_records.extend(records)
        print(f"  Pagina {pagina} -- {len(records)} registros (total: {len(all_records)})")

        # Verifica se a API indica existencia de proxima pagina via link rel=next
        has_next = any(lk.get("rel") == "next" for lk in links)
        if not has_next:
            break

        # Avisa quando o limite de paginas configurado e atingido
        if pagina >= _max:
            print(f"  Limite de {_max} paginas atingido. Use carga incremental para continuar.")
            break

        # Throttle entre paginas para nao sobrecarregar a API publica
        time.sleep(0.3)

    if not all_records:
        raise ValueError(f"Nenhum registro retornado para: {endpoint}")

    # Converte a lista de dicionarios em Spark DataFrame
    df = spark.createDataFrame(all_records)
    return df


def fetch_detail_to_spark(endpoint: str, ids: list, id_col: str = "id"):
    """
    Busca detalhes individuais de recursos por ID e retorna um Spark DataFrame.
    Limita a quantidade de IDs processados ao MAX_IDS_DETALHE do config
    para evitar timeout no ambiente Serverless.
    """
    all_records = []
    _ids        = ids[:MAX_IDS_DETALHE]

    for idx, rid in enumerate(_ids):
        import urllib.request
        url = f"{API_BASE_URL}/{endpoint}/{rid}"

        # Tentativas com backoff exponencial para cada ID
        for attempt in range(3):
            try:
                with urllib.request.urlopen(url, timeout=30) as r:
                    data = json.loads(r.read().decode())
                rec = data.get("dados")
                if rec:
                    all_records.append(rec)
                break
            except Exception as e:
                if attempt == 2:
                    print(f"  ID {rid}: {e}")
                time.sleep(2 ** attempt)

        # Progresso a cada 50 IDs processados
        if (idx + 1) % 50 == 0:
            print(f"  {idx + 1}/{len(_ids)} detalhes buscados")

        # Throttle entre requisicoes individuais
        time.sleep(0.2)

    if not all_records:
        raise ValueError(f"Nenhum detalhe encontrado para: {endpoint}")

    return spark.createDataFrame(all_records)

# COMMAND ----------

# DBTITLE 1,Campos de auditoria

def add_audit_cols(df, endpoint: str):
    """
    Adiciona colunas de auditoria padrao a qualquer DataFrame antes de persistir.
    Colunas adicionadas:
    - _ingest_timestamp: data e hora exata da ingestao
    - _ingest_date: data da ingestao (util para particionar)
    - _source_endpoint: endpoint da API de origem dos dados
    """
    return (df
        .withColumn("_ingest_timestamp", F.current_timestamp())
        .withColumn("_ingest_date",      F.current_date())
        .withColumn("_source_endpoint",  F.lit(endpoint))
    )

# COMMAND ----------

# DBTITLE 1,Persistencia Delta — workspace catalog

# Nome do catalogo padrao do Databricks Free Tier
CATALOG = "workspace"

def _full_name(schema: str, table: str) -> str:
    """
    Retorna o nome completo da tabela no formato catalogo.schema.tabela.
    Garante que todas as operacoes usem o catalogo correto explicitamente.
    """
    return f"{CATALOG}.{schema}.{table}"

def table_exists(schema: str, table: str) -> bool:
    """Verifica se a tabela ja existe no catalogo workspace."""
    return spark.catalog.tableExists(_full_name(schema, table))

def save_delta(df, schema: str, table: str, merge_keys: List[str] = None, mode: str = "overwrite"):
    """
    Persiste um DataFrame como tabela Delta no catalogo workspace.

    Comportamento:
    - Se a tabela nao existe ou mode='overwrite': cria ou sobrescreve completamente.
    - Se merge_keys fornecidas: executa MERGE (upsert) usando as chaves informadas.
    - Caso contrario: executa append simples.

    O MERGE garante idempotencia: registros existentes sao atualizados,
    novos registros sao inseridos, sem duplicatas.
    """
    from delta.tables import DeltaTable

    full   = _full_name(schema, table)
    count  = df.count()
    exists = table_exists(schema, table)

    print(f"Salvando {full} -- {count} linhas")

    # Primeira carga ou sobrescrita total
    if not exists or mode == "overwrite":
        (df.write
           .format("delta")
           .mode("overwrite")
           .option("overwriteSchema", "true")
           .saveAsTable(full))
        print(f"  {'Criada' if not exists else 'Sobrescrita'}: {full}")
        return

    # Carga incremental via MERGE (upsert)
    if merge_keys:
        dt        = DeltaTable.forName(spark, full)
        on_clause = " AND ".join([f"t.{k} = s.{k}" for k in merge_keys])
        (dt.alias("t")
           .merge(df.alias("s"), on_clause)
           .whenMatchedUpdateAll()
           .whenNotMatchedInsertAll()
           .execute())
        print(f"  Upsert concluido: {full}")
    else:
        # Append sem controle de duplicatas
        (df.write
           .format("delta")
           .mode("append")
           .saveAsTable(full))
        print(f"  Append concluido: {full}")


def save_bronze(df, table: str, merge_keys: List[str] = None):
    """Persiste DataFrame na camada Bronze usando o schema configurado."""
    save_delta(df, SCHEMA_BRONZE, table, merge_keys=merge_keys)

def save_prata(df, table: str, merge_keys: List[str] = None, mode: str = "overwrite"):
    """Persiste DataFrame na camada Prata. Padrao overwrite pois e sempre reconstruida da Bronze."""
    save_delta(df, SCHEMA_PRATA, table, merge_keys=merge_keys, mode=mode)

def save_ouro(df, table: str):
    """
    Persiste DataFrame na camada Ouro.
    Sempre sobrescreve pois as tabelas Ouro sao os relatorios finais,
    reconstruidos a partir das camadas Bronze e Prata.
    """
    save_delta(df, SCHEMA_OURO, table, mode="overwrite")

# COMMAND ----------

# DBTITLE 1,Leitura de tabelas Delta — workspace catalog

def read_bronze(table: str):
    """Le tabela da camada Bronze diretamente do catalogo workspace."""
    return spark.table(_full_name(SCHEMA_BRONZE, table))

def read_prata(table: str):
    """Le tabela da camada Prata diretamente do catalogo workspace."""
    return spark.table(_full_name(SCHEMA_PRATA, table))

def read_ouro(table: str):
    """Le tabela da camada Ouro diretamente do catalogo workspace."""
    return spark.table(_full_name(SCHEMA_OURO, table))

# COMMAND ----------

# DBTITLE 1,Listar tabelas disponiveis

def list_tables():
    """
    Lista todas as tabelas Delta existentes nos tres schemas do projeto.
    Util para verificar o estado da carga apos execucao dos notebooks.
    """
    for schema in [SCHEMA_BRONZE, SCHEMA_PRATA, SCHEMA_OURO]:
        print(f"\n{CATALOG}.{schema}:")
        try:
            tabs = spark.sql(f"SHOW TABLES IN {CATALOG}.{schema}").collect()
            if tabs:
                for t in tabs:
                    print(f"  - {t['tableName']}")
            else:
                print(f"  Nenhuma tabela encontrada.")
        except Exception as e:
            print(f"  Erro ao listar: {e}")

# COMMAND ----------

print("API Utils carregado -- workspace catalog")
print(f"Catalogo: {CATALOG} | Schemas: {SCHEMA_BRONZE} | {SCHEMA_PRATA} | {SCHEMA_OURO}")
