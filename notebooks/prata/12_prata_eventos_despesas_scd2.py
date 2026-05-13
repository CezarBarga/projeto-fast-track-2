# Databricks notebook source
# MAGIC %md
# MAGIC # 12 - Prata: Eventos, Despesas, Votacoes e Proposicoes SCD2
# MAGIC PySpark nativo -- Window Functions, tipagem e SCD Type 2.
# MAGIC
# MAGIC Etapas:
# MAGIC - Eventos: criacao de dim_orgao, dim_data (calendario analitico) e fato_eventos
# MAGIC - Despesas: criacao de dim_fornecedor, dim_categoria_despesa e fato_despesas
# MAGIC - Proposicoes: implementacao de SCD Type 2 para rastreamento historico de tramitacoes
# MAGIC
# MAGIC Decisao de projeto: SCD Type 2 permite reconstruir o estado de qualquer PL
# MAGIC em qualquer data historica via campos valid_from, valid_to e is_current.
# MAGIC Complementado pelo Delta Time Travel para granularidade adicional.

# COMMAND ----------

# Carrega configuracoes globais e funcoes utilitarias
# MAGIC %run ../utils/00_api_utils

# COMMAND ----------

# MAGIC %md ## Eventos — dim_orgao, dim_data, fato_eventos

# COMMAND ----------

# Carrega a tabela Bronze de eventos como base para as tres tabelas da Prata
df_ev = read_bronze("eventos_lista")

# dim_orgao: dimensao de orgaos legislativos extraida dos eventos
# Cada evento esta associado a um orgao principal (comissao, plenario, etc.)
df_dim_orgao = (
    df_ev
    .select(
        F.col("orgao_id").cast("long").alias("id_orgao"),
        F.trim(F.col("orgao_nome")).alias("nome_orgao"),
        F.trim(F.col("orgao_sigla")).alias("sigla_orgao"),
    )
    .filter(F.col("id_orgao").isNotNull())
    .dropDuplicates(["id_orgao"])
)
save_prata(df_dim_orgao, "dim_orgao")

# dim_data: calendario analitico com atributos temporais derivados
# Permite analises por semana, mes, trimestre, dia da semana e fim de semana
df_dim_data = (
    df_ev
    .withColumn("data_evento", F.to_date("dataHoraInicio"))
    .select("data_evento").distinct()
    .filter(F.col("data_evento").isNotNull())
    .withColumn("ano",        F.year("data_evento"))
    .withColumn("mes",        F.month("data_evento"))
    .withColumn("dia",        F.dayofmonth("data_evento"))
    .withColumn("semana_ano", F.weekofyear("data_evento"))
    .withColumn("dia_semana", F.dayofweek("data_evento"))
    .withColumn("trimestre",  F.quarter("data_evento"))
    .withColumn("nome_mes",   F.date_format("data_evento", "MMMM"))
    # Flag para identificar fins de semana (1=domingo, 7=sabado no Spark)
    .withColumn("is_fds",     F.col("dia_semana").isin([1, 7]))
)
save_prata(df_dim_data, "dim_data")

# fato_eventos: tabela fato central de eventos legislativos
# Relaciona eventos com orgaos e datas para analises de calendario
df_fato_ev = (
    df_ev
    .select(
        F.col("id").cast("long").alias("id_evento"),
        F.col("orgao_id").cast("long").alias("id_orgao"),
        F.trim(F.col("descricaoTipo")).alias("desc_tipo_evento"),
        F.to_date("dataHoraInicio").alias("data_evento"),
        F.to_timestamp("dataHoraInicio").alias("dt_inicio"),
        F.to_timestamp("dataHoraFim").alias("dt_fim"),
        F.trim(F.col("descricao")).alias("descricao"),
        # Campo opcional: retorna null se nao existir na Bronze
        F.trim(F.col("local_nome")).alias("local") if "local_nome" in df_ev.columns else F.lit(None).alias("local"),
        F.trim(F.col("situacao")).alias("situacao"),
        F.col("urlRegistro"),
        F.col("_ingest_timestamp"),
    )
    .dropDuplicates(["id_evento"])
    .withColumn("_updated_at", F.current_timestamp())
)
save_prata(df_fato_ev, "fato_eventos")
print(f"Eventos -- orgao: {df_dim_orgao.count()} | datas: {df_dim_data.count()} | fato: {df_fato_ev.count()}")

# COMMAND ----------

# MAGIC %md ## Despesas — dim_fornecedor, dim_categoria, fato_despesas

# COMMAND ----------

# Carrega a tabela Bronze de despesas como base para as tres tabelas da Prata
df_desp = read_bronze("despesas_ceap")

# dim_fornecedor: dimensao de fornecedores da CEAP
# Inclui flag para identificar pessoas fisicas (CPF com ate 11 digitos numericos)
# Util para sinalizar pagamentos suspeitos a pessoas fisicas
df_dim_forn = (
    df_desp
    .select(
        F.trim(F.col("cnpjCpfFornecedor")).alias("cnpj_cpf_fornecedor"),
        F.trim(F.col("nomeFornecedor")).alias("nome_fornecedor"),
    )
    .filter(F.col("cnpj_cpf_fornecedor").isNotNull())
    .dropDuplicates(["cnpj_cpf_fornecedor"])
    # CPF tem 11 digitos numericos; CNPJ tem 14 — apos remover pontuacao
    .withColumn("is_pessoa_fisica",
        F.length(F.regexp_replace("cnpj_cpf_fornecedor", r"[.\-/]", "")) <= 11)
)
save_prata(df_dim_forn, "dim_fornecedor")

# dim_categoria_despesa: dimensao de tipos de despesa da CEAP
# ID gerado por CRC32 do nome da categoria para consistencia entre execucoes
df_dim_cat = (
    df_desp
    .select(F.trim(F.col("tipoDespesa")).alias("desc_categoria"))
    .dropDuplicates()
    .withColumn("id_categoria", F.crc32("desc_categoria").cast("long"))
)
save_prata(df_dim_cat, "dim_categoria_despesa")

# fato_despesas: tabela fato central de despesas parlamentares
# Filtra apenas registros com valor liquido positivo para excluir estornos e glosas totais
# Usa try_cast para tolerar valores vazios vindos da Bronze (todos os campos vem como String)
df_fato_desp = (
    df_desp
    .select(
        F.col("_record_hash").alias("id_despesa"),
        F.expr("try_cast(_deputado_id as long)").alias("id_deputado"),
        F.trim(F.col("cnpjCpfFornecedor")).alias("cnpj_cpf_fornecedor"),
        F.trim(F.col("tipoDespesa")).alias("desc_categoria"),
        F.expr("try_cast(_ano as int)").alias("ano"),
        F.expr("try_cast(mes as int)").alias("mes"),
        F.to_date(F.col("dataDocumento")).alias("data_documento"),
        F.expr("try_cast(valorBruto as double)").alias("valor_bruto"),
        F.expr("try_cast(valorLiquido as double)").alias("valor_liquido"),
        F.expr("try_cast(valorGlosa as double)").alias("valor_glosa"),
        F.trim(F.col("numDocumento")).alias("num_documento"),
        F.col("urlDocumento").alias("url_documento"),
        F.col("_ingest_timestamp"),
    )
    .filter(F.col("valor_liquido") > 0)
    .dropDuplicates(["id_despesa"])
    .withColumn("_updated_at", F.current_timestamp())
)
save_prata(df_fato_desp, "fato_despesas")
print(f"Despesas -- fornecedores: {df_dim_forn.count()} | categorias: {df_dim_cat.count()} | fato: {df_fato_desp.count()}")

# COMMAND ----------

# MAGIC %md ## Proposicoes — SCD Type 2

# COMMAND ----------

# DBTITLE 1,proposicoes_scd2

# Implementacao de Slowly Changing Dimension Type 2 para tramitacoes de proposicoes
# Permite rastrear toda a evolucao de status de um PL ao longo do tempo
# e reconstruir seu estado em qualquer data historica

df_tram = read_bronze("proposicoes_tramitacoes")

# Window particionada por proposicao e ordenada por sequencia e data
# Usada para calcular valid_to como a data do proximo status da mesma proposicao
w_prop = Window.partitionBy("_proposicao_id").orderBy("sequencia", "dataHora")

df_scd2 = (
    df_tram
    .select(
        F.col("_proposicao_id").cast("long").alias("id_proposicao"),
        F.col("_payload_hash").alias("hash_registro"),
        F.col("sequencia").cast("int"),
        F.to_timestamp("dataHora").alias("data_hora_status"),
        F.trim(F.col("descricaoSituacao")).alias("situacao"),
        F.trim(F.col("descricaoTramitacao")).alias("descricao_tramitacao"),
        F.trim(F.col("siglaOrgao")).alias("sigla_orgao"),
        F.trim(F.col("despacho")).alias("despacho"),
    )
    .dropDuplicates(["hash_registro"])
    # valid_from: data em que este status entrou em vigor
    .withColumn("valid_from", F.col("data_hora_status"))
    # valid_to: data do proximo status (null indica o registro atual)
    # Calculado com LEAD sobre a window ordenada por sequencia
    .withColumn("valid_to", F.lead("data_hora_status", 1).over(w_prop))
    # is_current: true apenas para o status mais recente da proposicao
    .withColumn("is_current", F.col("valid_to").isNull())
    # Registros atuais recebem data futura maxima como valid_to
    # Permite consultas por intervalo sem tratamento especial para registros atuais
    .withColumn("valid_to",
        F.when(F.col("is_current"), F.lit("9999-12-31").cast("timestamp"))
        .otherwise(F.col("valid_to")))
    # Chave surrogate composta por id da proposicao e numero de sequencia
    .withColumn("sk_tramitacao",
        F.concat_ws("_", F.col("id_proposicao").cast("string"), F.col("sequencia").cast("string")))
    .withColumn("_updated_at", F.current_timestamp())
    .select("sk_tramitacao", "id_proposicao", "sequencia", "situacao",
            "descricao_tramitacao", "sigla_orgao", "despacho",
            "valid_from", "valid_to", "is_current", "hash_registro", "_updated_at")
)

save_prata(df_scd2, "proposicoes_scd2")
print(f"SCD2: {df_scd2.count()} registros | Atuais: {df_scd2.filter('is_current').count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Delta Time Travel -- Reconstrucao historica
# MAGIC
# MAGIC Consulta por data especifica usando os campos SCD2:
# MAGIC ```sql
# MAGIC -- Estado de qualquer PL em qualquer data
# MAGIC SELECT * FROM prata_camara.proposicoes_scd2
# MAGIC WHERE valid_from <= '2024-06-01'
# MAGIC   AND valid_to   >  '2024-06-01'
# MAGIC ```
# MAGIC
# MAGIC Consulta usando versao do Delta Lake (Time Travel nativo):
# MAGIC ```sql
# MAGIC SELECT * FROM prata_camara.proposicoes_scd2 VERSION AS OF 3
# MAGIC ```
