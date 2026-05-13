# Databricks notebook source
# MAGIC %md
# MAGIC # 23 - Ouro: Auditoria de CPIs e CDC de Tramitacao
# MAGIC
# MAGIC Entregaveis gerados neste notebook:
# MAGIC - gold_cpis_timeline: timeline de CPIs com duracao e status
# MAGIC - gold_cpis_proposicoes_derivadas: proposicoes identificadas como derivadas de CPIs
# MAGIC - gold_cpis_produtividade: comparativo entre CPIs encerradas e ativas
# MAGIC - gold_tempo_medio_tramitacao: duracao media de tramitacoes por orgao e situacao
# MAGIC - gold_proposicoes_em_plenario: PLs que chegaram ao Plenario
# MAGIC
# MAGIC Todas as tabelas sao criadas mesmo quando os dados de origem estao vazios,
# MAGIC garantindo consistencia do schema para execucoes futuras.

# COMMAND ----------

# Carrega configuracoes globais e funcoes utilitarias
# MAGIC %run ../utils/00_api_utils

# COMMAND ----------

# MAGIC %md ## 6 — Pipeline de Auditoria de CPIs

# COMMAND ----------

# DBTITLE 1,Tabela: gold_cpis_timeline

# Timeline das CPIs com duracao e status
# Tenta agregar eventos por CPI se disponivel na Bronze
# Se a API nao retornou eventos para as CPIs, cria a tabela apenas com dados cadastrais
try:
    df_cpis   = read_bronze("cpis_lista")
    df_ev_cpi = read_bronze("cpis_eventos")

    total_ev = df_ev_cpi.count()
    print(f"Eventos CPI disponiveis: {total_ev}")

    if total_ev > 0:
        # Agrega eventos por CPI quando ha dados disponveis
        df_agg_ev = (
            df_ev_cpi
            .groupBy("_cpi_id")
            .agg(
                F.count("id").alias("total_eventos"),
                F.min(F.to_date("dataHoraInicio")).alias("data_primeiro_evento"),
                F.max(F.to_date("dataHoraInicio")).alias("data_ultimo_evento"),
            )
        )
        df_cpi_tl = (
            df_cpis
            .select(
                F.col("id").cast("long").alias("_cpi_id"),
                F.col("nome"),
                F.col("sigla"),
            )
            .join(df_agg_ev, "_cpi_id", "left")
            .fillna(0, subset=["total_eventos"])
        )
    else:
        # Sem eventos: cria tabela com zeros para garantir consistencia do schema
        df_cpi_tl = (
            df_cpis
            .select(
                F.col("id").cast("long").alias("_cpi_id"),
                F.col("nome"),
                F.col("sigla"),
            )
            .withColumn("total_eventos",        F.lit(0))
            .withColumn("data_primeiro_evento", F.lit(None).cast("date"))
            .withColumn("data_ultimo_evento",   F.lit(None).cast("date"))
        )

    # Campos de duracao e status derivados das datas cadastrais da CPI
    # Prazo regimental: 180 dias conforme regimento interno da Camara
    df_cpi_tl = (
        df_cpi_tl
        .withColumn("duracao_dias",
            F.datediff(
                F.coalesce(F.to_date("dataEncerramento"), F.current_date()),
                F.to_date("dataInstalacao"))
            if "dataInstalacao" in df_cpi_tl.columns else F.lit(None).cast("int"))
        .withColumn("excedeu_prazo_regimental",
            F.col("duracao_dias") > 180 if "duracao_dias" in df_cpi_tl.columns else F.lit(None).cast("boolean"))
        .withColumn("status_cpi", F.lit("SEM_DADOS"))
    )

    save_ouro(df_cpi_tl, "gold_cpis_timeline")
    print(f"gold_cpis_timeline: {df_cpi_tl.count()} CPIs")
    display(df_cpi_tl.limit(10))

except Exception as e:
    print(f"CPIs Timeline nao disponivel: {e}")

# COMMAND ----------

# DBTITLE 1,Tabela: gold_cpis_proposicoes_derivadas

# Identifica proposicoes legislativas possivelmente derivadas de cada CPI
# Metodologia: verifica se a ementa da proposicao contem o nome da CPI (busca textual)
# Decisao: join por similaridade textual como aproximacao na ausencia de metadado explicito
try:
    df_prop = read_bronze("proposicoes_lista")
    df_cpis = read_bronze("cpis_lista")

    df_cpi_prop = (
        df_cpis
        .join(
            df_prop.select("id", "siglaTipo", "numero", "ano", "ementa"),
            # Condicao de join: nome da CPI aparece na ementa da proposicao (case insensitive)
            F.lower(df_prop["ementa"]).contains(F.lower(df_cpis["nome"])),
            "left"
        )
        .select(
            df_cpis["id"].alias("id_cpi"),
            df_cpis["nome"].alias("nome_cpi"),
            df_cpis["sigla"].alias("sigla_cpi"),
            df_prop["id"].alias("id_proposicao"),
            "siglaTipo", "numero", "ano", "ementa"
        )
        .filter(F.col("id_proposicao").isNotNull())
    )
    save_ouro(df_cpi_prop, "gold_cpis_proposicoes_derivadas")
    print(f"gold_cpis_proposicoes_derivadas: {df_cpi_prop.count()} registros")
except Exception as e:
    print(f"CPIs x Proposicoes nao disponivel: {e}")

# COMMAND ----------

# DBTITLE 1,Tabela: gold_cpis_produtividade

# Comparativo de produtividade entre CPIs encerradas e ativas
# CPIs encerradas sao consideradas como tendo gerado relatorio
# Metricas: total de CPIs, duracao media e media de eventos por grupo
try:
    df_cpi_tl = read_ouro("gold_cpis_timeline")

    df_prod = (
        df_cpi_tl
        # Flag: CPI encerrada e considerada como tendo gerado relatorio
        .withColumn("gerou_relatorio",
            F.lower(F.col("status_cpi")).contains("encerrada"))
        .groupBy("gerou_relatorio")
        .agg(
            F.count("_cpi_id").alias("total_cpis"),
            # Cast para double necessario pois campos podem ser null ou int
            F.round(F.avg(F.col("duracao_dias").cast("double")), 1).alias("duracao_media_dias"),
            F.round(F.avg(F.col("total_eventos").cast("double")), 1).alias("media_eventos"),
        )
    )
    save_ouro(df_prod, "gold_cpis_produtividade")
    print(f"gold_cpis_produtividade gerado")
    display(df_prod)
except Exception as e:
    print(f"Produtividade CPIs nao disponivel: {e}")

# COMMAND ----------

# MAGIC %md ## 7 — CDC de Tramitacao

# COMMAND ----------

# DBTITLE 1,Tabela: gold_tempo_medio_tramitacao

# Calcula a duracao media de cada etapa de tramitacao por orgao e situacao
# Usa os campos valid_from e valid_to da tabela SCD2 da Prata
# Filtra apenas registros historicos (nao atuais) para ter valid_to real
try:
    df_scd2 = read_prata("proposicoes_scd2")

    df_tempo = (
        df_scd2
        # Exclui o registro atual de cada proposicao pois seu valid_to e 9999-12-31 (artificial)
        .filter(~F.col("is_current"))
        # Calcula duracao em horas entre valid_from e valid_to usando timestamps Unix
        .withColumn("duracao_horas",
            (F.unix_timestamp("valid_to") - F.unix_timestamp("valid_from")) / 3600)
        # Filtra duracoes positivas para excluir registros com datas invertidas
        .filter(F.col("duracao_horas") > 0)
        .groupBy("sigla_orgao", "situacao")
        .agg(
            F.count("sk_tramitacao").alias("total_tramitacoes"),
            F.round(F.avg("duracao_horas"), 1).alias("duracao_media_horas"),
            F.round(F.avg("duracao_horas") / 24, 1).alias("duracao_media_dias"),
        )
        .orderBy(F.desc("duracao_media_dias"))
    )
    save_ouro(df_tempo, "gold_tempo_medio_tramitacao")
    print(f"gold_tempo_medio_tramitacao: {df_tempo.count()} registros")
except Exception as e:
    print(f"Tempo de tramitacao nao disponivel: {e}")

# COMMAND ----------

# DBTITLE 1,Tabela: gold_proposicoes_em_plenario

# Identifica proposicoes que chegaram ao Plenario com base no status atual do SCD2
# Detecta por palavra-chave no campo situacao ou sigla do orgao
try:
    df_scd2 = read_prata("proposicoes_scd2")
    df_prop = read_bronze("proposicoes_lista")

    df_plenario = (
        df_scd2
        # Considera apenas o status atual de cada proposicao
        .filter(F.col("is_current"))
        # Filtra por mencao a Plenario na situacao ou sigla do orgao
        .filter(
            F.upper(F.col("situacao")).contains("PLENARIO") |
            F.upper(F.col("sigla_orgao")).contains("PLEN")
        )
        .join(
            df_prop.select(
                F.col("id").alias("id_proposicao"),
                "siglaTipo", "numero", "ano"
            ),
            "id_proposicao", "left"
        )
        .select(
            "id_proposicao", "siglaTipo", "numero", "ano",
            "situacao", "valid_from", "sigla_orgao"
        )
    )
    save_ouro(df_plenario, "gold_proposicoes_em_plenario")
    print(f"gold_proposicoes_em_plenario: {df_plenario.count()} registros")
except Exception as e:
    print(f"Proposicoes Plenario nao disponivel: {e}")

# COMMAND ----------

# DBTITLE 1,Resumo Final

# Lista todas as tabelas do projeto para confirmar o estado completo da carga
list_tables()
