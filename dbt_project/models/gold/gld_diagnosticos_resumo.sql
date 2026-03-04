-- models/gold/gld_diagnosticos_resumo.sql
-- Camada Gold: ranking e análise de diagnósticos CID-10 por ano.
-- Agrega internações, custo e mortalidade por CID e capítulo.

{{
  config(
    materialized = 'table',
    tags = ['gold', 'diagnosticos', 'analytics']
  )
}}

WITH base AS (
    SELECT
        cid_principal,
        descricao_cid,
        capitulo_cid,
        ano,
        COUNT(*)                           AS total_internacoes,
        SUM(valor_aih)                     AS custo_total,
        ROUND(AVG(valor_aih), 2)           AS custo_medio,
        ROUND(AVG(dias_internacao), 2)     AS dias_medio,
        SUM(obito)                         AS total_obitos,
        ROUND(AVG(obito) * 100, 3)         AS taxa_obito_pct,
        ROUND(AVG(idade), 1)               AS idade_media_paciente,
        COUNT(DISTINCT sigla_uf)           AS estados_ativos,
        SUM(CASE WHEN sexo = 'F' THEN 1 ELSE 0 END)::DOUBLE / NULLIF(COUNT(*), 0) * 100 AS pct_feminino
    FROM {{ ref('slv_internacoes') }}
    GROUP BY 1, 2, 3, 4
),

ranqueado AS (
    SELECT
        *,
        RANK() OVER (PARTITION BY ano ORDER BY total_internacoes DESC) AS rank_volume,
        RANK() OVER (PARTITION BY ano ORDER BY custo_total DESC)       AS rank_custo,
        RANK() OVER (PARTITION BY ano ORDER BY taxa_obito_pct DESC)    AS rank_mortalidade,
        ROUND(custo_total::DOUBLE / NULLIF(
            SUM(custo_total) OVER (PARTITION BY ano), 0
        ) * 100, 2) AS pct_custo_total
    FROM base
)

SELECT
    cid_principal,
    descricao_cid,
    capitulo_cid,
    ano,
    total_internacoes,
    custo_total,
    custo_medio,
    dias_medio,
    total_obitos,
    taxa_obito_pct,
    idade_media_paciente,
    estados_ativos,
    ROUND(pct_feminino, 1)  AS pct_feminino,
    rank_volume,
    rank_custo,
    rank_mortalidade,
    pct_custo_total,
    CURRENT_TIMESTAMP       AS _updated_at
FROM ranqueado
ORDER BY ano, rank_volume
