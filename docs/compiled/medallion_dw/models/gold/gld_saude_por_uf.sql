-- models/gold/gld_saude_por_uf.sql
-- Camada Gold: visão analítica de saúde pública por UF e ano.
-- Combina DATASUS com IBGE para produzir indicadores de eficiência e acesso.
-- Esta é a tabela consumida pelo dashboard e pelos analistas.



WITH internacoes_uf AS (
    SELECT
        sigla_uf,
        regiao,
        ano,
        COUNT(*)                                  AS total_internacoes,
        SUM(valor_aih)                            AS custo_total,
        ROUND(AVG(valor_aih), 2)                  AS custo_medio_aih,
        ROUND(AVG(dias_internacao), 2)            AS dias_medio_internacao,
        ROUND(AVG(custo_por_dia), 2)              AS custo_medio_dia,
        SUM(obito)                                AS total_obitos,
        ROUND(AVG(obito) * 100, 3)                AS taxa_obito_pct,
        COUNT(DISTINCT cid_principal)             AS cids_distintos,
        COUNT(DISTINCT tipo_hospital)             AS tipos_hospital_ativos,
        SUM(CASE WHEN carater_internacao = 'Urgência' THEN 1 ELSE 0 END) AS internacoes_urgencia,
        SUM(CASE WHEN carater_internacao = 'Eletivo'  THEN 1 ELSE 0 END) AS internacoes_eletivo,
        ROUND(
            SUM(CASE WHEN carater_internacao = 'Urgência' THEN 1.0 ELSE 0 END)
            / NULLIF(COUNT(*), 0) * 100, 2
        )                                         AS pct_urgencia
    FROM "medallion"."main_silver"."slv_internacoes"
    GROUP BY 1, 2, 3
),

socioeconomico AS (
    SELECT
        sigla_uf,
        nome_uf,
        regiao,
        populacao,
        pib_per_capita,
        idh,
        nivel_idh,
        quartil_pib,
        densidade_demografica,
        ano
    FROM "medallion"."main_silver"."slv_estados"
),

joined AS (
    SELECT
        s.sigla_uf,
        s.nome_uf,
        s.regiao,
        s.ano,
        s.populacao,
        s.pib_per_capita,
        s.idh,
        s.nivel_idh,
        s.quartil_pib,
        s.densidade_demografica,

        COALESCE(i.total_internacoes, 0)    AS total_internacoes,
        COALESCE(i.custo_total, 0)          AS custo_total,
        COALESCE(i.custo_medio_aih, 0)      AS custo_medio_aih,
        COALESCE(i.dias_medio_internacao, 0) AS dias_medio_internacao,
        COALESCE(i.custo_medio_dia, 0)      AS custo_medio_dia,
        COALESCE(i.total_obitos, 0)         AS total_obitos,
        COALESCE(i.taxa_obito_pct, 0)       AS taxa_obito_pct,
        COALESCE(i.cids_distintos, 0)       AS cids_distintos,
        COALESCE(i.pct_urgencia, 0)         AS pct_urgencia,
        COALESCE(i.internacoes_urgencia, 0) AS internacoes_urgencia,
        COALESCE(i.internacoes_eletivo, 0)  AS internacoes_eletivo

    FROM socioeconomico s
    LEFT JOIN internacoes_uf i USING (sigla_uf, ano)
),

com_indicadores AS (
    SELECT
        *,

        -- Taxa de internação por 1.000 habitantes
        ROUND(
            total_internacoes::DOUBLE / NULLIF(populacao, 0) * 1000, 4
        ) AS tx_internacao_1k_hab,

        -- Custo per capita de saúde
        ROUND(
            custo_total::DOUBLE / NULLIF(populacao, 0), 2
        ) AS custo_saude_per_capita,

        -- Ranking nacional de custo médio (por ano)
        RANK() OVER (PARTITION BY ano ORDER BY custo_medio_aih DESC)   AS rank_custo_nacional,
        RANK() OVER (PARTITION BY ano ORDER BY taxa_obito_pct ASC)     AS rank_obito_nacional,
        RANK() OVER (PARTITION BY ano ORDER BY dias_medio_internacao ASC) AS rank_eficiencia,

        -- Variação YoY internações
        total_internacoes - LAG(total_internacoes) OVER (
            PARTITION BY sigla_uf ORDER BY ano
        ) AS variacao_internacoes_yoy,

        -- Score composto de performance (menor = melhor)
        ROUND(
            (taxa_obito_pct * 3)
            + (dias_medio_internacao * 0.5)
            + (custo_medio_aih / 1000 * 0.2),
            4
        ) AS score_performance

    FROM joined
)

SELECT
    sigla_uf,
    nome_uf,
    regiao,
    ano,
    populacao,
    pib_per_capita,
    idh,
    nivel_idh,
    quartil_pib,
    densidade_demografica,
    total_internacoes,
    custo_total,
    custo_medio_aih,
    dias_medio_internacao,
    custo_medio_dia,
    total_obitos,
    taxa_obito_pct,
    cids_distintos,
    pct_urgencia,
    internacoes_urgencia,
    internacoes_eletivo,
    tx_internacao_1k_hab,
    custo_saude_per_capita,
    rank_custo_nacional,
    rank_obito_nacional,
    rank_eficiencia,
    COALESCE(variacao_internacoes_yoy, 0) AS variacao_internacoes_yoy,
    score_performance,
    CURRENT_TIMESTAMP AS _updated_at
FROM com_indicadores
ORDER BY sigla_uf, ano