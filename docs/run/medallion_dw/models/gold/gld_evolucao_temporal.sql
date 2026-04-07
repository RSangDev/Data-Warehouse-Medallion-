
  
    
    

    create  table
      "medallion"."main_gold"."gld_evolucao_temporal__dbt_tmp"
  
    as (
      -- models/gold/gld_evolucao_temporal.sql
-- Camada Gold: série temporal mensal de internações com médias móveis.
-- Consumida por gráficos de tendência e detecção de sazonalidade.



WITH mensal AS (
    SELECT
        ano,
        mes,
        CONCAT('Q', CAST(CEIL(mes::DOUBLE / 3) AS INTEGER)) AS trimestre,
        regiao,
        COUNT(*)                               AS total_internacoes,
        SUM(valor_aih)                         AS custo_total,
        ROUND(AVG(valor_aih), 2)               AS custo_medio,
        ROUND(AVG(dias_internacao), 2)         AS dias_medio,
        SUM(obito)                             AS total_obitos,
        ROUND(AVG(obito) * 100, 3)             AS taxa_obito_pct,
        COUNT(DISTINCT sigla_uf)               AS ufs_com_dados,
        SUM(CASE WHEN carater_internacao = 'Urgência' THEN 1 ELSE 0 END) AS urgencias
    FROM "medallion"."main_silver"."slv_internacoes"
    GROUP BY 1, 2, 3, 4
),

com_media_movel AS (
    SELECT
        *,
        -- Média móvel 3 meses de internações
        ROUND(AVG(total_internacoes) OVER (
            PARTITION BY regiao
            ORDER BY ano, mes
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ), 1) AS mm3_internacoes,

        -- Média móvel 3 meses de custo
        ROUND(AVG(custo_medio) OVER (
            PARTITION BY regiao
            ORDER BY ano, mes
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ), 2) AS mm3_custo,

        -- % variação MoM internações
        ROUND(
            (total_internacoes - LAG(total_internacoes) OVER (
                PARTITION BY regiao ORDER BY ano, mes
            ))::DOUBLE / NULLIF(LAG(total_internacoes) OVER (
                PARTITION BY regiao ORDER BY ano, mes
            ), 0) * 100, 2
        ) AS variacao_mom_pct
    FROM mensal
)

SELECT
    ano,
    mes,
    trimestre,
    regiao,
    total_internacoes,
    custo_total,
    custo_medio,
    dias_medio,
    total_obitos,
    taxa_obito_pct,
    ufs_com_dados,
    urgencias,
    mm3_internacoes,
    mm3_custo,
    COALESCE(variacao_mom_pct, 0) AS variacao_mom_pct,
    CURRENT_TIMESTAMP AS _updated_at
FROM com_media_movel
ORDER BY regiao, ano, mes
    );
  
  