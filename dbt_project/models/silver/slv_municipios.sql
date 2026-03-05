-- models/silver/slv_municipios.sql
{{
  config(
    materialized = 'table',
    tags = ['silver', 'ibge', 'dimensao']
  )
}}

WITH base AS (
    SELECT
        id_municipio,
        TRIM(nome_municipio)                  AS nome_municipio,
        sigla_uf,
        regiao,
        populacao,
        pib_per_capita,
        ROUND(idh_municipal, 3)               AS idh_municipal,
        area_km2,
        ano
    FROM {{ ref('brz_ibge_municipios') }}
    WHERE
        nome_municipio IS NOT NULL
        AND populacao > 0
        AND ano BETWEEN {{ var('start_year') }} AND {{ var('end_year') }}
),

enriquecido AS (
    SELECT
        m.*,

        -- Porte demográfico
        CASE
            WHEN m.populacao >= 1000000  THEN 'Metropole'
            WHEN m.populacao >= 500000   THEN 'Grande'
            WHEN m.populacao >= 100000   THEN 'Medio'
            WHEN m.populacao >= 20000    THEN 'Pequeno'
            ELSE 'Micro'
        END AS porte_municipio,

        -- Densidade urbana
        ROUND(m.populacao::DOUBLE / NULLIF(m.area_km2, 0), 2) AS densidade_hab_km2,

        -- Ranking por UF
        RANK() OVER (
            PARTITION BY m.sigla_uf, m.ano ORDER BY m.populacao DESC
        ) AS rank_populacao_uf,

        -- Indicadores do estado de referência
        e.pib_per_capita  AS pib_pc_uf,
        e.idh             AS idh_uf,
        e.nivel_idh

    FROM base m
    LEFT JOIN {{ ref('slv_estados') }} e
        ON m.sigla_uf = e.sigla_uf AND m.ano = e.ano
)

SELECT
    id_municipio,
    nome_municipio,
    sigla_uf,
    regiao,
    porte_municipio,
    populacao,
    pib_per_capita,
    idh_municipal,
    area_km2,
    densidade_hab_km2,
    rank_populacao_uf,
    pib_pc_uf,
    idh_uf,
    nivel_idh,
    ano,
    CURRENT_TIMESTAMP AS _updated_at
FROM enriquecido