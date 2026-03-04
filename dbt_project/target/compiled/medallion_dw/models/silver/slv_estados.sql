-- models/silver/slv_estados.sql
-- Camada Silver: dados de estados limpos, validados e enriquecidos.
-- Regras de negócio aplicadas: normalização de nomes, categorização de
-- desenvolvimento, ranking intra-regional.



WITH base AS (
    SELECT
        sigla_uf,
        TRIM(nome_uf)                         AS nome_uf,
        regiao,
        populacao,
        pib_per_capita,
        ROUND(CAST(idh AS DOUBLE), 3)         AS idh,
        ano

    FROM "medallion"."main_bronze"."brz_ibge_estados"
    WHERE
        sigla_uf IS NOT NULL
        AND populacao > 0
        AND pib_per_capita > 0
        AND idh BETWEEN 0 AND 1
        AND ano BETWEEN 2019 AND 2023
),

categorizado AS (
    SELECT
        *,

        -- Nível de desenvolvimento humano (PNUD)
        CASE
            WHEN idh >= 0.800 THEN 'Muito Alto'
            WHEN idh >= 0.700 THEN 'Alto'
            WHEN idh >= 0.555 THEN 'Médio'
            ELSE 'Baixo'
        END AS nivel_idh,

        -- Quartil de renda per capita (por ano)
        NTILE(4) OVER (PARTITION BY ano ORDER BY pib_per_capita) AS quartil_pib,

        -- Ranking de PIB per capita dentro da região
        RANK() OVER (
            PARTITION BY regiao, ano ORDER BY pib_per_capita DESC
        ) AS rank_pib_regiao,

        -- Variação absoluta de PIB vs. ano anterior
        pib_per_capita - LAG(pib_per_capita) OVER (
            PARTITION BY sigla_uf ORDER BY ano
        ) AS variacao_pib_yoy,

        -- Densidade demográfica calculada (placeholder — área fixa por UF)
        ROUND(
            populacao::DOUBLE / NULLIF(
                CASE sigla_uf
                    WHEN 'SP' THEN 248219 WHEN 'RJ' THEN 43696  WHEN 'MG' THEN 586519
                    WHEN 'BA' THEN 564692 WHEN 'RS' THEN 281748 WHEN 'PR' THEN 199298
                    WHEN 'SC' THEN 95730  WHEN 'GO' THEN 340111 WHEN 'MT' THEN 903207
                    WHEN 'MS' THEN 357145 WHEN 'PA' THEN 1247954 WHEN 'AM' THEN 1559148
                    WHEN 'CE' THEN 148894 WHEN 'PE' THEN 98067  WHEN 'MA' THEN 331937
                    WHEN 'DF' THEN 5760   WHEN 'ES' THEN 46074  WHEN 'PI' THEN 251612
                    WHEN 'RN' THEN 52811  WHEN 'PB' THEN 56469  WHEN 'AL' THEN 27843
                    WHEN 'SE' THEN 21910  WHEN 'TO' THEN 277423 WHEN 'RO' THEN 237590
                    WHEN 'AC' THEN 164174 WHEN 'RR' THEN 224299 WHEN 'AP' THEN 142829
                    ELSE 100000
                END, 0
            ), 2
        ) AS densidade_demografica

    FROM base
)

SELECT
    sigla_uf,
    nome_uf,
    regiao,
    populacao,
    pib_per_capita,
    idh,
    nivel_idh,
    quartil_pib,
    rank_pib_regiao,
    ROUND(COALESCE(variacao_pib_yoy, 0), 2) AS variacao_pib_yoy,
    densidade_demografica,
    ano,
    CURRENT_TIMESTAMP AS _updated_at
FROM categorizado