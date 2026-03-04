
  
    
    

    create  table
      "medallion"."main_silver"."slv_internacoes__dbt_tmp"
  
    as (
      -- models/silver/slv_internacoes.sql
-- Camada Silver: internações limpas, tipadas e enriquecidas com dados do estado.
-- Remove registros inválidos, padroniza categorias e cria features derivadas.



WITH internacoes_limpas AS (
    SELECT
        id_aih,
        UPPER(TRIM(sigla_uf))                 AS sigla_uf,
        TRIM(tipo_hospital)                   AS tipo_hospital,
        TRIM(natureza_juridica)               AS natureza_juridica,
        TRIM(carater_internacao)              AS carater_internacao,
        UPPER(TRIM(cid_principal))            AS cid_principal,
        TRIM(descricao_cid)                   AS descricao_cid,
        TRIM(capitulo_cid)                    AS capitulo_cid,
        CAST(idade AS INTEGER)                AS idade,
        TRIM(faixa_etaria)                    AS faixa_etaria,
        UPPER(TRIM(sexo))                     AS sexo,
        CAST(dias_internacao AS INTEGER)      AS dias_internacao,
        CAST(valor_aih AS DOUBLE)             AS valor_aih,
        CAST(obito AS INTEGER)                AS obito,
        CAST(ano AS INTEGER)                  AS ano,
        CAST(mes AS INTEGER)                  AS mes,
        data_internacao
    FROM "medallion"."main_bronze"."brz_datasus_internacoes"
    WHERE
        id_aih IS NOT NULL
        AND sigla_uf IS NOT NULL
        AND dias_internacao BETWEEN 1 AND 365
        AND valor_aih > 0
        AND obito IN (0, 1)
        AND idade BETWEEN 0 AND 120
        AND ano BETWEEN 2019 AND 2023
),

enriquecido AS (
    SELECT
        i.*,

        -- Custo diário da internação
        ROUND(i.valor_aih / i.dias_internacao, 2)  AS custo_por_dia,

        -- Faixa de custo AIH
        CASE
            WHEN i.valor_aih < 1000  THEN 'Baixo (< R$1k)'
            WHEN i.valor_aih < 5000  THEN 'Médio (R$1k-5k)'
            WHEN i.valor_aih < 15000 THEN 'Alto (R$5k-15k)'
            ELSE 'Muito Alto (> R$15k)'
        END AS faixa_custo,

        -- Tempo de internação categorizado
        CASE
            WHEN i.dias_internacao <= 2  THEN 'Muito Curta (≤2d)'
            WHEN i.dias_internacao <= 7  THEN 'Curta (3-7d)'
            WHEN i.dias_internacao <= 14 THEN 'Média (8-14d)'
            WHEN i.dias_internacao <= 30 THEN 'Longa (15-30d)'
            ELSE 'Prolongada (>30d)'
        END AS categoria_permanencia,

        -- Trimestre
        CONCAT('Q', CAST(CEIL(i.mes::DOUBLE / 3) AS INTEGER)) AS trimestre,

        -- Indicadores do estado
        e.pib_per_capita   AS pib_pc_uf,
        e.idh              AS idh_uf,
        e.nivel_idh,
        e.regiao,
        e.populacao        AS populacao_uf,

        -- Natureza simplificada
        CASE
            WHEN i.natureza_juridica LIKE '%Federal%'   THEN 'Federal'
            WHEN i.natureza_juridica LIKE '%Estadual%'  THEN 'Estadual'
            WHEN i.natureza_juridica LIKE '%Municipal%' THEN 'Municipal'
            WHEN i.natureza_juridica LIKE '%Privado%'   THEN 'Privado'
            ELSE 'Outros'
        END AS esfera_hospital

    FROM internacoes_limpas i
    LEFT JOIN "medallion"."main_silver"."slv_estados" e
        ON i.sigla_uf = e.sigla_uf AND i.ano = e.ano
)

SELECT
    id_aih,
    sigla_uf,
    regiao,
    tipo_hospital,
    natureza_juridica,
    esfera_hospital,
    carater_internacao,
    cid_principal,
    descricao_cid,
    capitulo_cid,
    idade,
    faixa_etaria,
    sexo,
    dias_internacao,
    valor_aih,
    custo_por_dia,
    faixa_custo,
    categoria_permanencia,
    obito,
    ano,
    mes,
    trimestre,
    data_internacao,
    pib_pc_uf,
    idh_uf,
    nivel_idh,
    populacao_uf,
    CURRENT_TIMESTAMP AS _updated_at
FROM enriquecido
    );
  
  