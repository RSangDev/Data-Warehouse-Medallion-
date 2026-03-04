select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

with all_values as (

    select
        trimestre as value_field,
        count(*) as n_records

    from "medallion"."main_gold"."gld_evolucao_temporal"
    group by trimestre

)

select *
from all_values
where value_field not in (
    'Q1','Q2','Q3','Q4'
)



      
    ) dbt_internal_test