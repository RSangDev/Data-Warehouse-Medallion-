select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

with all_values as (

    select
        nivel_idh as value_field,
        count(*) as n_records

    from "medallion"."main_silver"."slv_estados"
    group by nivel_idh

)

select *
from all_values
where value_field not in (
    'Muito Alto','Alto','Medio','Baixo'
)



      
    ) dbt_internal_test