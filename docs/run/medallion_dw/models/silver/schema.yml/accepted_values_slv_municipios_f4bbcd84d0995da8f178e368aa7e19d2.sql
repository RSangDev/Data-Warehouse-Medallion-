select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

with all_values as (

    select
        porte_municipio as value_field,
        count(*) as n_records

    from "medallion"."main_silver"."slv_municipios"
    group by porte_municipio

)

select *
from all_values
where value_field not in (
    'Metropole','Grande','Medio','Pequeno','Micro'
)



      
    ) dbt_internal_test