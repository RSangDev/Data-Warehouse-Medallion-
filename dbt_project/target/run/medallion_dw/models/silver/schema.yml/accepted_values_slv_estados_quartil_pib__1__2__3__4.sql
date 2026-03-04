select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

with all_values as (

    select
        quartil_pib as value_field,
        count(*) as n_records

    from "medallion"."main_silver"."slv_estados"
    group by quartil_pib

)

select *
from all_values
where value_field not in (
    '1','2','3','4'
)



      
    ) dbt_internal_test