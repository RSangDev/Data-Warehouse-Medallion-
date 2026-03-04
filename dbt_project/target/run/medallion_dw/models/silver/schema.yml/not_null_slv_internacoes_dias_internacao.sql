select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select dias_internacao
from "medallion"."main_silver"."slv_internacoes"
where dias_internacao is null



      
    ) dbt_internal_test