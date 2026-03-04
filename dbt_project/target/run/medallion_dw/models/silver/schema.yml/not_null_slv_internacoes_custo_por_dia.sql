select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select custo_por_dia
from "medallion"."main_silver"."slv_internacoes"
where custo_por_dia is null



      
    ) dbt_internal_test