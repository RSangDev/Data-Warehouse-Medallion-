select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select cid_principal
from "medallion"."main_gold"."gld_diagnosticos_resumo"
where cid_principal is null



      
    ) dbt_internal_test