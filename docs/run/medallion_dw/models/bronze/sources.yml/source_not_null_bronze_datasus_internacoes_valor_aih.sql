select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select valor_aih
from "medallion"."bronze"."datasus_internacoes"
where valor_aih is null



      
    ) dbt_internal_test