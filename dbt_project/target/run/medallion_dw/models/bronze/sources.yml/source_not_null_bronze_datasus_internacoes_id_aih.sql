select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select id_aih
from "medallion"."bronze"."datasus_internacoes"
where id_aih is null



      
    ) dbt_internal_test