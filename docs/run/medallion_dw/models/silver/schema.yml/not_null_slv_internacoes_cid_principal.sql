select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select cid_principal
from "medallion"."main_silver"."slv_internacoes"
where cid_principal is null



      
    ) dbt_internal_test