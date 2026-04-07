select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select ano
from "medallion"."main_gold"."gld_saude_por_uf"
where ano is null



      
    ) dbt_internal_test