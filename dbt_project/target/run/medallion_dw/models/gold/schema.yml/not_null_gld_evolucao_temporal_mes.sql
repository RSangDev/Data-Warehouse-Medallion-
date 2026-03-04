select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select mes
from "medallion"."main_gold"."gld_evolucao_temporal"
where mes is null



      
    ) dbt_internal_test