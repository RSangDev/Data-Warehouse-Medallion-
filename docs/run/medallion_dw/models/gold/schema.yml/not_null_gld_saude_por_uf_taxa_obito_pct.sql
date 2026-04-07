select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select taxa_obito_pct
from "medallion"."main_gold"."gld_saude_por_uf"
where taxa_obito_pct is null



      
    ) dbt_internal_test