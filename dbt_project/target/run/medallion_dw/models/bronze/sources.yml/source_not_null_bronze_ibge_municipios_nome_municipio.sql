select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select nome_municipio
from "medallion"."bronze"."ibge_municipios"
where nome_municipio is null



      
    ) dbt_internal_test