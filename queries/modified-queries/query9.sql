
select case when (select count(*) 
                  from mongodb.tpcds.store_sales 
                  where ss_quantity between 1 and 20) > 169437
            then (select avg(ss_ext_tax) 
                  from mongodb.tpcds.store_sales 
                  where ss_quantity between 1 and 20) 
            else (select avg(ss_net_paid)
                  from mongodb.tpcds.store_sales
                  where ss_quantity between 1 and 20) end bucket1 ,
       case when (select count(*)
                  from mongodb.tpcds.store_sales
                  where ss_quantity between 21 and 40) > 62947
            then (select avg(ss_ext_tax)
                  from mongodb.tpcds.store_sales
                  where ss_quantity between 21 and 40) 
            else (select avg(ss_net_paid)
                  from mongodb.tpcds.store_sales
                  where ss_quantity between 21 and 40) end bucket2,
       case when (select count(*)
                  from mongodb.tpcds.store_sales
                  where ss_quantity between 41 and 60) > 56581
            then (select avg(ss_ext_tax)
                  from mongodb.tpcds.store_sales
                  where ss_quantity between 41 and 60)
            else (select avg(ss_net_paid)
                  from mongodb.tpcds.store_sales
                  where ss_quantity between 41 and 60) end bucket3,
       case when (select count(*)
                  from mongodb.tpcds.store_sales
                  where ss_quantity between 61 and 80) > 10098
            then (select avg(ss_ext_tax)
                  from mongodb.tpcds.store_sales
                  where ss_quantity between 61 and 80)
            else (select avg(ss_net_paid)
                  from mongodb.tpcds.store_sales
                  where ss_quantity between 61 and 80) end bucket4,
       case when (select count(*)
                  from mongodb.tpcds.store_sales
                  where ss_quantity between 81 and 100) > 77817
            then (select avg(ss_ext_tax)
                  from mongodb.tpcds.store_sales
                  where ss_quantity between 81 and 100)
            else (select avg(ss_net_paid)
                  from mongodb.tpcds.store_sales
                  where ss_quantity between 81 and 100) end bucket5
from mongodb.tpcds.reason
where r_reason_sk = 1
;


