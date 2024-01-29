
with ss as (
 select
          i_manufact_id,sum(ss_ext_sales_price) total_sales
 from
 	mongodb.tpcds.store_sales,
 	cassandra.tpcds.date_dim,
         redis.customer_address.customer_address,
         mongodb.tpcds.item
 where
         i_manufact_id in (select
  i_manufact_id
from
 mongodb.tpcds.item
where i_category in ('Books'))
 and     ss_item_sk              = i_item_sk
 and     ss_sold_date_sk         = d_date_sk
 and     d_year                  = 1999
 and     d_moy                   = 3
 and     ss_addr_sk              = ca_address_sk
 and     ca_gmt_offset           = -6 
 group by i_manufact_id),
 cs as (
 select
          i_manufact_id,sum(cs_ext_sales_price) total_sales
 from
 	cassandra.tpcds.catalog_sales,
 	cassandra.tpcds.date_dim,
         redis.customer_address.customer_address,
         mongodb.tpcds.item
 where
         i_manufact_id               in (select
  i_manufact_id
from
 mongodb.tpcds.item
where i_category in ('Books'))
 and     cs_item_sk              = i_item_sk
 and     cs_sold_date_sk         = d_date_sk
 and     d_year                  = 1999
 and     d_moy                   = 3
 and     cs_bill_addr_sk         = ca_address_sk
 and     ca_gmt_offset           = -6 
 group by i_manufact_id),
 ws as (
 select
          i_manufact_id,sum(ws_ext_sales_price) total_sales
 from
 	mongodb.tpcds.web_sales,
 	cassandra.tpcds.date_dim,
         redis.customer_address.customer_address,
         mongodb.tpcds.item
 where
         i_manufact_id               in (select
  i_manufact_id
from
 mongodb.tpcds.item
where i_category in ('Books'))
 and     ws_item_sk              = i_item_sk
 and     ws_sold_date_sk         = d_date_sk
 and     d_year                  = 1999
 and     d_moy                   = 3
 and     ws_bill_addr_sk         = ca_address_sk
 and     ca_gmt_offset           = -6
 group by i_manufact_id)
  select  i_manufact_id ,sum(total_sales) total_sales
 from  (select * from ss 
        union all
        select * from cs 
        union all
        select * from ws) tmp1
 group by i_manufact_id
 order by total_sales
limit 100;


