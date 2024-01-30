
select  dt.d_year 
       ,redis.item.item.i_brand_id brand_id 
       ,redis.item.item.i_brand brand
       ,sum(ss_ext_sales_price) sum_agg
 from  cassandra.tpcds.date_dim dt 
      ,mongodb.tpcds.store_sales
      ,redis.item.item
 where dt.d_date_sk = mongodb.tpcds.store_sales.ss_sold_date_sk
   and mongodb.tpcds.store_sales.ss_item_sk = redis.item.item.i_item_sk
   and redis.item.item.i_manufact_id = 436
   and dt.d_moy=12
 group by dt.d_year
      ,redis.item.item.i_brand
      ,redis.item.item.i_brand_id
 order by dt.d_year
         ,sum_agg desc
         ,brand_id
 limit 100;


