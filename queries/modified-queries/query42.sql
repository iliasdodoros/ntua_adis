
select  dt.d_year
 	,cassandra.tpcds.item.i_category_id
 	,cassandra.tpcds.item.i_category
 	,sum(ss_ext_sales_price)
 from 	cassandra.tpcds.date_dim dt
 	,cassandra.tpcds.store_sales
 	,cassandra.tpcds.item
 where dt.d_date_sk = cassandra.tpcds.store_sales.ss_sold_date_sk
 	and cassandra.tpcds.store_sales.ss_item_sk = cassandra.tpcds.item.i_item_sk
 	and cassandra.tpcds.item.i_manager_id = 1  	
 	and dt.d_moy=12
 	and dt.d_year=1998
 group by 	dt.d_year
 		,cassandra.tpcds.item.i_category_id
 		,cassandra.tpcds.item.i_category
 order by       sum(ss_ext_sales_price) desc,dt.d_year
 		,cassandra.tpcds.item.i_category_id
 		,cassandra.tpcds.item.i_category
limit 100 ;


