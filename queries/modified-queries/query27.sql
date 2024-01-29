
select  i_item_id,
        s_state, grouping(s_state) g_state,
        avg(ss_quantity) agg1,
        avg(ss_list_price) agg2,
        avg(ss_coupon_amt) agg3,
        avg(ss_sales_price) agg4
 from mongodb.tpcds.store_sales, redis.customer_demographics.customer_demographics, cassandra.tpcds.date_dim, mongodb.tpcds.store, mongodb.tpcds.item
 where ss_sold_date_sk = d_date_sk and
       ss_item_sk = i_item_sk and
       ss_store_sk = s_store_sk and
       ss_cdemo_sk = cd_demo_sk and
       cd_gender = 'M' and
       cd_marital_status = 'S' and
       cd_education_status = 'College' and
       d_year = 2002 and
       s_state in ('TN','SD', 'SD', 'TN', 'SD', 'SD')
 group by rollup (i_item_id, s_state)
 order by i_item_id
         ,s_state
 limit 100;


