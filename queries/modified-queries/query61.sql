
select  promotions,total,cast(promotions as decimal(15,4))/cast(total as decimal(15,4))*100
from
  (select sum(ss_ext_sales_price) promotions
   from  mongodb.tpcds.store_sales
        ,mongodb.tpcds.store
        ,redis.promotion.promotion
        ,cassandra.tpcds.date_dim
        ,redis.customer.customer
        ,redis.customer_address.customer_address 
        ,redis.item.item
   where ss_sold_date_sk = d_date_sk
   and   ss_store_sk = s_store_sk
   and   ss_promo_sk = p_promo_sk
   and   ss_customer_sk= c_customer_sk
   and   ca_address_sk = c_current_addr_sk
   and   ss_item_sk = i_item_sk 
   and   ca_gmt_offset = -7
   and   i_category = 'Books'
   and   (p_channel_dmail = 'Y' or p_channel_email = 'Y' or p_channel_tv = 'Y')
   and   s_gmt_offset = -7
   and   d_year = 1999
   and   d_moy  = 11) promotional_sales,
  (select sum(ss_ext_sales_price) total
   from  mongodb.tpcds.store_sales
        ,mongodb.tpcds.store
        ,cassandra.tpcds.date_dim
        ,redis.customer.customer
        ,redis.customer_address.customer_address
        ,redis.item.item
   where ss_sold_date_sk = d_date_sk
   and   ss_store_sk = s_store_sk
   and   ss_customer_sk= c_customer_sk
   and   ca_address_sk = c_current_addr_sk
   and   ss_item_sk = i_item_sk
   and   ca_gmt_offset = -7
   and   i_category = 'Books'
   and   s_gmt_offset = -7
   and   d_year = 1999
   and   d_moy  = 11) all_sales
order by promotions, total
limit 100;


