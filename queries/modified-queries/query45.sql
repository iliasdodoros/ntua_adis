
select  ca_zip, ca_county, sum(ws_sales_price)
 from mongodb.tpcds.web_sales, redis.customer.customer, redis.customer_address.customer_address, cassandra.tpcds.date_dim, mongodb.tpcds.item
 where ws_bill_customer_sk = c_customer_sk
 	and c_current_addr_sk = ca_address_sk 
 	and ws_item_sk = i_item_sk 
 	and ( substr(ca_zip,1,5) in ('85669', '86197','88274','83405','86475', '85392', '85460', '80348', '81792')
 	      or 
 	      i_item_id in (select i_item_id
                             from mongodb.tpcds.item
                             where i_item_sk in (2, 3, 5, 7, 11, 13, 17, 19, 23, 29)
                             )
 	    )
 	and ws_sold_date_sk = d_date_sk
 	and d_qoy = 2 and d_year = 2000
 group by ca_zip, ca_county
 order by ca_zip, ca_county
 limit 100;

