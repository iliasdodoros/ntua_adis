
select  
  cd_gender,
  cd_marital_status,
  cd_education_status,
  count(*) cnt1,
  cd_purchase_estimate,
  count(*) cnt2,
  cd_credit_rating,
  count(*) cnt3
 from
  redis.customer.customer c,redis.customer_address.customer_address ca,redis.customer_demographics.customer_demographics
 where
  c.c_current_addr_sk = ca.ca_address_sk and
  ca_state in ('CO','IL','MN') and
  cd_demo_sk = c.c_current_cdemo_sk and 
  exists (select *
          from mongodb.tpcds.store_sales,cassandra.tpcds.date_dim
          where c.c_customer_sk = ss_customer_sk and
                ss_sold_date_sk = d_date_sk and
                d_year = 1999 and
                d_moy between 1 and 1+2) and
   (not exists (select *
            from mongodb.tpcds.web_sales,cassandra.tpcds.date_dim
            where c.c_customer_sk = ws_bill_customer_sk and
                  ws_sold_date_sk = d_date_sk and
                  d_year = 1999 and
                  d_moy between 1 and 1+2) and
    not exists (select * 
            from cassandra.tpcds.catalog_sales,cassandra.tpcds.date_dim
            where c.c_customer_sk = cs_ship_customer_sk and
                  cs_sold_date_sk = d_date_sk and
                  d_year = 1999 and
                  d_moy between 1 and 1+2))
 group by cd_gender,
          cd_marital_status,
          cd_education_status,
          cd_purchase_estimate,
          cd_credit_rating
 order by cd_gender,
          cd_marital_status,
          cd_education_status,
          cd_purchase_estimate,
          cd_credit_rating
 limit 100;


