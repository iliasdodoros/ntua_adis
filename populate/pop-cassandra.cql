CREATE TABLE tpcds.dbgen_version (
    dv_version TEXT,
    dv_create_date DATE,
    dv_create_time TIME,
    dv_cmdline_args TEXT,
    PRIMARY KEY (dv_version)
);

CREATE TABLE tpcds.customer_address (
    ca_address_sk INT PRIMARY KEY,
    ca_address_id TEXT,
    ca_street_number TEXT,
    ca_street_name TEXT,
    ca_street_type TEXT,
    ca_suite_number TEXT,
    ca_city TEXT,
    ca_county TEXT,
    ca_state TEXT,
    ca_zip TEXT,
    ca_country TEXT,
    ca_gmt_offset DECIMAL,
    ca_location_type TEXT
);

CREATE TABLE tpcds.customer_demographics (
    cd_demo_sk INT PRIMARY KEY,
    cd_gender TEXT,
    cd_marital_status TEXT,
    cd_education_status TEXT,
    cd_purchase_estimate INT,
    cd_credit_rating TEXT,
    cd_dep_count INT,
    cd_dep_employed_count INT,
    cd_dep_college_count INT
);

CREATE TABLE tpcds.date_dim (
    d_date_sk INT PRIMARY KEY,
    d_date_id TEXT,
    d_date DATE,
    d_month_seq INT,
    d_week_seq INT,
    d_quarter_seq INT,
    d_year INT,
    d_dow INT,
    d_moy INT,
    d_dom INT,
    d_qoy INT,
    d_fy_year INT,
    d_fy_quarter_seq INT,
    d_fy_week_seq INT,
    d_day_name TEXT,
    d_quarter_name TEXT,
    d_holiday TEXT,
    d_weekend TEXT,
    d_following_holiday TEXT,
    d_first_dom INT,
    d_last_dom INT,
    d_same_day_ly INT,
    d_same_day_lq INT,
    d_current_day TEXT,
    d_current_week TEXT,
    d_current_month TEXT,
    d_current_quarter TEXT,
    d_current_year TEXT
);

CREATE TABLE tpcds.warehouse (
    w_warehouse_sk INT PRIMARY KEY,
    w_warehouse_id TEXT,
    w_warehouse_name TEXT,
    w_warehouse_sq_ft INT,
    w_street_number TEXT,
    w_street_name TEXT,
    w_street_type TEXT,
    w_suite_number TEXT,
    w_city TEXT,
    w_county TEXT,
    w_state TEXT,
    w_zip TEXT,
    w_country TEXT,
    w_gmt_offset DECIMAL
);

CREATE TABLE tpcds.ship_mode (
    sm_ship_mode_sk INT PRIMARY KEY,
    sm_ship_mode_id TEXT,
    sm_type TEXT,
    sm_code TEXT,
    sm_carrier TEXT,
    sm_contract TEXT
);

CREATE TABLE tpcds.time_dim (
    t_time_sk INT PRIMARY KEY,
    t_time_id TEXT,
    t_time INT,
    t_hour INT,
    t_minute INT,
    t_second INT,
    t_am_pm TEXT,
    t_shift TEXT,
    t_sub_shift TEXT,
    t_meal_time TEXT
);

CREATE TABLE tpcds.reason (
    r_reason_sk INT PRIMARY KEY,
    r_reason_id TEXT,
    r_reason_desc TEXT
);

CREATE TABLE tpcds.income_band (
    ib_income_band_sk INT PRIMARY KEY,
    ib_lower_bound INT,
    ib_upper_bound INT
);

CREATE TABLE tpcds.item (
    i_item_sk INT PRIMARY KEY,
    i_item_id TEXT,
    i_rec_start_date DATE,
    i_rec_end_date DATE,
    i_item_desc VARCHAR,
    i_current_price DECIMAL,
    i_wholesale_cost DECIMAL,
    i_brand_id INT,
    i_brand TEXT,
    i_class_id INT,
    i_class TEXT,
    i_category_id INT,
    i_category TEXT,
    i_manufact_id INT,
    i_manufact TEXT,
    i_size TEXT,
    i_formulation TEXT,
    i_color TEXT,
    i_units TEXT,
    i_container TEXT,
    i_manager_id INT,
    i_product_name TEXT
);

CREATE TABLE tpcds.store (
    s_store_sk INT PRIMARY KEY,
    s_store_id TEXT,
    s_rec_start_date DATE,
    s_rec_end_date DATE,
    s_closed_date_sk INT,
    s_store_name VARCHAR,
    s_number_employees INT,
    s_floor_space INT,
    s_hours TEXT,
    s_manager VARCHAR,
    s_market_id INT,
    s_geography_class VARCHAR,
    s_market_desc VARCHAR,
    s_market_manager VARCHAR,
    s_division_id INT,
    s_division_name VARCHAR,
    s_company_id INT,
    s_company_name VARCHAR,
    s_street_number VARCHAR,
    s_street_name VARCHAR,
    s_street_type TEXT,
    s_suite_number TEXT,
    s_city VARCHAR,
    s_county VARCHAR,
    s_state TEXT,
    s_zip TEXT,
    s_country VARCHAR,
    s_gmt_offset DECIMAL,
    s_tax_percentage DECIMAL
);

CREATE TABLE tpcds.call_center (
    cc_call_center_sk INT PRIMARY KEY,
    cc_call_center_id TEXT,
    cc_rec_start_date DATE,
    cc_rec_end_date DATE,
    cc_closed_date_sk INT,
    cc_open_date_sk INT,
    cc_name VARCHAR,
    cc_class VARCHAR,
    cc_employees INT,
    cc_sq_ft INT,
    cc_hours TEXT,
    cc_manager VARCHAR,
    cc_mkt_id INT,
    cc_mkt_class TEXT,
    cc_mkt_desc VARCHAR,
    cc_market_manager VARCHAR,
    cc_division INT,
    cc_division_name VARCHAR,
    cc_company INT,
    cc_company_name TEXT,
    cc_street_number TEXT,
    cc_street_name VARCHAR,
    cc_street_type TEXT,
    cc_suite_number TEXT,
    cc_city VARCHAR,
    cc_county VARCHAR,
    cc_state TEXT,
    cc_zip TEXT,
    cc_country VARCHAR,
    cc_gmt_offset DECIMAL,
    cc_tax_percentage DECIMAL
);

CREATE TABLE tpcds.customer (
    c_customer_sk INT PRIMARY KEY,
    c_customer_id TEXT,
    c_current_cdemo_sk INT,
    c_current_hdemo_sk INT,
    c_current_addr_sk INT,
    c_first_shipto_date_sk INT,
    c_first_sales_date_sk INT,
    c_salutation TEXT,
    c_first_name TEXT,
    c_last_name TEXT,
    c_preferred_cust_flag TEXT,
    c_birth_day INT,
    c_birth_month INT,
    c_birth_year INT,
    c_birth_country VARCHAR,
    c_login TEXT,
    c_email_address TEXT,
    c_last_review_date TEXT
);

CREATE TABLE tpcds.web_site (
    web_site_sk INT PRIMARY KEY,
    web_site_id TEXT,
    web_rec_start_date DATE,
    web_rec_end_date DATE,
    web_name VARCHAR,
    web_open_date_sk INT,
    web_close_date_sk INT,
    web_class VARCHAR,
    web_manager VARCHAR,
    web_mkt_id INT,
    web_mkt_class VARCHAR,
    web_mkt_desc VARCHAR,
    web_market_manager VARCHAR,
    web_company_id INT,
    web_company_name TEXT,
    web_street_number TEXT,
    web_street_name VARCHAR,
    web_street_type TEXT,
    web_suite_number TEXT,
    web_city VARCHAR,
    web_county VARCHAR,
    web_state TEXT,
    web_zip TEXT,
    web_country VARCHAR,
    web_gmt_offset DECIMAL,
    web_tax_percentage DECIMAL
);

CREATE TABLE tpcds.store_returns (
    sr_returned_date_sk INT,
    sr_return_time_sk INT,
    sr_item_sk INT PRIMARY KEY,
    sr_customer_sk INT,
    sr_cdemo_sk INT,
    sr_hdemo_sk INT,
    sr_addr_sk INT,
    sr_store_sk INT,
    sr_reason_sk INT,
    sr_ticket_number INT,
    sr_return_quantity INT,
    sr_return_amt DECIMAL,
    sr_return_tax DECIMAL,
    sr_return_amt_inc_tax DECIMAL,
    sr_fee DECIMAL,
    sr_return_ship_cost DECIMAL,
    sr_refunded_cash DECIMAL,
    sr_reversed_charge DECIMAL,
    sr_store_credit DECIMAL,
    sr_net_loss DECIMAL
);

CREATE TABLE tpcds.household_demographics (
    hd_demo_sk INT PRIMARY KEY,
    hd_income_band_sk INT,
    hd_buy_potential TEXT,
    hd_dep_count INT,
    hd_vehicle_count INT
);

CREATE TABLE tpcds.web_page (
    wp_web_page_sk INT PRIMARY KEY,
    wp_web_page_id TEXT,
    wp_rec_start_date DATE,
    wp_rec_end_date DATE,
    wp_creation_date_sk INT,
    wp_access_date_sk INT,
    wp_autogen_flag TEXT,
    wp_customer_sk INT,
    wp_url VARCHAR,
    wp_type TEXT,
    wp_char_count INT,
    wp_link_count INT,
    wp_image_count INT,
    wp_max_ad_count INT
);

CREATE TABLE tpcds.promotion (
    p_promo_sk INT PRIMARY KEY,
    p_promo_id TEXT,
    p_start_date_sk INT,
    p_end_date_sk INT,
    p_item_sk INT,
    p_cost DECIMAL,
    p_response_target INT,
    p_promo_name TEXT,
    p_channel_dmail TEXT,
    p_channel_email TEXT,
    p_channel_catalog TEXT,
    p_channel_tv TEXT,
    p_channel_radio TEXT,
    p_channel_press TEXT,
    p_channel_event TEXT,
    p_channel_demo TEXT,
    p_channel_details VARCHAR,
    p_purpose TEXT,
    p_discount_active TEXT
);

CREATE TABLE tpcds.catalog_page (
    cp_catalog_page_sk INT PRIMARY KEY,
    cp_catalog_page_id TEXT,
    cp_start_date_sk INT,
    cp_end_date_sk INT,
    cp_department VARCHAR,
    cp_catalog_number INT,
    cp_catalog_page_number INT,
    cp_description VARCHAR,
    cp_type VARCHAR
);

CREATE TABLE tpcds.inventory (
    inv_date_sk INT,
    inv_item_sk INT,
    inv_warehouse_sk INT,
    inv_quantity_on_hand INT,
    PRIMARY KEY (inv_date_sk, inv_item_sk, inv_warehouse_sk)
);

CREATE TABLE tpcds.catalog_returns (
    cr_returned_date_sk INT,
    cr_returned_time_sk INT,
    cr_item_sk INT PRIMARY KEY,
    cr_refunded_customer_sk INT,
    cr_refunded_cdemo_sk INT,
    cr_refunded_hdemo_sk INT,
    cr_refunded_addr_sk INT,
    cr_returning_customer_sk INT,
    cr_returning_cdemo_sk INT,
    cr_returning_hdemo_sk INT,
    cr_returning_addr_sk INT,
    cr_call_center_sk INT,
    cr_catalog_page_sk INT,
    cr_ship_mode_sk INT,
    cr_warehouse_sk INT,
    cr_reason_sk INT,
    cr_order_number INT,
    cr_return_quantity INT,
    cr_return_amount DECIMAL,
    cr_return_tax DECIMAL,
    cr_return_amt_inc_tax DECIMAL,
    cr_fee DECIMAL,
    cr_return_ship_cost DECIMAL,
    cr_refunded_cash DECIMAL,
    cr_reversed_charge DECIMAL,
    cr_store_credit DECIMAL,
    cr_net_loss DECIMAL
);

CREATE TABLE tpcds.web_returns (
    wr_returned_date_sk INT,
    wr_returned_time_sk INT,
    wr_item_sk INT PRIMARY KEY,
    wr_refunded_customer_sk INT,
    wr_refunded_cdemo_sk INT,
    wr_refunded_hdemo_sk INT,
    wr_refunded_addr_sk INT,
    wr_returning_customer_sk INT,
    wr_returning_cdemo_sk INT,
    wr_returning_hdemo_sk INT,
    wr_returning_addr_sk INT,
    wr_web_page_sk INT,
    wr_reason_sk INT,
    wr_order_number INT,
    wr_return_quantity INT,
    wr_return_amt DECIMAL,
    wr_return_tax DECIMAL,
    wr_return_amt_inc_tax DECIMAL,
    wr_fee DECIMAL,
    wr_return_ship_cost DECIMAL,
    wr_refunded_cash DECIMAL,
    wr_reversed_charge DECIMAL,
    wr_account_credit DECIMAL,
    wr_net_loss DECIMAL
);

CREATE TABLE tpcds.web_sales (
    ws_sold_date_sk INT,
    ws_sold_time_sk INT,
    ws_ship_date_sk INT,
    ws_item_sk INT PRIMARY KEY,
    ws_bill_customer_sk INT,
    ws_bill_cdemo_sk INT,
    ws_bill_hdemo_sk INT,
    ws_bill_addr_sk INT,
    ws_ship_customer_sk INT,
    ws_ship_cdemo_sk INT,
    ws_ship_hdemo_sk INT,
    ws_ship_addr_sk INT,
    ws_web_page_sk INT,
    ws_web_site_sk INT,
    ws_ship_mode_sk INT,
    ws_warehouse_sk INT,
    ws_promo_sk INT,
    ws_order_number INT,
    ws_quantity INT,
    ws_wholesale_cost DECIMAL,
    ws_list_price DECIMAL,
    ws_sales_price DECIMAL,
    ws_ext_discount_amt DECIMAL,
    ws_ext_sales_price DECIMAL,
    ws_ext_wholesale_cost DECIMAL,
    ws_ext_list_price DECIMAL,
    ws_ext_tax DECIMAL,
    ws_coupon_amt DECIMAL,
    ws_ext_ship_cost DECIMAL,
    ws_net_paid DECIMAL,
    ws_net_paid_inc_tax DECIMAL,
    ws_net_paid_inc_ship DECIMAL,
    ws_net_paid_inc_ship_tax DECIMAL,
    ws_net_profit DECIMAL
);

CREATE TABLE tpcds.catalog_sales (
    cs_sold_date_sk INT,
    cs_sold_time_sk INT,
    cs_ship_date_sk INT,
    cs_bill_customer_sk INT,
    cs_bill_cdemo_sk INT,
    cs_bill_hdemo_sk INT,
    cs_bill_addr_sk INT,
    cs_ship_customer_sk INT,
    cs_ship_cdemo_sk INT,
    cs_ship_hdemo_sk INT,
    cs_ship_addr_sk INT,
    cs_call_center_sk INT,
    cs_catalog_page_sk INT,
    cs_ship_mode_sk INT,
    cs_warehouse_sk INT,
    cs_item_sk INT PRIMARY KEY,
    cs_promo_sk INT,
    cs_order_number INT,
    cs_quantity INT,
    cs_wholesale_cost DECIMAL,
    cs_list_price DECIMAL,
    cs_sales_price DECIMAL,
    cs_ext_discount_amt DECIMAL,
    cs_ext_sales_price DECIMAL,
    cs_ext_wholesale_cost DECIMAL,
    cs_ext_list_price DECIMAL,
    cs_ext_tax DECIMAL,
    cs_coupon_amt DECIMAL,
    cs_ext_ship_cost DECIMAL,
    cs_net_paid DECIMAL,
    cs_net_paid_inc_tax DECIMAL,
    cs_net_paid_inc_ship DECIMAL,
    cs_net_paid_inc_ship_tax DECIMAL,
    cs_net_profit DECIMAL
);

CREATE TABLE tpcds.store_sales (
    ss_sold_date_sk INT,
    ss_sold_time_sk INT,
    ss_item_sk INT PRIMARY KEY,
    ss_customer_sk INT,
    ss_cdemo_sk INT,
    ss_hdemo_sk INT,
    ss_addr_sk INT,
    ss_store_sk INT,
    ss_promo_sk INT,
    ss_ticket_number INT,
    ss_quantity INT,
    ss_wholesale_cost DECIMAL,
    ss_list_price DECIMAL,
    ss_sales_price DECIMAL,
    ss_ext_discount_amt DECIMAL,
    ss_ext_sales_price DECIMAL,
    ss_ext_wholesale_cost DECIMAL,
    ss_ext_list_price DECIMAL,
    ss_ext_tax DECIMAL,
    ss_coupon_amt DECIMAL,
    ss_net_paid DECIMAL,
    ss_net_paid_inc_tax DECIMAL,
    ss_net_profit DECIMAL
);


COPY tpcds.dbgen_version(dv_version, dv_create_date, dv_create_time, dv_cmdline_args) FROM '/home/user/data/dbgen_version.dat' WITH DELIMITER='|' AND HEADER=false;

COPY tpcds.customer_address(ca_address_sk, ca_address_id, ca_street_number, ca_street_name, ca_street_type, ca_suite_number, ca_city, ca_county, ca_state, ca_zip, ca_country, ca_gmt_offset, ca_location_type) FROM '/home/user/data/customer_address.dat' WITH DELIMITER='|' AND HEADER=false;

COPY tpcds.customer_demographics(cd_demo_sk, cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating, cd_dep_count, cd_dep_employed_count, cd_dep_college_count) FROM '/home/user/data/customer_demographics.dat' WITH DELIMITER='|' AND HEADER=false;

COPY tpcds.date_dim(d_date_sk, d_date_id, d_date, d_month_seq, d_week_seq, d_quarter_seq, d_year, d_dow, d_moy, d_dom, d_qoy, d_fy_year, d_fy_quarter_seq, d_fy_week_seq, d_day_name, d_quarter_name, d_holiday, d_weekend, d_following_holiday, d_first_dom, d_last_dom, d_same_day_ly, d_same_day_lq, d_current_day, d_current_week, d_current_month, d_current_quarter, d_current_year) FROM '/home/user/data/date_dim.dat' WITH DELIMITER='|' AND HEADER=false;

COPY tpcds.warehouse(w_warehouse_sk, w_warehouse_id, w_warehouse_name, w_warehouse_sq_ft, w_street_number, w_street_name, w_street_type, w_suite_number, w_city, w_county, w_state, w_zip, w_country, w_gmt_offset) FROM '/home/user/data/warehouse.dat' WITH DELIMITER='|' AND HEADER=false;

COPY tpcds.ship_mode(sm_ship_mode_sk, sm_ship_mode_id, sm_type, sm_code, sm_carrier, sm_contract) FROM '/home/user/data/ship_mode.dat' WITH DELIMITER='|' AND HEADER=false;

COPY tpcds.time_dim(t_time_sk, t_time_id, t_time, t_hour, t_minute, t_second, t_am_pm, t_shift, t_sub_shift, t_meal_time) FROM '/home/user/data/time_dim.dat' WITH DELIMITER='|' AND HEADER=false;

COPY tpcds.reason(r_reason_sk, r_reason_id, r_reason_desc) FROM '/home/user/data/reason.dat' WITH DELIMITER='|' AND HEADER=false;

COPY tpcds.income_band(ib_income_band_sk, ib_lower_bound, ib_upper_bound) FROM '/home/user/data/income_band.dat' WITH DELIMITER='|' AND HEADER=false;

COPY tpcds.item(i_item_sk, i_item_id, i_rec_start_date, i_rec_end_date, i_item_desc, i_current_price, i_wholesale_cost, i_brand_id, i_brand, i_class_id, i_class, i_category_id, i_category, i_manufact_id, i_manufact, i_size, i_formulation, i_color, i_units, i_container, i_manager_id, i_product_name) FROM '/home/user/data/item.dat' WITH DELIMITER='|' AND HEADER=false;

COPY tpcds.store(s_store_sk, s_store_id, s_rec_start_date, s_rec_end_date, s_closed_date_sk, s_store_name, s_number_employees, s_floor_space, s_hours, s_manager, s_market_id, s_geography_class, s_market_desc, s_market_manager, s_division_id, s_division_name, s_company_id, s_company_name, s_street_number, s_street_name, s_street_type, s_suite_number, s_city, s_county, s_state, s_zip, s_country, s_gmt_offset, s_tax_precentage) FROM '/home/user/data/store.dat' WITH DELIMITER='|' AND HEADER=false;

COPY tpcds.call_center(cc_call_center_sk, cc_call_center_id, cc_rec_start_date, cc_rec_end_date, cc_closed_date_sk, cc_open_date_sk, cc_name, cc_class, cc_employees, cc_sq_ft, cc_hours, cc_manager, cc_mkt_id, cc_mkt_class, cc_mkt_desc, cc_market_manager, cc_division, cc_division_name, cc_company, cc_company_name, cc_street_number, cc_street_name, cc_street_type, cc_suite_number, cc_city, cc_county, cc_state, cc_zip, cc_country, cc_gmt_offset, cc_tax_percentage) FROM '/home/user/data/call_center.dat' WITH DELIMITER='|' AND HEADER=false;

COPY tpcds.customer(c_customer_sk, c_customer_id, c_current_cdemo_sk, c_current_hdemo_sk, c_current_addr_sk, c_first_shipto_date_sk, c_first_sales_date_sk, c_salutation, c_first_name, c_last_name, c_preferred_cust_flag, c_birth_day, c_birth_month, c_birth_year, c_birth_country, c_login, c_email_address, c_last_review_date) FROM '/home/user/data/customer.dat' WITH DELIMITER='|' AND HEADER=false;

COPY tpcds.web_site(web_site_sk, web_site_id, web_rec_start_date, web_rec_end_date, web_name, web_open_date_sk, web_close_date_sk, web_class, web_manager, web_mkt_id, web_mkt_class, web_mkt_desc, web_market_manager, web_company_id, web_company_name, web_street_number, web_street_name, web_street_type, web_suite_number, web_city, web_county, web_state, web_zip, web_country, web_gmt_offset, web_tax_percentage) FROM '/home/user/data/web_site.dat' WITH DELIMITER='|' AND HEADER=false;

COPY tpcds.store_returns(sr_returned_date_sk, sr_return_time_sk, sr_item_sk, sr_customer_sk, sr_cdemo_sk, sr_hdemo_sk, sr_addr_sk, sr_store_sk, sr_reason_sk, sr_ticket_number, sr_return_quantity, sr_return_amt, sr_return_tax, sr_return_amt_inc_tax, sr_fee, sr_return_ship_cost, sr_refunded_cash, sr_reversed_charge, sr_store_credit, sr_net_loss) FROM '/home/user/data/store_returns.dat' WITH DELIMITER='|' AND HEADER=false;

COPY tpcds.household_demographics(hd_demo_sk, hd_income_band_sk, hd_buy_potential, hd_dep_count, hd_vehicle_count) FROM '/home/user/data/household_demographics.dat' WITH DELIMITER='|' AND HEADER=false;

COPY tpcds.web_page(wp_web_page_sk, wp_web_page_id, wp_rec_start_date, wp_rec_end_date, wp_creation_date_sk, wp_access_date_sk, wp_autogen_flag, wp_customer_sk, wp_url, wp_type, wp_char_count, wp_link_count, wp_image_count, wp_max_ad_count) FROM '/home/user/data/web_page.dat' WITH DELIMITER='|' AND HEADER=false;

COPY tpcds.promotion(p_promo_sk, p_promo_id, p_start_date_sk, p_end_date_sk, p_item_sk, p_cost, p_response_target, p_promo_name, p_channel_dmail, p_channel_email, p_channel_catalog, p_channel_tv, p_channel_radio, p_channel_press, p_channel_event, p_channel_demo, p_channel_details, p_purpose, p_discount_active) FROM '/home/user/data/promotion.dat' WITH DELIMITER='|' AND HEADER=false;

COPY tpcds.catalog_page(cp_catalog_page_sk, cp_catalog_page_id, cp_start_date_sk, cp_end_date_sk, cp_department, cp_catalog_number, cp_catalog_page_number, cp_description, cp_type) FROM '/home/user/data/catalog_page.dat' WITH DELIMITER='|' AND HEADER=false;

COPY tpcds.inventory(inv_date_sk, inv_item_sk, inv_warehouse_sk, inv_quantity_on_hand) FROM '/home/user/data/inventory.dat' WITH DELIMITER='|' AND HEADER=false;

COPY tpcds.catalog_returns(cr_returned_date_sk, cr_returned_time_sk, cr_item_sk, cr_refunded_customer_sk, cr_refunded_cdemo_sk, cr_refunded_hdemo_sk, cr_refunded_addr_sk, cr_returning_customer_sk, cr_returning_cdemo_sk, cr_returning_hdemo_sk, cr_returning_addr_sk, cr_call_center_sk, cr_catalog_page_sk, cr_ship_mode_sk, cr_warehouse_sk, cr_reason_sk, cr_order_number, cr_return_quantity, cr_return_amount, cr_return_tax, cr_return_amt_inc_tax, cr_fee, cr_return_ship_cost, cr_refunded_cash, cr_reversed_charge, cr_store_credit, cr_net_loss) FROM '/home/user/data/catalog_returns.dat' WITH DELIMITER='|' AND HEADER=false;

COPY tpcds.web_returns(wr_returned_date_sk, wr_returned_time_sk, wr_item_sk, wr_refunded_customer_sk, wr_refunded_cdemo_sk, wr_refunded_hdemo_sk, wr_refunded_addr_sk, wr_returning_customer_sk, wr_returning_cdemo_sk, wr_returning_hdemo_sk, wr_returning_addr_sk, wr_web_page_sk, wr_reason_sk, wr_order_number, wr_return_quantity, wr_return_amt, wr_return_tax, wr_return_amt_inc_tax, wr_fee, wr_return_ship_cost, wr_refunded_cash, wr_reversed_charge, wr_account_credit, wr_net_loss) FROM '/home/user/data/web_returns.dat' WITH DELIMITER='|' AND HEADER=false;

COPY tpcds.web_sales(ws_sold_date_sk, ws_sold_time_sk, ws_ship_date_sk, ws_item_sk, ws_bill_customer_sk, ws_bill_cdemo_sk, ws_bill_hdemo_sk, ws_bill_addr_sk, ws_ship_customer_sk, ws_ship_cdemo_sk, ws_ship_hdemo_sk, ws_ship_addr_sk, ws_web_page_sk, ws_web_site_sk, ws_ship_mode_sk, ws_warehouse_sk, ws_promo_sk, ws_order_number, ws_quantity, ws_wholesale_cost, ws_list_price, ws_sales_price, ws_ext_discount_amt, ws_ext_sales_price, ws_ext_wholesale_cost, ws_ext_list_price, ws_ext_tax, ws_coupon_amt, ws_ext_ship_cost, ws_net_paid, ws_net_paid_inc_tax, ws_net_paid_inc_ship, ws_net_paid_inc_ship_tax, ws_net_profit) FROM '/home/user/data/web_sales.dat' WITH DELIMITER='|' AND HEADER=false;

COPY tpcds.catalog_sales(cs_sold_date_sk, cs_sold_time_sk, cs_ship_date_sk, cs_bill_customer_sk, cs_bill_cdemo_sk, cs_bill_hdemo_sk, cs_bill_addr_sk, cs_ship_customer_sk, cs_ship_cdemo_sk, cs_ship_hdemo_sk, cs_ship_addr_sk, cs_call_center_sk, cs_catalog_page_sk, cs_ship_mode_sk, cs_warehouse_sk, cs_item_sk, cs_promo_sk, cs_order_number, cs_quantity, cs_wholesale_cost, cs_list_price, cs_sales_price, cs_ext_discount_amt, cs_ext_sales_price, cs_ext_wholesale_cost, cs_ext_list_price, cs_ext_tax, cs_coupon_amt, cs_ext_ship_cost, cs_net_paid, cs_net_paid_inc_tax, cs_net_paid_inc_ship, cs_net_paid_inc_ship_tax, cs_net_profit) FROM '/home/user/data/catalog_sales.dat' WITH DELIMITER='|' AND HEADER=false;

COPY tpcds.store_sales(ss_sold_date_sk, ss_sold_time_sk, ss_item_sk, ss_customer_sk, ss_cdemo_sk, ss_hdemo_sk, ss_addr_sk, ss_store_sk, ss_promo_sk, ss_ticket_number, ss_quantity, ss_wholesale_cost, ss_list_price, ss_sales_price, ss_ext_discount_amt, ss_ext_sales_price, ss_ext_wholesale_cost, ss_ext_list_price, ss_ext_tax, ss_coupon_amt, ss_net_paid, ss_net_paid_inc_tax, ss_net_profit) FROM '/home/user/data/store_sales.dat' WITH DELIMITER='|' AND HEADER=false;
