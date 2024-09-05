use database data_engineering;

create schema if not exists data_engineering.silver;

create table if not exists data_engineering.silver.consumer (
  consumer_id long,
  sex string,
  ethnicity string,
  race string,
  age int,
  raw_file_name string,
  load_timestamp timestamp,
  updated_at timestamp
) using delta;

create table if not exists data_engineering.silver.purchase (
  purchase_id long,
  consumer_id long,
  graphed_date date,
  avocado_bunch_id int,
  reporting_year int,
  qa_process string,
  billing_provider_sku long,
  grocery_store_id int,
  price_index int,
  raw_file_name string,
  load_timestamp timestamp,
  updated_at timestamp
) using delta;

create table if not exists data_engineering.silver.avocado (
  purchase_id long,
  consumer_id long,
  avocado_bunch_id int,
  plu int,
  ripe_index_when_picked int,
  born_at date,
  picked_at date,
  sold_at date,
  raw_file_name string,
  load_timestamp timestamp,
  updated_at timestamp
) using delta;

create table if not exists data_engineering.silver.fertilizer (
  fertilizer_id long,
  purchase_id long,
  consumer_id long,
  type string,
  mg int,
  frequency string,
  raw_file_name string,
  load_timestamp timestamp,
  updated_at timestamp
) using delta;
