use database data_engineering;

create table if not exists data_engineering.consumer_bronze (
  consumer_id string,
  sex string,
  ethnicity string,
  race string,
  age string,
  raw_file_name string,
  load_timestamp timestamp
) using delta;

create table if not exists data_engineering.purchase_bronze (
  purchase_id string,
  consumer_id string,
  graphed_date string,
  avocado_bunch_id string,
  reporting_year string,
  qa_process string,
  billing_provider_sku string,
  grocery_store_id string,
  price_index string,
  raw_file_name string,
  load_timestamp timestamp
) using delta;

create table if not exists data_engineering.avocado_bronze (
  consumer_id string,
  purchase_id string,
  avocado_bunch_id string,
  plu string,
  ripe_index_when_picked string,
  born_at string,
  picked_at string,
  sold_at string,
  raw_file_name string,
  load_timestamp timestamp
) using delta;

create table if not exists data_engineering.fertilizer_bronze (
  fertilizer_id string,
  purchase_id string,
  consumer_id string,
  type string,
  mg string,
  frequency string,
  raw_file_name string,
  load_timestamp timestamp
) using delta;
