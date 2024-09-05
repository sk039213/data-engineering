use database data_engineering;

create schema if not exists data_engineering.gold;

create table if not exists data_engineering.gold.output (
  consumer_id long,
  sex string,
  age int,
  avocado_days_old int,
  avocado_ripe_index int,
  avocado_days_picked int,
  fertilizer_type string,
  updated_at timestamp
) using delta;