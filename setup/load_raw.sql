-- Load AutoElite flat files from GCS into BigQuery raw schema.
-- Run once at the start of class 2. 
--
-- NOTE:  you must have your project set in the shell/terminal or set programmatically
--
-- Usage:
--   source .env
--   bq query --project_id=$GCP_PROJECT --use_legacy_sql=false < setup/load_raw.sql

-- -------------------------------------------------------------------------
-- Schema
-- -------------------------------------------------------------------------

CREATE SCHEMA IF NOT EXISTS autoelite_raw
  OPTIONS (location = 'us-central1');

-- -------------------------------------------------------------------------
-- customers
-- -------------------------------------------------------------------------

CREATE OR REPLACE TABLE autoelite_raw.customers (
  id            STRING,
  first_name    STRING,
  last_name     STRING,
  phone         STRING,
  description   STRING,
  shipping_state STRING,
  person_email  STRING,
  _loaded_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  _source       STRING    DEFAULT 'flat-file'
);

LOAD DATA INTO autoelite_raw.customers
  (id, first_name, last_name, phone, description, shipping_state, person_email)
FROM FILES (
  format            = 'CSV',
  uris              = ['gs://qst-public/ba882/202609/autoelite/customers.csv'],
  skip_leading_rows = 1
);

-- -------------------------------------------------------------------------
-- reps
-- -------------------------------------------------------------------------

CREATE OR REPLACE TABLE autoelite_raw.reps (
  id                   STRING,
  first_name           STRING,
  last_name            STRING,
  email                STRING,
  phone                STRING,
  username             STRING,
  alias                STRING,
  profile_id           STRING,
  language_locale_key  STRING,
  email_encoding_key   STRING,
  time_zone_sid_key    STRING,
  locale_sid_key       STRING,
  _loaded_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  _source              STRING    DEFAULT 'flat-file'
);

LOAD DATA INTO autoelite_raw.reps
  (id, first_name, last_name, email, phone, username, alias, profile_id, language_locale_key, email_encoding_key, time_zone_sid_key, locale_sid_key)
FROM FILES (
  format            = 'CSV',
  uris              = ['gs://qst-public/ba882/202609/autoelite/reps.csv'],
  skip_leading_rows = 1
);

-- -------------------------------------------------------------------------
-- products
-- -------------------------------------------------------------------------

CREATE OR REPLACE TABLE autoelite_raw.products (
  id             STRING,
  name           STRING,
  description    STRING,
  is_active      BOOL,
  external_id    STRING,
  _loaded_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  _source        STRING    DEFAULT 'flat-file'
);

LOAD DATA INTO autoelite_raw.products
  (id, name, description, is_active, external_id)
FROM FILES (
  format            = 'CSV',
  uris              = ['gs://qst-public/ba882/202609/autoelite/products.csv'],
  skip_leading_rows = 1
);

-- -------------------------------------------------------------------------
-- orders
-- -------------------------------------------------------------------------

CREATE OR REPLACE TABLE autoelite_raw.orders (
  id               STRING,
  account_id       STRING,
  status           STRING,
  effective_date   DATE,
  pricebook_id     STRING,
  owner_id         STRING,
  opportunity_id   STRING,
  _loaded_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  _source          STRING    DEFAULT 'flat-file'
);

LOAD DATA INTO autoelite_raw.orders
  (id, account_id, status, effective_date, pricebook_id, owner_id, opportunity_id)
FROM FILES (
  format            = 'CSV',
  uris              = ['gs://qst-public/ba882/202609/autoelite/orders.csv'],
  skip_leading_rows = 1
);

-- -------------------------------------------------------------------------
-- order_items
-- -------------------------------------------------------------------------

CREATE OR REPLACE TABLE autoelite_raw.order_items (
  id                STRING,
  order_id          STRING,
  product_id        STRING,
  quantity          FLOAT64,
  unit_price        FLOAT64,
  pricebook_entry_id STRING,
  _loaded_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  _source           STRING    DEFAULT 'flat-file'
);

LOAD DATA INTO autoelite_raw.order_items
  (id, order_id, product_id, quantity, unit_price, pricebook_entry_id)
FROM FILES (
  format            = 'CSV',
  uris              = ['gs://qst-public/ba882/202609/autoelite/order_items.csv'],
  skip_leading_rows = 1
);
