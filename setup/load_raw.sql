-- Load AutoElite flat files from GCS into BigQuery raw schema.
-- Run once at the start of class (re-run safe -- uses CREATE OR REPLACE).
--
-- NOTE:  you must have your project set in the shell/terminal or set programmatically
--
-- Usage (project is already set in the cli):
--   bq query --use_legacy_sql=false < setup/load_raw.sql

-- -------------------------------------------------------------------------
-- Schema
-- -------------------------------------------------------------------------

CREATE SCHEMA IF NOT EXISTS autoelite_raw
  OPTIONS (location = 'US');

-- -------------------------------------------------------------------------
-- customers
-- -------------------------------------------------------------------------

CREATE OR REPLACE TABLE autoelite_raw.customers (
  id             STRING,
  first_name     STRING,
  last_name      STRING,
  phone          STRING,
  description    STRING,
  shipping_state STRING,
  person_email   STRING,
  _loaded_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  _source        STRING    DEFAULT 'flat-file'
);

LOAD DATA INTO autoelite_raw.customers (
  id             STRING,
  first_name     STRING,
  last_name      STRING,
  phone          STRING,
  description    STRING,
  shipping_state STRING,
  person_email   STRING
)
FROM FILES (
  format            = 'CSV',
  uris              = ['gs://qst-public/ba882/202609/autoelite/customers.csv'],
  skip_leading_rows = 1
);

-- -------------------------------------------------------------------------
-- reps
-- -------------------------------------------------------------------------

CREATE OR REPLACE TABLE autoelite_raw.reps (
  id                  STRING,
  first_name          STRING,
  last_name           STRING,
  email               STRING,
  phone               STRING,
  username            STRING,
  alias               STRING,
  profile_id          STRING,
  language_locale_key STRING,
  email_encoding_key  STRING,
  time_zone_sid_key   STRING,
  locale_sid_key      STRING,
  _loaded_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  _source             STRING    DEFAULT 'flat-file'
);

LOAD DATA INTO autoelite_raw.reps (
  id                  STRING,
  first_name          STRING,
  last_name           STRING,
  email               STRING,
  phone               STRING,
  username            STRING,
  alias               STRING,
  profile_id          STRING,
  language_locale_key STRING,
  email_encoding_key  STRING,
  time_zone_sid_key   STRING,
  locale_sid_key      STRING
)
FROM FILES (
  format            = 'CSV',
  uris              = ['gs://qst-public/ba882/202609/autoelite/reps.csv'],
  skip_leading_rows = 1
);

-- -------------------------------------------------------------------------
-- products
-- -------------------------------------------------------------------------

CREATE OR REPLACE TABLE autoelite_raw.products (
  id          STRING,
  name        STRING,
  description STRING,
  is_active   BOOL,
  external_id STRING,
  _loaded_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  _source     STRING    DEFAULT 'flat-file'
);

LOAD DATA INTO autoelite_raw.products (
  id          STRING,
  name        STRING,
  description STRING,
  is_active   BOOL,
  external_id STRING
)
FROM FILES (
  format            = 'CSV',
  uris              = ['gs://qst-public/ba882/202609/autoelite/products.csv'],
  skip_leading_rows = 1
);

-- -------------------------------------------------------------------------
-- orders
-- -------------------------------------------------------------------------

CREATE OR REPLACE TABLE autoelite_raw.orders (
  id             STRING,
  account_id     STRING,
  status         STRING,
  effective_date DATE,
  pricebook_id   STRING,
  owner_id       STRING,
  opportunity_id STRING,
  _loaded_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  _source        STRING    DEFAULT 'flat-file'
);

LOAD DATA INTO autoelite_raw.orders (
  id             STRING,
  account_id     STRING,
  status         STRING,
  effective_date DATE,
  pricebook_id   STRING,
  owner_id       STRING,
  opportunity_id STRING
)
FROM FILES (
  format            = 'CSV',
  uris              = ['gs://qst-public/ba882/202609/autoelite/orders.csv'],
  skip_leading_rows = 1
);

-- -------------------------------------------------------------------------
-- order_items
-- -------------------------------------------------------------------------

CREATE OR REPLACE TABLE autoelite_raw.order_items (
  id                 STRING,
  order_id           STRING,
  product_id         STRING,
  quantity           FLOAT64,
  unit_price         FLOAT64,
  pricebook_entry_id STRING,
  _loaded_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  _source            STRING    DEFAULT 'flat-file'
);

LOAD DATA INTO autoelite_raw.order_items (
  id                 STRING,
  order_id           STRING,
  product_id         STRING,
  quantity           FLOAT64,
  unit_price         FLOAT64,
  pricebook_entry_id STRING
)
FROM FILES (
  format            = 'CSV',
  uris              = ['gs://qst-public/ba882/202609/autoelite/order_items.csv'],
  skip_leading_rows = 1
);

-- -------------------------------------------------------------------------
-- deals
-- -------------------------------------------------------------------------

CREATE OR REPLACE TABLE autoelite_raw.deals (
  id           STRING,
  contract_id  STRING,
  account_id   STRING,
  contact_id   STRING,
  owner_id     STRING,
  probability  FLOAT64,
  amount       FLOAT64,
  stage_name   STRING,
  name         STRING,
  description  STRING,
  created_date STRING,
  close_date   DATE,
  _loaded_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  _source      STRING    DEFAULT 'flat-file'
);

LOAD DATA INTO autoelite_raw.deals (
  id           STRING,
  contract_id  STRING,
  account_id   STRING,
  contact_id   STRING,
  owner_id     STRING,
  probability  FLOAT64,
  amount       FLOAT64,
  stage_name   STRING,
  name         STRING,
  description  STRING,
  created_date STRING,
  close_date   DATE
)
FROM FILES (
  format            = 'CSV',
  uris              = ['gs://qst-public/ba882/202609/autoelite/deals.csv'],
  skip_leading_rows = 1
);
