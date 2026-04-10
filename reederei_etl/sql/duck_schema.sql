-- Reederei Nord mart (DuckDB). Staging tables are created via read_csv_auto in Python.

CREATE TABLE Vessel (
  imo_number VARCHAR PRIMARY KEY,
  vessel_name VARCHAR NOT NULL,
  vessel_type VARCHAR NOT NULL,
  dwt_mt DOUBLE NOT NULL,
  build_year INTEGER NOT NULL,
  flag_state VARCHAR NOT NULL,
  scrubber_fitted BOOLEAN NOT NULL
);

CREATE TABLE Port (
  port_id INTEGER PRIMARY KEY,
  port_name VARCHAR NOT NULL,
  country VARCHAR,
  region VARCHAR,
  is_virtual_port BOOLEAN NOT NULL DEFAULT false
);

CREATE TABLE Charterer (
  charterer_id INTEGER PRIMARY KEY,
  charterer_name VARCHAR NOT NULL UNIQUE
);

CREATE TABLE Cargo (
  cargo_id INTEGER PRIMARY KEY,
  cargo_grade VARCHAR NOT NULL UNIQUE
);

CREATE TABLE DateDim (
  date_key DATE PRIMARY KEY,
  year INTEGER NOT NULL,
  quarter INTEGER NOT NULL,
  month INTEGER NOT NULL,
  day INTEGER NOT NULL
);

CREATE TABLE Voyage_Leg (
  leg_id BIGINT PRIMARY KEY,
  voyage_id VARCHAR NOT NULL UNIQUE,
  imo_number VARCHAR NOT NULL,
  charterer_id INTEGER NOT NULL,
  cargo_id INTEGER NOT NULL,
  origin_port_id INTEGER NOT NULL,
  destination_port_id INTEGER NOT NULL,
  start_date DATE NOT NULL,
  end_date DATE NOT NULL,
  leg_type VARCHAR NOT NULL,
  sts_transfer BOOLEAN NOT NULL,
  disputed BOOLEAN NOT NULL,
  leg_days DOUBLE NOT NULL,
  allocated_freight_usd DOUBLE NOT NULL,
  bunker_cost_usd DOUBLE NOT NULL,
  port_cost_usd DOUBLE NOT NULL,
  canal_transit_usd DOUBLE NOT NULL,
  demurrage_cost_usd DOUBLE,
  tce DOUBLE,
  est_tce DOUBLE
);
