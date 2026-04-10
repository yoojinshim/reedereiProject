-- Reederei Nord mart (SQLite). Staging + dimensional model + voyage legs.

PRAGMA foreign_keys = ON;

-- ---------------------------------------------------------------------------
-- Staging (raw-ish loads; Python normalizes types)
-- ---------------------------------------------------------------------------
CREATE TABLE stg_vessel (
  imo_number TEXT NOT NULL,
  vessel_name TEXT,
  vessel_type TEXT,
  dwt_mt REAL,
  build_year INTEGER,
  flag_state TEXT,
  scrubber_fitted TEXT
);

CREATE TABLE stg_voyage (
  voyage_id TEXT NOT NULL,
  imo_number TEXT NOT NULL,
  vessel_name TEXT,
  vessel_type TEXT,
  cp_date TEXT,
  laycan_start TEXT,
  laycan_end TEXT,
  actual_load_date TEXT,
  actual_discharge_date TEXT,
  load_port TEXT,
  load_country TEXT,
  load_region TEXT,
  discharge_port TEXT,
  discharge_country TEXT,
  discharge_region TEXT,
  cargo_grade TEXT,
  cargo_qty_mt REAL,
  charterer TEXT,
  sts_transfer TEXT,
  ballast_origin TEXT,
  laden_days REAL,
  ballast_days REAL
);

CREATE TABLE stg_worldscale_flat_rate (
  load_port TEXT NOT NULL,
  discharge_port TEXT NOT NULL,
  month TEXT NOT NULL,
  flat_rate_usd_per_mt REAL NOT NULL
);

CREATE TABLE stg_freight_invoice (
  invoice_id TEXT NOT NULL,
  voyage_id TEXT NOT NULL,
  imo_number TEXT NOT NULL,
  charterer TEXT,
  worldscale_points REAL,
  flat_rate_usd_per_mt REAL,
  cargo_qty_mt REAL,
  overage_qty_mt REAL,
  gross_freight_usd REAL,
  currency TEXT,
  invoice_date TEXT,
  payment_status TEXT,
  payment_date TEXT
);

CREATE TABLE stg_port_cost (
  port_call_id TEXT NOT NULL,
  voyage_id TEXT NOT NULL,
  port TEXT,
  port_type TEXT NOT NULL,
  call_date TEXT,
  agency_fee REAL,
  pilotage REAL,
  towage REAL,
  port_dues REAL,
  mooring REAL,
  canal_transit REAL,
  currency TEXT NOT NULL
);

-- Line total in USD (static FX); used for QA and mart allocation.
CREATE TABLE stg_port_cost_usd (
  port_call_id TEXT PRIMARY KEY,
  voyage_id TEXT NOT NULL,
  line_usd REAL NOT NULL
);

CREATE TABLE stg_bunker_stem (
  stem_id TEXT NOT NULL,
  voyage_id TEXT NOT NULL,
  imo_number TEXT NOT NULL,
  bunker_port TEXT,
  stem_date TEXT,
  grade TEXT,
  quantity_mt REAL,
  price_usd_per_mt REAL,
  total_cost_usd REAL,
  supplier TEXT,
  sts_stem TEXT
);

CREATE TABLE stg_laytime_statement (
  laytime_id TEXT NOT NULL,
  voyage_id TEXT NOT NULL,
  imo_number TEXT NOT NULL,
  charterer TEXT,
  nor_tendered TEXT,
  commencement TEXT,
  allowed_hours REAL,
  used_hours REAL,
  net_hours REAL,
  statement_type TEXT,
  demurrage_rate_usd_day REAL,
  amount_usd REAL,
  disputed TEXT,
  counterparty TEXT
);

CREATE TABLE stg_broker_message (
  line_no INTEGER NOT NULL,
  message_id TEXT NOT NULL,
  received_at TEXT NOT NULL,
  from_addr TEXT NOT NULL,
  subject TEXT NOT NULL,
  body TEXT NOT NULL
);

CREATE TABLE stg_excel_open_position (
  row_no INTEGER NOT NULL,
  vessel TEXT,
  voy_ref TEXT,
  charterer TEXT,
  load_port TEXT,
  disch_port TEXT,
  grade TEXT,
  qty_mt REAL,
  ws_rate REAL,
  est_tce_usd_day REAL,
  status TEXT,
  notes TEXT
);

CREATE TABLE stg_excel_demurrage_claim (
  row_no INTEGER NOT NULL,
  voy_ref TEXT,
  vessel TEXT,
  counterparty TEXT,
  claim_type TEXT,
  claimed_usd REAL,
  agreed_usd REAL,
  status TEXT,
  days_outstanding REAL
);

CREATE TABLE stg_excel_bunker_budget (
  row_no INTEGER NOT NULL,
  voyage_id TEXT,
  vessel TEXT,
  grade TEXT,
  budget_mt REAL,
  budget_usd_per_mt REAL,
  actual_mt REAL,
  actual_usd_per_mt REAL,
  variance_usd REAL
);

-- ---------------------------------------------------------------------------
-- Mart
-- ---------------------------------------------------------------------------
CREATE TABLE Vessel (
  imo_number TEXT PRIMARY KEY,
  vessel_name TEXT NOT NULL,
  vessel_type TEXT NOT NULL,
  dwt_mt REAL NOT NULL,
  build_year INTEGER NOT NULL,
  flag_state TEXT NOT NULL,
  scrubber_fitted INTEGER NOT NULL CHECK (scrubber_fitted IN (0, 1))
);

CREATE TABLE Port (
  port_id INTEGER PRIMARY KEY,
  port_name TEXT NOT NULL,
  country TEXT,
  region TEXT,
  is_virtual_port INTEGER NOT NULL DEFAULT 0 CHECK (is_virtual_port IN (0, 1)),
  UNIQUE (port_name, country, region)
);

CREATE TABLE Charterer (
  charterer_id INTEGER PRIMARY KEY,
  charterer_name TEXT NOT NULL UNIQUE
);

CREATE TABLE Cargo (
  cargo_id INTEGER PRIMARY KEY,
  cargo_grade TEXT NOT NULL UNIQUE
);

CREATE TABLE DateDim (
  date_key TEXT PRIMARY KEY,
  year INTEGER NOT NULL,
  quarter INTEGER NOT NULL,
  month INTEGER NOT NULL,
  day INTEGER NOT NULL
);

CREATE TABLE Voyage_Leg (
  leg_id INTEGER PRIMARY KEY,
  voyage_id TEXT NOT NULL,
  imo_number TEXT NOT NULL,
  charterer_id INTEGER NOT NULL,
  cargo_id INTEGER NOT NULL,
  original_port_id INTEGER NOT NULL,
  destination_port_id INTEGER NOT NULL,
  start_date TEXT NOT NULL,
  end_date TEXT NOT NULL,
  leg_type TEXT NOT NULL CHECK (leg_type IN ('Laden', 'Ballast')),
  sts_transfer INTEGER NOT NULL CHECK (sts_transfer IN (0, 1)),
  leg_days REAL NOT NULL,
  allocated_freight_usd REAL NOT NULL,
  bunker_cost_usd REAL NOT NULL,
  port_cost_usd REAL NOT NULL,
  demurrage_amount_usd REAL,
  tce REAL,
  est_tce REAL,
  FOREIGN KEY (imo_number) REFERENCES Vessel(imo_number),
  FOREIGN KEY (charterer_id) REFERENCES Charterer(charterer_id),
  FOREIGN KEY (cargo_id) REFERENCES Cargo(cargo_id),
  FOREIGN KEY (original_port_id) REFERENCES Port(port_id),
  FOREIGN KEY (destination_port_id) REFERENCES Port(port_id),
  FOREIGN KEY (start_date) REFERENCES DateDim(date_key),
  FOREIGN KEY (end_date) REFERENCES DateDim(date_key),
  UNIQUE (voyage_id)
);

CREATE INDEX idx_voyage_leg_voyage_id ON Voyage_Leg(voyage_id);
