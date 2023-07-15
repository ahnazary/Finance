-- This file contains the SQL code to create the database schema and tables

CREATE SCHEMA IF NOT EXISTS stocks;

CREATE TABLE IF NOT EXISTS stocks.tickers_list(
    ticker varchar(10) NOT NULL PRIMARY KEY,
    name varchar(100),
    exchange varchar(10),
    category_name varchar(100),
    country varchar(100)
)

CREATE TABLE IF NOT EXISTS stocks.valid_tickers(
    ticker varchar(10) NOT NULL PRIMARY KEY,
    date date NOT NULL DEFAULT now(),
    currency_code varchar(10),
    market_cap bigint,
    currency varchar(10),
    total_revenue bigint,
    free_cash_flow bigint,
    total_assets bigint,
    validity boolean NOT NULL
)