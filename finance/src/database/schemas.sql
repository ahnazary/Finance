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
    total_revenue bigint,
    free_cash_flow bigint,
    total_assets bigint,
    validity boolean NOT NULL
)

CREATE TABLE stocks.financials (
	ticker varchar(100) NOT NULL,
	insert_date date NOT NULL DEFAULT now(),
	report_date date NOT NULL,
	currency_code varchar(10) NULL,
	frequency varchar(10) NULL,
	"Tax Effect Of Unusual Items" float8 NULL,
	"Tax Rate For Calcs" float8 NULL,
	"Normalized EBITDA" float8 NULL,
	"Net Income From Continuing Operation Net Minority Interest" float8 NULL,
	"Reconciled Depreciation" float8 NULL,
	"Reconciled Cost Of Revenue" float8 NULL,
	"EBIT" float8 NULL,
	"Net Interest Income" float8 NULL,
	"Interest Expense" float8 NULL,
	"Interest Income" float8 NULL,
	"Normalized Income" float8 NULL,
	"Net Income From Continuing And Discontinued Operation" float8 NULL,
	"Total Expenses" float8 NULL,
	"Total Operating Income As Reported" float8 NULL,
	"Diluted Average Shares" float8 NULL,
	"Basic Average Shares" float8 NULL,
	"Diluted EPS" float8 NULL,
	"Basic EPS" float8 NULL,
	"Diluted NI Availto Com Stockholders" float8 NULL,
	"Net Income Common Stockholders" float8 NULL,
	"Net Income" float8 NULL,
	"Net Income Including Noncontrolling Interests" float8 NULL,
	"Net Income Continuous Operations" float8 NULL,
	"Tax Provision" float8 NULL,
	"Pretax Income" float8 NULL,
	"Other Income Expense" float8 NULL,
	"Other Non Operating Income Expenses" float8 NULL,
	"Net Non Operating Interest Income Expense" float8 NULL,
	"Interest Expense Non Operating" float8 NULL,
	"Interest Income Non Operating" float8 NULL,
	"Operating Income" float8 NULL,
	"Operating Expense" float8 NULL,
	"Research And Development" float8 NULL,
	"Selling General And Administration" float8 NULL,
	"Gross Profit" float8 NULL,
	"Cost Of Revenue" float8 NULL,
	"Total Revenue" float8 NULL,
	"Operating Revenue" float8 NULL,
	CONSTRAINT financials_pkey PRIMARY KEY (ticker, report_date)
);

CREATE TABLE stocks.cash_flow (
	ticker varchar(100) NOT NULL,
	insert_date date NOT NULL DEFAULT now(),
	report_date date NOT NULL,
	"Free Cash Flow" float8 NULL,
	"Repurchase Of Capital Stock" float8 NULL,
	"Repayment Of Debt" float8 NULL,
	"Issuance Of Debt" float8 NULL,
	"Issuance Of Capital Stock" float8 NULL,
	"Capital Expenditure" float8 NULL,
	"Interest Paid Supplemental Data" float8 NULL,
	"Income Tax Paid Supplemental Data" float8 NULL,
	"End Cash Position" float8 NULL,
	"Beginning Cash Position" float8 NULL,
	"Changes In Cash" float8 NULL,
	"Financing Cash Flow" float8 NULL,
	"Cash Flow From Continuing Financing Activities" float8 NULL,
	"Net Other Financing Charges" float8 NULL,
	"Cash Dividends Paid" float8 NULL,
	"Common Stock Dividend Paid" float8 NULL,
	"Net Common Stock Issuance" float8 NULL,
	"Common Stock Payments" float8 NULL,
	"Common Stock Issuance" float8 NULL,
	"Net Issuance Payments Of Debt" float8 NULL,
	"Net Short Term Debt Issuance" float8 NULL,
	"Short Term Debt Payments" float8 NULL,
	"Short Term Debt Issuance" float8 NULL,
	"Net Long Term Debt Issuance" float8 NULL,
	"Long Term Debt Payments" float8 NULL,
	"Long Term Debt Issuance" float8 NULL,
	"Investing Cash Flow" float8 NULL,
	"Cash Flow From Continuing Investing Activities" float8 NULL,
	"Net Other Investing Changes" float8 NULL,
	"Net Investment Purchase And Sale" float8 NULL,
	"Sale Of Investment" float8 NULL,
	"Purchase Of Investment" float8 NULL,
	"Net Business Purchase And Sale" float8 NULL,
	"Purchase Of Business" float8 NULL,
	"Net PPE Purchase And Sale" float8 NULL,
	"Purchase Of PPE" float8 NULL,
	"Operating Cash Flow" float8 NULL,
	"Cash Flow From Continuing Operating Activities" float8 NULL,
	"Change In Working Capital" float8 NULL,
	"Change In Other Working Capital" float8 NULL,
	"Change In Other Current Liabilities" float8 NULL,
	"Change In Other Current Assets" float8 NULL,
	"Change In Payables And Accrued Expense" float8 NULL,
	"Change In Payable" float8 NULL,
	"Change In Account Payable" float8 NULL,
	"Change In Inventory" float8 NULL,
	"Change In Receivables" float8 NULL,
	"Changes In Account Receivables" float8 NULL,
	"Other Non Cash Items" float8 NULL,
	"Stock Based Compensation" float8 NULL,
	"Deferred Tax" float8 NULL,
	"Deferred Income Tax" float8 NULL,
	"Depreciation Amortization Depletion" float8 NULL,
	"Depreciation And Amortization" float8 NULL,
	"Net Income From Continuing Operations" float8 NULL,
	currency_code varchar(10) NULL,
	frequency varchar(10) NULL,
	CONSTRAINT cash_flow_pkey PRIMARY KEY (ticker, report_date)
);

CREATE TABLE stocks.balance_sheet (
    ticker varchar(100) PRIMARY KEY,
    insert_date date NOT NULL DEFAULT now(),
    report_date date NOT NULL,
    "Ordinary Shares Number" float,
    "Share Issued" float,
    "Net Debt" float,
    "Total Debt" float,
    "Tangible Book Value" float,
    "Invested Capital" float,
    "Working Capital" float,
    "Net Tangible Assets" float,
    "Common Stock Equity" float,
    "Total Capitalization" float,
    "Total Equity Gross Minority Interest" float,
    "Stockholders Equity" float,
    "Gains Losses Not Affecting Retained Earnings" float,
    "Retained Earnings" float,
    "Capital Stock" float,
    "Common Stock" float,
    "Total Liabilities Net Minority Interest" float,
    "Total Non Current Liabilities Net Minority Interest" float,
    "Other Non Current Liabilities" float,
    "Tradeand Other Payables Non Current" float,
    "Long Term Debt And Capital Lease Obligation" float,
    "Long Term Debt" float,
    "Current Liabilities" float,
    "Other Current Liabilities" float,
    "Current Deferred Liabilities" float,
    "Current Deferred Revenue" float,
    "Current Debt And Capital Lease Obligation" float,
    "Current Debt" float,
    "Other Current Borrowings" float,
    "Commercial Paper" float,
    "Payables And Accrued Expenses" float,
    "Payables" float,
    "Accounts Payable" float,
    "Total Assets" float,
    "Total Non Current Assets" float,
    "Other Non Current Assets" float,
    "Investments And Advances" float,
    "Other Investments" float,
    "Investmentin Financial Assets" float,
    "Available For Sale Securities" float,
    "Net PPE" float,
    "Accumulated Depreciation" float,
    "Gross PPE" float,
    "Leases" float,
    "Machinery Furniture Equipment" float,
    "Land And Improvements" float,
    "Properties" float,
    "Current Assets" float,
    "Other Current Assets" float,
    "Inventory" float,
    "Receivables" float,
    "Other Receivables" float,
    "Accounts Receivable" float,
    "Cash Cash Equivalents And Short Term Investments" float,
    "Other Short Term Investments" float,
    "Cash And Cash Equivalents" float,
    "Cash Equivalents" float,
    "Cash Financial" float
);
