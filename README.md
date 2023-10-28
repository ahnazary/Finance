[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![Imports: isort](https://img.shields.io/badge/%20imports-isort-%231674b1?style=flat&labelColor=ef8336)](https://pycqa.github.io/isort/)



# Finance

<img src="docs/Finance github project chart.jpeg" alt="drawing" width="800" height="470" align="middle" style="middle"/>

<br />

For more detailes about the project, please refer to the [documentation](https://ahnazary.github.io/Finance/).

## Description

This project is meant to scrape stocks financial data from yahoo finance for a wide range of companies (mostly US and EU) and store them in a cload based database. Since the databse is private, the database cannot be accessed by the public, but scheduled tasks extracts all the data in the database, converts them into parquet and csv files and stores them in this same repository.

## What problem does it solve?


- Yahoo finance only provides last 4 quarters or yearly financial data for a company. This project solves this problem by scraping the data from yahoo finance every quarter, storing all old records in a database as well as the new ones. This way, the database contains all the financial data for a company since the scraping started.
- Yahoo finance does not provide a way to download all the financial data for a wide range of companies at once. This project solves this problem by scraping the data from yahoo finance and storing them in a postgres database. Access to data is quick through SQL queries.