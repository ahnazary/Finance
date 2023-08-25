[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![Imports: isort](https://img.shields.io/badge/%20imports-isort-%231674b1?style=flat&labelColor=ef8336)](https://pycqa.github.io/isort/)



# Finance

For more detailes about the project, please refer to the [documentation](https://ahnazary.github.io/Finance/).

## Description

This project is meant to scrape stocks financial data from yahoo finance for a wide range of companies (mostly US and EU) and store them in a cload based database. Since the databse is private, the database cannot be accessed by the public, but scheduled tasks extracts all the data in the database, converts them into parquet and csv files and stores them in this same repository.