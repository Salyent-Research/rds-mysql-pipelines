# AWS RDS Data Pipeline For Earnings Data
An ETL pipeline for collecting earnings related data from several data providers. The pipeline is hosted on an AWS EC2 instance and is scheduled to run daily via a cron job. Once the pipeline executes, it loads earnings, daily pricing, and a number of price action indicators into an AWS RDS MySQL database. The python pipeline utilizes SQLAlchemy in conjunction with PyMySQL as the ORM. 

## Data
This pipeline parses through several API endpoints: [Financial Modeling Prep's](https://site.financialmodelingprep.com/developer/docs) earnings calendar and historical data endpoints and [FMP Cloud's](https://fmpcloud.io/documentation) daily technical indicator endpoint. For each upcoming earnings, the pipeline finds the most recent pricing data as well as price action technical indicators and loads it into an RDS instance running MySQL. 

## Built With
The pipeline is built on these frameworks and platforms:
* AWS: EC2, RDS MySQL Database
* Python
* PyMySQL
* SQLAlchemy
* Cron
