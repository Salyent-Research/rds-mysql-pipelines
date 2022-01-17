# AWS RDS Data Pipeline For Earnings Data
An ETL pipeline for collecting earnings related data from several data providers. The pipeline is hosted on an AWS EC2 instance and is scheduled to run daily via a CRON job. Once the pipeline executes, it loads earnings, daily pricing, and a number of price action indicators into an AWS RDS MySQL database. The python pipeline utilizes SQLAlchemy in conjunction with PyMySQL as the ORM. 

## Background

## Built With
The pipeline is built on these frameworks and platforms:
* AWS: EC2, RDS MySQL Database
* Python
* PyMySQL
* SQLAlchemy
