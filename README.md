# AWS RDS Data Pipeline For Earnings Data
An ETL pipeline for collecting earnings related data from several data providers. The pipeline is hosted on an AWS EC2 instance and is scheduled to run daily via a cron job. Once the pipeline executes, it loads earnings, daily pricing, and a number of price action indicators into an AWS RDS MySQL database. The python pipeline utilizes SQLAlchemy in conjunction with PyMySQL as the ORM. 

## Built With
The pipeline is built on these frameworks and platforms:
* AWS: EC2, RDS MySQL Database
* Python
* PyMySQL
* SQLAlchemy
* Cron

## Data
This pipeline parses through several API endpoints: [Financial Modeling Prep's](https://site.financialmodelingprep.com/developer/docs) earnings calendar and historical data endpoints and [FMP Cloud's](https://fmpcloud.io/documentation) daily technical indicator endpoint. For each upcoming earnings announcement, the pipeline finds the most recent pricing data as well as price action technical indicators and loads it into an RDS instance running MySQL. 

![Untitled Workspace (1)](https://user-images.githubusercontent.com/45079557/150410944-eb8c8e30-ac2d-4f23-bb03-cb5c3f489cfb.png)

## Scheduling 
The ETL pipeline is scheduled to run daily at 9:30 AM Coordinated Universal Time (UTC). This should be more than enough time for all external data providers to refresh their daily endpoints. The scheduling is handled by a cron job that changes directory into the cloned repository, runs the python program, and logs the output into a log file with the following command:

```Shell
30 09 * * * cd ~/rds-mysql-piplines/src && python3 main.py > ~/rds-mysql-pipelines/pipelines.log 2>&1
```

## Future Plans
* Use collected data to train ML model 
