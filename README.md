# Data Engineering Assignment: Debt Collection ETL and Basic Analysis
### - To start execution of script run python3 pipeline_scripts/etl_script.py
- Script will take few seconds to execute we are using remte RDS MySQL DB hosted on AWS North Verginia region.
- Script will read data from csv and perform required transaformation
- Data will be loaded into RDS MySQL database.
- Logs are available on basis of day

### - Anslysis script contains sql queries
### - Analysis result contains result of queries

### - Understand folder structure
- dataset: contains csv file
- utils: contains connector scripts and other required scripts
- logs: logs are stored under log directory
- vault: connection credentials are stored under this folder

### - We can use airflow to schedule and monitor etl pipeline
