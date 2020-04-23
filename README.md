# Songplays Data Lake

Sparkify, a music streaming startup, has grown their user base and song database even more and want to move their data warehouse to a data lake.

Here, we are building an ETL pipeline that extracts Sparkify data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables.
 
Songplays Data Lake is a Spark ETL on songplays data created from files with songs data files and logs data files.

etl.py - processes the files and inserts data into Redshift
sql_querys.py - has all the queries used in the project
dl.cfg - A configuration file with AWS key and secret to allow for a connection to S3 to pull song and log data and create parquet files of processed data tables.

## Creation of an S3 bucket

Create an AWS S3 bucket at (https://s3.console.aws.amazon.com/)

## Usage

Edit dl.cfg and add the AWS credentials details.
[AWS]
ACCESS_KEY_ID=<!--ENTER AWS KEY ID HERE-->
SECRET_ACCESS_KEY=<!--ENTER AWS SECRET KEY HERE-->

Edit etl.py to add paths to which s3 bucket to source for data "input_data", and which s3 bucket to save the tables of processed data "output_data"
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://arita-bucket-demo/"

```bash
python etl.py
```

## Contributing
Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.

Please make sure to update tests as appropriate.

## License
[MIT]