import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from datetime import datetime, timedelta
from pyspark.sql.functions import col, when


## This is the AWS Glue script which we submit parameters to with the GlueJobOperator in Airflow 
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'BUCKET_NAME', 'START_DATE', 'END_DATE'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

start_date = datetime.strptime(args['START_DATE'], '%Y-%m-%d')
end_date = datetime.strptime(args['END_DATE'], '%Y-%m-%d')
source_s3_path = f's3://{args["BUCKET_NAME"]}/football_results_etl'
output_s3_path = source_s3_path.replace('-raw-', '-transformed-') + f'/weekstarting={start_date.strftime("%Y%m%d")}'

no_of_days = (end_date - start_date).days + 1
fixture_dates = [start_date + timedelta(i) for i in range(no_of_days)]
# Only read partitions for the date interval specified.
filenames = [f'{source_s3_path}/d={fixture_date.strftime("%Y%m%d")}/results.json' for fixture_date in fixture_dates]

df = spark.read.json(filenames)
df = df.withColumn('fixture_date', col('date').cast('date'))\
       .withColumn('attendance', col('attnd').cast('int'))\
       .withColumn('espn_match_id', col('id').cast('int'))\
       .withColumn('home_team_struct', col('teams').getItem(0))\
       .withColumn('away_team_struct', col('teams').getItem(1))\
       .withColumn('home_team', col('home_team_struct')['displayName'])\
       .withColumn('home_team_abbrev', col('home_team_struct')['abbrev'])\
       .withColumn('home_score', col('home_team_struct')['score'])\
       .withColumn('home_team_short', col('home_team_struct')['shortName'])\
       .withColumn('home_logo', col('home_team_struct')['logo'])\
       .withColumn('away_team', col('away_team_struct')['displayName'])\
       .withColumn('away_team_abbrev', col('away_team_struct')['abbrev'])\
       .withColumn('away_score', col('away_team_struct')['score'])\
       .withColumn('away_team_short', col('away_team_struct')['shortName'])\
       .withColumn('away_logo', col('away_team_struct')['logo'])\
       .withColumn('league_name', col('tableCaption'))\
       .withColumn('venue_name', col('venue')['fullName'])\
       .withColumn('venue_id', col('venue')['id'].cast('int'))\
       .withColumn('city', col('venue')['address']['city'])\
       .withColumn('country', col('venue')['address']['country'])\
       .withColumn('match_status', col('status')['detail'])\
       .withColumn('flag_cancelled', when(col('match_status') == 'Canceled', 1).when(col('match_status') == 'Postponed', 1).otherwise(0))\
       .withColumn('final_score', when(col('flag_cancelled') == 0, None).otherwise(col('atVs')['atVsText']))\
       .select([
           'fixture_date', 
           'attendance', 
           'final_score', 
           'espn_match_id', 
           'home_team',
           'home_team_abbrev',
           'home_score',
           'home_team_short',
           'home_logo',
           'away_team',
           'away_team_abbrev',
           'away_score',
           'away_team_short',
           'away_logo',
           'league_name',
           'venue_name',
           'venue_id',
           'city',
           'country',
           'match_status',
           'flag_cancelled'
           ])
df.write.mode('overwrite').parquet(output_s3_path)

job.commit()