import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession, DataFrame
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from datetime import datetime, timedelta, date
from pyspark.sql.functions import col, when, regexp_replace, size


def read_json_from_s3(spark: SparkSession, source_s3_path: str, start_date: date, end_date: date) -> DataFrame:
    no_of_days = (end_date - start_date).days + 1
    fixture_dates = [start_date + timedelta(i) for i in range(no_of_days)]
    # Only read partitions for the date interval specified.
    filenames = [f'{source_s3_path}/d={fixture_date.strftime("%Y%m%d")}/results.json' for fixture_date in fixture_dates]
    return spark.read.json(filenames)

def schema_check(df: DataFrame) -> None:
    expected_columns = set([
        'atVs', 
        'attnd',
        'competitors',
        'date', 
        'id', 
        'link', 
        'status', 
        'teams', 
        'venue',
        'tableCaption'
    ])
    actual_columns = set(df.columns)
    missing_columns = expected_columns - actual_columns
    extra_columns = actual_columns - expected_columns
    assert missing_columns == set(), f'Missing Columns: {missing_columns}'
    assert extra_columns == set(), f'Extra Columns: {extra_columns}'

def nullability_check(df: DataFrame) -> None:
    non_null_columns = [
            'date',
            'id',
            'teams',
            'status'
    ]
    nulls_check = df.select([col(column_name).isNull().cast('int').alias(column_name) for column_name in non_null_columns]).groupBy().sum()
    null_counts = nulls_check.collect()[0].asDict()
    for column, count in null_counts.items():
        assert count == 0, f'Column {column[4:-1]} cannot have null values'

def type_check(df: DataFrame) -> None:
    expected_types = {
        'atVs': 'struct',
        'attnd': 'string',
        'competitors': 'array',
        'date': 'string',
        'id': 'string',
        'link': 'string',
        'status': 'struct',
        'teams': 'array',
        'venue': 'struct',
        'tableCaption': 'string'
    }
    for column, actual_type in df.dtypes:
        if column in expected_types:
            expected_type = expected_types[column]
            assert actual_type.startswith(expected_type), f'Column {column} must be {expected_type}, not {actual_type}'

def run_transformations(df: DataFrame) -> DataFrame:
    df = df.select(
            '*',
            col('date').cast('date').alias('fixture_date'),
            regexp_replace('attnd', ',', '').cast('int').alias('attendance'),
            col('id').cast('int').alias('espn_match_id'),
            col('tableCaption').alias('league_name'),
            col('venue')['fullName'].alias('venue_name'),
            col('venue')['id'].cast('int').alias('venue_id'),
            col('venue')['address']['city'].alias('city'),
            col('venue')['address']['country'].alias('country'),
            col('status')['detail'].alias('match_status'),
            (when(size(col('teams')) > 1, col('teams').getItem(1)).otherwise(None)).alias('home_team_struct'), 
            (when(size(col('teams')) > 1, col('teams').getItem(0)).otherwise(None)).alias('away_team_struct')
    )
    df = df.select(
            '*',
            col('home_team_struct')['displayName'].alias('home_team'),
            col('home_team_struct')['abbrev'].alias('home_team_abbrev'),
            col('home_team_struct')['score'].alias('home_score'),
            col('home_team_struct')['shortName'].alias('home_team_short'),
            col('home_team_struct')['logo'].alias('home_logo'),
            col('away_team_struct')['displayName'].alias('away_team'),
            col('away_team_struct')['abbrev'].alias('away_team_abbrev'),
            col('away_team_struct')['score'].alias('away_score'),
            col('away_team_struct')['shortName'].alias('away_team_short'),
            col('away_team_struct')['logo'].alias('away_logo'),
            (when(col('match_status') == 'Canceled', 1).when(col('match_status') == 'Postponed', 1).otherwise(0)).alias('flag_cancelled')
        )\
        .withColumn('final_score', when(col('flag_cancelled') == 1, "N/A").otherwise(col('atVs')['atVsText']))\
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
    return df

def write_parquet_to_s3(df: DataFrame, output_s3_path) -> None:
    df = df.repartition(numPartitions=1)
    df.write.mode('overwrite').parquet(output_s3_path)

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

df = read_json_from_s3(
    spark,
    source_s3_path,
    start_date,
    end_date
)
schema_check(df)
type_check(df)
nullability_check(df)
df = run_transformations(df)
write_parquet_to_s3(
    df, 
    output_s3_path
)

job.commit()