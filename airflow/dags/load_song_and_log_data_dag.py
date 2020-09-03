from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from operators import S3ToRedshiftOperator, LoadFactOperator, LoadDimensionOperator, DataQualityOperator
from helpers import SqlQueries

default_args = {
    'owner': 'drobim',
    'start_date': datetime.now(),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': 300,
    'email_on_retry': False
}

dag = DAG('load_song_and_log_data',
          default_args=default_args,
          description='Extract Load and Transform Sparkify song and log data from S3 to Redshift',
          schedule_interval='@hourly',
          catchup=False
          )

start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

create_table_staging_events = PostgresOperator(
    task_id='Create_table_staging_events',
    dag=dag,
    postgres_conn_id='redshift',
    sql=SqlQueries.create_table_staging_events
)

create_table_staging_songs = PostgresOperator(
    task_id='Create_table_staging_songs',
    dag=dag,
    postgres_conn_id='redshift',
    sql=SqlQueries.create_table_staging_songs
)

create_table_songplays = PostgresOperator(
    task_id='Create_table_songplays',
    dag=dag,
    postgres_conn_id='redshift',
    sql=SqlQueries.create_table_songplays
)

create_table_artists = PostgresOperator(
    task_id='Create_table_artists',
    dag=dag,
    postgres_conn_id='redshift',
    sql=SqlQueries.create_table_artists
)

create_table_songs = PostgresOperator(
    task_id='Create_table_songs',
    dag=dag,
    postgres_conn_id='redshift',
    sql=SqlQueries.create_table_songs
)

create_table_users = PostgresOperator(
    task_id='Create_table_users',
    dag=dag,
    postgres_conn_id='redshift',
    sql=SqlQueries.create_table_users
)

create_table_time = PostgresOperator(
    task_id='Create_table_time',
    dag=dag,
    postgres_conn_id='redshift',
    sql=SqlQueries.create_table_time
)

schema_created = DummyOperator(task_id='Schema_created', dag=dag)

load_staging_events_table = S3ToRedshiftOperator(
    task_id='Load_staging_events_table',
    dag=dag,
    s3_bucket='udacity-dend',
    s3_prefix='log_data',
    table='staging_events',
    copy_options="JSON 's3://udacity-dend/log_json_path.json'"
)

load_staging_songs_table = S3ToRedshiftOperator(
    task_id='Load_staging_songs_table',
    dag=dag,
    s3_bucket='udacity-dend',
    s3_prefix='song_data',
    table='staging_songs',
    copy_options="FORMAT AS JSON 'auto'"
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    table='songplays',
    select_sql=SqlQueries.insert_songplays_table
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_users_dim_table',
    dag=dag,
    table='users',
    select_sql=SqlQueries.insert_users_table,
    mode='truncate'
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_songs_dim_table',
    dag=dag, table='songs',
    select_sql=SqlQueries.insert_songs_table,
    mode='truncate'
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artists_dim_table',
    dag=dag,
    table='artists',
    select_sql=SqlQueries.insert_artists_table,
    mode='truncate'
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    table='time',
    select_sql=SqlQueries.insert_time_table,
    mode='truncate'
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    check_stmts=[
        {
            'sql': 'SELECT COUNT(*) FROM songplays;',
            'op': 'gt',
            'val': 0
        },
        {
            'sql': 'SELECT COUNT(*) FROM songplays WHERE songid IS NULL;',
            'op': 'eq',
            'val': 0
        }
    ]
)

end_operator = DummyOperator(task_id='Stop_execution', dag=dag)

# DAG dependencies
start_operator >> create_table_staging_songs
start_operator >> create_table_staging_events
start_operator >> create_table_songplays
start_operator >> create_table_artists
start_operator >> create_table_songs
start_operator >> create_table_users
start_operator >> create_table_time

create_table_staging_events >> schema_created
create_table_staging_songs >> schema_created
create_table_songplays >> schema_created
create_table_artists >> schema_created
create_table_songs >> schema_created
create_table_users >> schema_created
create_table_time >> schema_created

schema_created >> load_staging_events_table
schema_created >> load_staging_songs_table

load_staging_events_table >> load_songplays_table
load_staging_songs_table >> load_songplays_table

load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table

load_song_dimension_table >> run_quality_checks
load_user_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks

run_quality_checks >> end_operator