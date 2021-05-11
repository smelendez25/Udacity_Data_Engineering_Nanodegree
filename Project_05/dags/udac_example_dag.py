import os
from pathlib import Path
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                               LoadDimensionOperator, DataQualityOperator)

from helpers import SqlQueries

# Global Variables
TABLES = ["staging_events", "staging_songs", "users", "songs",
          "artists", "time", "songplays"]

# Convert SQL commands from create_tables file into dictionary of commands
sql_commands = {}
filename = os.path.join(Path(__file__).parents[1], 'create_tables.sql')
with open(filename, 'r') as sqlfile:
    commands = s = " ".join(sqlfile.readlines())
    for idx, sql_stmt in enumerate(commands.split(';')[:-1]):
        table = sql_stmt.split('.')[-1].split(' ')[0].strip('"').strip('\n')
        sql_commands[table] = sql_stmt


default_args = {
    'owner': 'Steven_Melendez',
    'depends_on_past': False,
    'catchup': False,
    'start_date': datetime(2019, 1, 12),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@hourly',
          catchup=False
          )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)
load_staging_tables = DummyOperator(task_id='load_staging_tables',  dag=dag)
end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

# Drop & Create Tables
for table in TABLES:
    # Drop Table
    drop_table_task = PostgresOperator(
        task_id=f"drop_{table}",
        postgres_conn_id="redshift",
        sql=f"DROP table IF EXISTS {table}",
        dag=dag
    )
    
    # Create Table
    create_table_task = PostgresOperator(
        task_id=f"create_{table}",
        postgres_conn_id="redshift",
        sql=sql_commands[table],
        dag=dag
    )

    start_operator >> drop_table_task
    drop_table_task >> create_table_task
    create_table_task >> load_staging_tables

# Stage Events Data
stage_events_to_redshift = StageToRedshiftOperator(
    task_id='stage_events',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_events",
    s3_bucket="udacity-dend",
    s3_key="log_data/",
    file_format = 'JSON \'s3://udacity-dend/log_json_path.json\'' 
)

# Stage Song Data
stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='stage_songs',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_songs",
    s3_bucket="udacity-dend",
    s3_key="song_data",
    file_format = 'JSON \'auto\''
)

# Insert Fact Tables
load_songplays_table = LoadFactOperator(
    task_id=f'load_songplays',
    redshift_conn_id="redshift",
    table='songplays',
    sql_stmt=SqlQueries.songplay_table_insert,
    dag=dag
)

# Insert DIM Tables
load_users_table = LoadDimensionOperator(
    task_id=f'load_users',
    redshift_conn_id="redshift",
    table='users',
    truncate=True,
    sql_stmt=SqlQueries.user_table_insert,
    dag=dag
)

load_songs_table = LoadDimensionOperator(
    task_id=f'load_songs',
    redshift_conn_id="redshift",
    table='songs',
    truncate=True,
    sql_stmt=SqlQueries.song_table_insert,
    dag=dag
)

load_artists_table = LoadDimensionOperator(
    task_id=f'load_artists',
    redshift_conn_id="redshift",
    table='artists',
    truncate=True,
    sql_stmt=SqlQueries.artist_table_insert,
    dag=dag
)

load_time_table = LoadDimensionOperator(
    task_id=f'load_time',
    redshift_conn_id="redshift",
    table='time',
    truncate=True,
    sql_stmt=SqlQueries.time_table_insert,
    dag=dag
)

# Data Quality Checks
run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    redshift_conn_id='redshift',
    test_query='select count(*) from songs where songid is null;',
    expected_result=0,
    dag=dag
)

load_staging_tables >> [stage_events_to_redshift, stage_songs_to_redshift]
stage_songs_to_redshift >> load_songplays_table
stage_events_to_redshift >> load_songplays_table
load_songplays_table >> load_songs_table
load_songplays_table >> load_artists_table
load_songplays_table >> load_users_table
load_songplays_table >> load_time_table

[load_users_table,
    load_artists_table,
    load_songs_table,
    load_time_table] >> run_quality_checks

run_quality_checks >> end_operator

