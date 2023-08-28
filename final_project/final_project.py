from datetime import datetime, timedelta
import pendulum
import os
from airflow.decorators import dag
from airflow.operators.dummy_operator import DummyOperator
from final_project_operators.stage_redshift import StageToRedshiftOperator
from final_project_operators.load_fact import LoadFactOperator
from final_project_operators.load_dimension import LoadDimensionOperator
from final_project_operators.data_quality import DataQualityOperator
from udacity.common import final_project_sql_statements 
from airflow.operators.postgres_operator import PostgresOperator


default_args = {
    'owner': 'udacity',
    'start_date': pendulum.now(),
    'retries': 3, 
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False,
    'depends_on_past': False,
}

@dag(
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='0 * * * *',
    catchup=False,
)
def final_project():

    start_operator = DummyOperator(task_id='Begin_execution')

    # self.log.info('Creating tables for the DAG')
    create_tables_task = PostgresOperator(
        task_id='create_tables',
        postgres_conn_id='redshift',
        sql='create_tables.sql'
    )

    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='Stage_events',
        table='staging_events',
        redshift_conn_id='redshift',
        aws_credentials_id='aws_credentials',
        s3_bucket='graviton-tachyon',
        s3_key='log-data'
    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='Stage_songs',
        table='staging_songs',
        redshift_conn_id='redshift',
        aws_credentials_id='aws_credentials',
        s3_bucket= 'graviton-tachyon',
        s3_key='song-data'
    )

    load_songplays_table = LoadFactOperator(
        task_id='Load_songplays_fact_table',
        redshift_conn_id='redshift',
        table='songplays', 
        sql_statement=final_project_sql_statements.SqlQueries.songplay_table_insert
    )

    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',
        redshift_conn_id="redshift",
        table='users', 
        sql_statement=final_project_sql_statements.SqlQueries.user_table_insert,
        append_mode = False
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
        redshift_conn_id='redshift',
        table='songs', 
        sql_statement=final_project_sql_statements.SqlQueries.song_table_insert,
        append_mode = False
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
        redshift_conn_id='redshift',
        table='artists', 
        sql_statement=final_project_sql_statements.SqlQueries.artist_table_insert,
        append_mode = False
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
        redshift_conn_id='redshift',
        table='time', 
        sql_statement=final_project_sql_statements.SqlQueries.time_table_insert,
        append_mode = True
    )

    run_quality_checks = DataQualityOperator(
        task_id='Run_data_quality_checks',
        redshift_conn_id='redshift',
        table='songplays', 
        minimum_expected_result=1
    )

    start_operator >> create_tables_task
    create_tables_task >> stage_events_to_redshift
    create_tables_task >> stage_songs_to_redshift

    stage_events_to_redshift >> load_songplays_table
    stage_songs_to_redshift >> load_songplays_table

    load_songplays_table >> load_song_dimension_table >> run_quality_checks
    load_songplays_table >> load_user_dimension_table >> run_quality_checks
    load_songplays_table >> load_artist_dimension_table >> run_quality_checks
    load_songplays_table >> load_time_dimension_table >> run_quality_checks

final_project_dag = final_project()