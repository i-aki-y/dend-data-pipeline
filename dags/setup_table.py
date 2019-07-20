import datetime
import logging

from airflow import DAG
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook

from airflow.operators import (
    PostgresOperator,
    PythonOperator,
)

from helpers import SqlQueries


dag = DAG(
    "reset_tables_dag",
    start_date=datetime.datetime(2018, 1, 1, 0, 0, 0, 0),
    schedule_interval=None,
    max_active_runs=1
)


drop_staging_events = PostgresOperator(
    task_id="staging_events_table_drop",
    dag=dag,
    postgres_conn_id="redshift",
    sql=SqlQueries.staging_events_table_drop
)

drop_staging_songs = PostgresOperator(
    task_id="staging_songs_table_drop",
    dag=dag,
    postgres_conn_id="redshift",
    sql=SqlQueries.staging_songs_table_drop
)

drop_user = PostgresOperator(
    task_id="user_table_drop",
    dag=dag,
    postgres_conn_id="redshift",
    sql=SqlQueries.user_table_drop
)

drop_song = PostgresOperator(
    task_id="song_table_drop",
    dag=dag,
    postgres_conn_id="redshift",
    sql=SqlQueries.song_table_drop
)

drop_artist = PostgresOperator(
    task_id="artist_table_drop",
    dag=dag,
    postgres_conn_id="redshift",
    sql=SqlQueries.artist_table_drop
)

drop_time = PostgresOperator(
    task_id="time_table_drop",
    dag=dag,
    postgres_conn_id="redshift",
    sql=SqlQueries.time_table_drop
)


drop_songplay = PostgresOperator(
    task_id="songplay_table_drop",
    dag=dag,
    postgres_conn_id="redshift",
    sql=SqlQueries.songplay_table_drop
)


create_staging_events = PostgresOperator(
    task_id="staging_events_table_create",
    dag=dag,
    postgres_conn_id="redshift",
    sql=SqlQueries.staging_events_table_create
)


create_staging_songs = PostgresOperator(
    task_id="staging_songs_table_create",
    dag=dag,
    postgres_conn_id="redshift",
    sql=SqlQueries.staging_songs_table_create
)

create_songplay = PostgresOperator(
    task_id="songplay_table_create",
    dag=dag,
    postgres_conn_id="redshift",
    sql=SqlQueries.songplay_table_create
)

create_user = PostgresOperator(
    task_id="user_table_create",
    dag=dag,
    postgres_conn_id="redshift",
    sql=SqlQueries.user_table_create
)

create_song = PostgresOperator(
    task_id="song_table_create",
    dag=dag,
    postgres_conn_id="redshift",
    sql=SqlQueries.song_table_create
)

create_artist = PostgresOperator(
    task_id="artist_table_create",
    dag=dag,
    postgres_conn_id="redshift",
    sql=SqlQueries.artist_table_create
)

create_time = PostgresOperator(
    task_id="time_table_create",
    dag=dag,
    postgres_conn_id="redshift",
    sql=SqlQueries.time_table_create
)


drop_staging_events >> create_staging_events
drop_staging_songs >> create_staging_songs
drop_user >> create_user
drop_artist >> create_artist
drop_song >> create_song
drop_time >> create_time
drop_songplay >> create_songplay
