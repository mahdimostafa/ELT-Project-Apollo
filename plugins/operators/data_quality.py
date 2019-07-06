import logging
import os
import sys
from airflow.hooks import PostgresHook
from airflow.models import BaseOperator
from airflow.utils import apply_defaults


class PostgresOperator(BaseOperator):


    template_fields = ('sql',)
    template_ext = ('.sql',)
    ui_color = '#ededed'

    @apply_defaults
    def __init__(
            self,
            sql,

            table="public.new",
            postgres_conn_id='postgres_default',
            database=None,
            autocommit=False,
            *args, **kwargs):
        super(PostgresOperator, self).__init__(*args, **kwargs)

        # self.sql = sql
        self.postgres_conn_id = postgres_conn_id
        self.autocommit = autocommit
        self.table = table
        self.database = database
        self.sql = sql


    def execute(self, context):
        self.log.info('Executing: %s', self.sql)
        self.hook = PostgresHook(postgres_conn_id=self.postgres_conn_id,
                                 schema=self.database)
        self.hook.run(self.sql, self.autocommit)
        for output in self.hook.conn.notices:
            self.log.info(output)
        logging.info('Executing: ' + self.sql)
        self.hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        self.hook.run(self.sql, self.autocommit)
        records = self.hook.get_records(f"SELECT COUNT(*) FROM {self.table}")
        if len(records) < 1 or len(records[0]) < 1:
            raise ValueError(f"Data quality check failed. {self.table} returned no results")
        num_records = records[0][0]
        if num_records < 1:
            raise ValueError(f"Data quality check failed. {self.table} contained 0 rows")
        self.log.info(f'Data quality on table {self.table} check passed with {records[0][0]} records')
