from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers import SqlQueries


class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id="redshift",
                 table="",
                 sql="",
                 is_append_only=False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id = redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql = sql
        self.is_append_only = is_append_only

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if not self.is_append_only:
            self.log.info("Delete current data from %s table", self.table)
            redshift.run("DELETE FROM {}".format(self.table))

        self.log.info("Load %s dimension data", self.table)
        formatted_sql = "INSERT INTO {} ".format(self.table)
        formatted_sql += self.sql
        redshift.run(formatted_sql)
