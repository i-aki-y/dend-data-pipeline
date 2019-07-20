from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id="redshift",
                 tables=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id = redshift_conn_id = redshift_conn_id
        self.tables = tables

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        for table in self.tables:
            records = redshift.get_records("SELECT COUNT(*) FROM {}".format(table))

            if records is None or len(records[0]) < 1:
                self.log.error("No records present in table %s", table)
                raise ValueError("No records present in table {}".format(table))

            self.log.info("Data quality on table %s check passed with %s", table, records[0][0])
