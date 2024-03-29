from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    template_fields = ('s3_key',)

    staging_copy_template = ("""
    copy {0}
    from '{1}'
    ACCESS_KEY_ID '{2}'
    SECRET_ACCESS_KEY '{3}'
    REGION '{4}'
    """)

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # redshift_conn_id=your-connection-name
                 redshift_conn_id="",
                 aws_credentials_id="",
                 region="us-west-2",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 data_format="json",
                 ## for csv
                 delimiter=",",
                 quote_char='""',
                 ignore_headers=1,
                 ## for json
                 json_opt="auto",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.aws_credentials_id = aws_credentials_id
        self.region = region
        self.data_format = data_format
        self.json_opt = json_opt
        self.delimiter = delimiter
        self.quote_char = quote_char
        self.ignore_headers = ignore_headers


    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Clearing data from destination Redshift table")
        redshift.run("DELETE FROM {}".format(self.table))

        self.log.info("Copying data from S3 to Redshift")
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        self.log.info("Load from %s", s3_path)
        formatted_sql = StageToRedshiftOperator.staging_copy_template.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.region,
        )
        if self.data_format == "csv":
            formatted_sql += """
            CSV QUOTE '{0}'
            DELIMITER '{1}'
            IGNOREDEADER {2}
            """.format(self.quote_char,
                       self.delimiter,
                       self.ignore_headers)

        elif self.data_format == "json":
            formatted_sql += """
            json '{0}'
            """.format(self.json_opt)
        else:
            raise ValueError("Unknown format is specified")

        redshift.run(formatted_sql)
