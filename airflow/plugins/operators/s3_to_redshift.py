from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

class S3ToRedshiftOperator(BaseOperator):
    ui_color = '#8EB6D4'

    @apply_defaults
    def __init__(self,
                 s3_bucket,
                 s3_prefix,
                 table,
                 redshift_conn_id='redshift',
                 aws_conn_id='aws_credentials',
                 copy_options='',
                 *args, **kwargs):
        super(S3ToRedshiftOperator, self).__init__(*args, **kwargs)
        self.s3_bucket = s3_bucket
        self.s3_prefix = s3_prefix
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.aws_conn_id = aws_conn_id
        self.copy_options = copy_options

    def execute(self, context):
        aws_hook = AwsHook("aws_credentials")
        credentials = aws_hook.get_credentials()
        redshift_hook = PostgresHook("redshift")

        self.log.info(f'Preparing to stage data from {self.s3_bucket}/{self.s3_prefix} to {self.table} table...')

        copy_query = f"""
                    COPY {self.table}
                    FROM 's3://{self.s3_bucket}/{self.s3_prefix}'
                    with credentials
                    'aws_access_key_id={credentials.access_key};aws_secret_access_key={credentials.secret_key}'
                    {self.copy_options};
                """

        self.log.info('Executing COPY command...')
        redshift_hook.run(copy_query)
        self.log.info("COPY command complete.")