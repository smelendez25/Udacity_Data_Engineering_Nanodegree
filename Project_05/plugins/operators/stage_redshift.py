  
from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    
    template_fields = ("s3_key",)
    copy_sql = """
        COPY {table}
        FROM '{s3_path}'
        ACCESS_KEY_ID '{key_id}'
        SECRET_ACCESS_KEY '{access_key}'
        FORMAT AS {file_format} 
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",                 
                 s3_bucket="",
                 s3_key="",
                 file_format="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.aws_credentials_id=aws_credentials_id
        self.table=table
        self.s3_bucket=s3_bucket
        self.s3_key=s3_key
        self.file_format=file_format

    def execute(self, context):
        try:
            aws_hook = AwsHook(self.aws_credentials_id)
            credentials = aws_hook.get_credentials()
            self.log.info("AWS Hook Defined")
            redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
            self.log.info("Redshift Hook Defined")
        except AirflowError as e:
            self.log.error(e)
        
        # Clearing data from staging table
        self.log.info(f"Clearing data from {self.table}")
        redshift.run(f"DELETE FROM {self.table}")
        
        # Rendering S3 according to the formated values given
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        
        # Copying data from S3 to Redshift
        self.log.info(f"Copying data from {s3_path} to Redshift")
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            table=self.table,
            s3_path=s3_path,
            key_id=credentials.access_key,
            access_key=credentials.secret_key,
            file_format=self.file_format
        )
        redshift.run(formatted_sql)
