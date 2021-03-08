from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    
    template_fields=("s3_key",)
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        JSON 'auto'
        REGION 'us-west-2'
        TIMEFORMAT AS 'epochmillisecs'
        IGNOREHEADER {}
    """
    

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="redshift",
                 aws_credentials_id="aws_credentials",
                 table="",
                 s3_bucket="udacity-dend",
                 s3_key="",
                 ignore_headers=1,
                 *args,
                 **kwargs):


        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)       
        self.redshift_conn_id=redshift_conn_id
        self.aws_credentials_id=aws_credentials_id
        self.table=table
        self.s3_bucket=s3_bucket
        self.s3_key=s3_key
        self.ignore_headers=ignore_headers
        
        
    def execute(self, context):
        self.log.info('Starting StageToRedshiftOperator')
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        
        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        self.log.info(f"TRUNCATE TABLE {self.table}")
        redshift.run(f"TRUNCATE TABLE {self.table}")
        
        self.log.info(f"Copying data from s3 to {self.table}")
        rendered_key = self.s3_key.format(**context)
        s3_path = f"s3://{self.s3_bucket}/{rendered_key}"
        
        sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.ignore_headers,
        )

        redshift.run(sql)
