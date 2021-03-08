from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="redshift",
                 tables= [], 
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.tables = tables

        
    def execute(self, context):
        self.log.info('Starting DataQualityOperator')
        
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        for table in self.tables:
            
            records = redshift.get_records(f"SELECT COUNT(*) FROM {table}")
            
            if len(records) < 1 or len(records[0]) < 1:
                self.log.info(f"Data quality check failed. {table} returned no results")
                raise ValueError(f"Data quality check failed. {table} returned no results")
        
        