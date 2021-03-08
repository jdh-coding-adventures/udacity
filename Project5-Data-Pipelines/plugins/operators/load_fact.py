from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "redshift",
                 aws_credentials_id="aws_credentials",
                 table = "",
                 sql_songplay_table_insert="",
                 append_data=False,
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.aws_credentials_id=aws_credentials_id
        self.table=table
        self.sql_songplay_table_insert=sql_songplay_table_insert
        self.append_data=append_data

    def execute(self, context):
        self.log.info('Starting LoadFactOperator')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        sql_insert = f"""INSERT INTO {self.table} 
                                    {self.sql_songplay_table_insert}
                                    """
        
        
        if self.append_data == True:
            
            self.log.info(f"Inserting data into {self.table}")
            redshift.run(sql_insert)
            
        else:
            self.log.info(f"Truncating table {self.table}")
            redshift.run(f"TRUNCATE TABLE {self.table}")
            
            self.log.info(f"Inserting data into {self.table}")
            redshift.run(sql_insert)
        
        
        
