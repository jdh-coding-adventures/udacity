from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 table = "",
                 sql_dim_table_insert="",
                 append_data="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.table=table
        self.sql_dim_table_insert=sql_dim_table_insert
        self.append_data=append_data

    def execute(self, context):
        self.log.info('Starting LoadDimensionOperator')
        
         redshift = PostgresHook(postgres_conn_id=redshift_conn_id)
        
        sql_insert = f"""INSERT INTO {self.table} 
                                    {sql_dim_table_insert}
                                    """
        
        
        if append_data == True:
            
            self.log.info(f"Inserting data into {self.table}")
            redshift.run(sql_insert)
            
        else:
            self.log.info(f"Truncating table {self.table}")
            redshift.run(f"TRUNCATE TABLE {self.table}")
            
            self.log.info(f"Inserting data into {self.table}")
            redshift.run(sql_insert)
