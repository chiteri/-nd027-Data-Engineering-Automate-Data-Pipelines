from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    quality_check_sql_template = """ 
        SELECT COUNT(*) AS no_of_occurences 
        FROM {table};
    """

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                redshift_conn_id = '', 
                table='', 
                minimum_expected_result=0,
                *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.minimum_expected_result = minimum_expected_result
        #self.table_column = table_column

    def execute(self, context):
        # self.log.info('DataQualityOperator not implemented yet')
        self.log.info('Checking for the occurences / counts of records in {}'.format(self.table ))

        redshift_hook = PostgresHook(self.redshift_conn_id)
        records = redshift_hook.get_records(self.quality_check_sql_template.format(table=self.table))

        if len(records) < self.minimum_expected_result or len(records[0]) < self.minimum_expected_result:
            raise ValueError(f'Data quality check failed. {self.table} returned no results')
        num_records = records[0][0]
        if num_records < self.minimum_expected_result:
            raise ValueError(f'Data quality check failed. {self.table} contained 0 rows')         
        self.log.info(f'Data quality on table {self.table} check passed with {records[0][0]} records')
