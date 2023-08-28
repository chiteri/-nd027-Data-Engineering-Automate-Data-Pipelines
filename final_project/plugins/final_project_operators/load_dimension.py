from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                redshift_conn_id='',
                table='',
                sql_statement='',
                append_mode = True,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id        
        self.redshift_conn_id = redshift_conn_id
        self.table=table
        self.sql_statement = sql_statement
        self.append_mode = append_mode

    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)

        if not self.append_mode:
            self.log.info('Truncating dimension table {}'.format(self.table))
            redshift_hook.run('TRUNCATE TABLE {} '.format(self.table))

        self.log.info('Loading dimension table {}'.format(self.table))
        redshift_hook.run('INSERT INTO {} {}'.format(self.table, self.sql_statement))
