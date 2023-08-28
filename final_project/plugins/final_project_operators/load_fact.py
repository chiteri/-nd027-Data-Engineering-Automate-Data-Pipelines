from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                redshift_conn_id="",
                table="",
                sql_statement="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id = redshift_conn_id
        self.table=table
        self.sql_statement = sql_statement

    def execute(self, context):
        # self.log.info('LoadFactOperator not implemented yet')
        self.log.info('Loading fact table {}'.format(self.table))
        redshift_hook = PostgresHook(self.redshift_conn_id)
        redshift_hook.run("INSERT INTO {} {}".format(self.table, self.sql_statement))
