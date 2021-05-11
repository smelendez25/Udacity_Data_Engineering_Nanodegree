from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    """
    Runs data quality check by passing test SQL query and expected result
    
    :param redshift_conn_id: Redshift connection ID
    :param test_query: SQL query to run on Redshift data warehouse
    :param expected_result: Expected result to match against result of
        test_query
    """
    
    ui_color = '#89DA59'
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 test_query="",
                 expected_result="",
                 *args, **kwargs):
        
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.test_query = test_query
        self.expected_result = expected_result
    
    def execute(self, context):
        
        self.log.info("Getting credentials")
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info("Running test")
        records = redshift_hook.get_records(self.test_query)
        if records[0][0] != self.expected_result:
            raise ValueError(f"""
                Data quality check failed. \
                {records[0][0]} does not equal {self.expected_result}
            """)
        else:
            self.log.info("Data quality check passed")