from airflow.models import DAG

from airflow_ext.gfw.models import DagFactory
from airflow_ext.gfw.operators.python_operator import ExecutionDateBranchOperator

from datetime import datetime, timedelta


PIPELINE = 'pipe_vms_belize'

#
# PIPE_VMS_BELIZE
#

class PipelineDagFactory(DagFactory):
    """Concrete class to handle the DAG for pipe_vms_belize"""

    def __init__(self, pipeline=PIPELINE, **kwargs):
        """
        Constructs the DAG.

        :@param pipeline: The pipeline name. Default value the PIPELINE.
        :@type pipeline: str.
        :@param kwargs: A dict of optional parameters.
        :@param kwargs: dict.
        """
        super(PipelineDagFactory, self).__init__(pipeline=pipeline, **kwargs)

    def format_date_sharded_table(self, table, date=None):
        """
        Override of format_date_sharded_table method.

        :@param table: The BigQuery table.
        :@type table: str.
        :@param date: The date sharded. Default None.
        :@type date: str.
        """
        return "{}${}".format(table, date) if date is not None else table

    def build(self, dag_id):
        """
        Override of build method.

        :@param dag_id: The id of the DAG.
        :@type table: str.
        """
        config = self.config
        config['source_paths'] = ','.join(self.source_table_paths())
        config['source_dates'] = ','.join(self.source_date_range())
        config['check_tables'] = self.config.get('source_table') or self.config.get('source_tables')

        with DAG(dag_id, schedule_interval=self.schedule_interval, default_args=self.default_args) as dag:

            source_exists = self.tables_checker(
                retries=2*24*2,                       # Retries 2 days with 30 minutes.
                execution_timeout=timedelta(days=2)   # TimeOut of 2 days.
            )

            fetch_normalized_daily = self.build_docker_task({
                'task_id':'fetch_normalized_daily',
                'pool':'k8operators_limit',
                'docker_run':'{docker_run}'.format(**config),
                'image':'{docker_image}'.format(**config),
                'name':'fetch-normalized-daily',
                'dag':dag,
                'arguments':['fetch_normalized_belize_vms',
                             '{source_dates}'.format(**config),
                             'daily',
                             '{source_paths}'.format(**config),
                             '{project_id}:{pipeline_dataset}.{normalized}'.format(**config)]
            })

            for sensor in source_exists:
                dag >> sensor >> fetch_normalized_daily

        return dag


for mode in ['daily','monthly', 'yearly']:
    dag_id = '{}_{}'.format(PIPELINE, mode)
    globals()[dag_id] = PipelineDagFactory(schedule_interval='@{}'.format(mode)).build(dag_id)

