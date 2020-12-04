from airflow import DAG

from airflow_ext.gfw.models import DagFactory

from datetime import datetime, timedelta

BELIZE_VMS_SCRAPPER_POOL = 'belize-vms-scrappers'
PIPELINE = 'pipe_vms_belize_api'


class PipeVMSBelizeDagFactory(DagFactory):
    """Concrete class to handle the DAG for pipe_vms_belize_api."""

    def __init__(self, pipeline=PIPELINE, **kwargs):
        """
        Constructs the DAG.

        :@param pipeline: The pipeline name. Default value the PIPELINE.
        :@type pipeline: str.
        :@param kwargs: A dict of optional parameters.
        :@param kwargs: dict.
        """
        super(PipeVMSBelizeDagFactory, self).__init__(pipeline=pipeline, **kwargs)

    def source_date(self):
        """
        Validates that the schedule interval only be in daily mode.

        :raise: A ValueError.
        """
        if self.schedule_interval != '@daily':
            raise ValueError('Unsupported schedule interval {}'.format(self.schedule_interval))

    def build(self, dag_id):
        """
        Override of build method.

        :@param dag_id: The id of the DAG.
        :@type table: str.
        """
        config = self.config
        belize_vms_gcs_path=config['belize_vms_gcs_path']
        config['belize_vms_gcs_path']=belize_vms_gcs_path[:-1] if belize_vms_gcs_path.endswith('/') else belize_vms_gcs_path

        with DAG(dag_id, schedule_interval=self.schedule_interval, default_args=self.default_args) as dag:

            fetch = self.build_docker_task({
                'task_id':'pipe_belize_fetch',
                'pool':BELIZE_VMS_SCRAPPER_POOL,
                'docker_run':'{docker_run}'.format(**config),
                'image':'{docker_image}'.format(**config),
                'name':'pipe-belize-fetch',
                'dag':dag,
                'retries':5,
                'max_retry_delay': timedelta(hours=5),
                'arguments':['fetch_belize_vms_data',
                             '-d {ds}'.format(**config),
                             '-o {belize_vms_gcs_path}/'.format(**config),
                             '-rtr {}'.format(config.get('belize_api_max_retries', 3))]
            })

            load = self.build_docker_task({
                'task_id':'pipe_belize_load',
                'pool':'k8operators_limit',
                'docker_run':'{docker_run}'.format(**config),
                'image':'{docker_image}'.format(**config),
                'name':'pipe-belize-load',
                'dag':dag,
                'retries':5,
                'max_retry_delay': timedelta(hours=5),
                'arguments':['load_belize_vms_data',
                             '{ds}'.format(**config),
                             '{belize_vms_gcs_path}'.format(**config),
                             '{project_id}:{belize_vms_bq_dataset_table}'.format(**config)]
            })

            delete_duplicated = self.build_docker_task({
                'task_id':'pipe_belize_deduplicated',
                'pool':'k8operators_limit',
                'docker_run':'{docker_run}'.format(**config),
                'image':'{docker_image}'.format(**config),
                'name':'pipe-belize-deduplicate',
                'dag':dag,
                'arguments':['delete_duplicated',
                             '{ds}'.format(**config),
                             '{project_id}:{belize_vms_bq_dataset_table}'.format(**config),
                             '{project_id}:{belize_vms_dedup_bq_dataset_table}'.format(**config)]
            })

            dag >> fetch >> load >> delete_duplicated

        return dag

pipe_vms_belize_api_dag = PipeVMSBelizeDagFactory().build(PIPELINE)
