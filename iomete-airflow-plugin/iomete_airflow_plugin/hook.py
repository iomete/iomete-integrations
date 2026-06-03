from abc import ABC

from airflow.hooks.base import BaseHook
from iomete_sdk.spark import SparkJobApiClient


class IometeHook(BaseHook, ABC):
    def __init__(
        self,
        host: str,
        domain: str,
        access_token: str,
        host_verify: bool = True,
    ):
        super().__init__()

        self.host = host
        self.domain = domain
        self.access_token = access_token
        self.host_verify = host_verify

        self.iom_client = SparkJobApiClient(
            host=host,
            api_key=access_token,
            domain=domain,
            verify=host_verify,
        )

    def submit_job_run(self, job_id, payload):
        response = self.iom_client.submit_job_run(job_id=job_id, payload=payload)
        return response

    def get_job_run(self, job_id, run_id):
        response = self.iom_client.get_job_run_by_id(job_id=job_id, run_id=run_id)
        return response

    def cancel_job_run(self, job_id, run_id):
        response = self.iom_client.cancel_job_run(job_id=job_id, run_id=run_id)
        return response
