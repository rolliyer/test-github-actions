import json
from airflow.sensors.base import BaseSensorOperator
from airflow.exceptions import AirflowException
from airflow.providers.http.hooks.http import HttpHook

class WorkatoJobPollSensor(BaseSensorOperator):
    template_fields = ('record_id',)

    def __init__(self,*, record_id, http_conn_id, api_token, polling_endpoint, **kwargs):
        super().__init__(**kwargs)
        self.http_conn_id = http_conn_id
        self.record_id = record_id
        self.api_token = api_token
        self.polling_endpoint = polling_endpoint

    def poke(self, context):
        http_hook = HttpHook(method='GET', http_conn_id=self.http_conn_id)
        
        self.log.info(f"Poking for record_id: {self.record_id}")
        
        response = http_hook.run(endpoint=self.polling_endpoint, 
                                 data={'record_id': self.record_id},
                                 headers={"Content-Type": "application/json",
                                          "api-token": self.api_token})
        
        self.log.info(f"Received response: {response.text}")
        
        if response.status_code != 200:
            self.log.error(f"Polling failed with status code {response.status_code}: {response.text}")
            return False

        try:
            payload = response.json()
        except json.JSONDecodeError:
            self.log.error("Failed to decode JSON from response")
            return False

        status = payload.get('status')
        self.log.info(f"Polling status for {self.record_id} is '{status}'")

        if status == 'DONE':
            return True
        elif status == 'FAILED':
            raise AirflowException(f"Polling failed for record_id {self.record_id}. Status: {status}")
        
        return False
