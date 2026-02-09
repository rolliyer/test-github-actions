from airflow.providers.google.cloud.operators.cloud_run import CloudRunExecuteJobOperator
from airflow.utils.context import Context

class RollerCloudRunExecuteJobOperator(CloudRunExecuteJobOperator):
    ui_color = "#8fd3f5"  # Light blue background
    """
    A custom, reusable operator for the Roller team that inherits from
    CloudRunExecuteJobOperator and manually renders the 'overrides' field at
    execution time. This works around issues in older provider versions where
    this field is not correctly templated.
    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def execute(self, context: Context):
        # Manually render the overrides field using the execution context.
        # This ensures Jinja templates are processed just before the job runs.
        if self.overrides:
            self.overrides = self.render_template(self.overrides, context)
        
        # Call the original execute method with the now-rendered overrides
        return super().execute(context)
