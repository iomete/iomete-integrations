from airflow.plugins_manager import AirflowPlugin

from iomete_airflow_plugin.hook import IometeHook
from iomete_airflow_plugin.iomete_operator import IometeOperator

plugin_name = "iomete"


class IometePlugin(AirflowPlugin):
    name = plugin_name
    operators = [IometeOperator]
    hooks = [IometeHook]
    executors = []
    macros = []
    admin_views = []
    menu_links = []
