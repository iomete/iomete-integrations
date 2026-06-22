from airflow.plugins_manager import AirflowPlugin

from iomete_airflow_plugin.hook import IometeHook
from iomete_airflow_plugin.iomete_operator import IometeOperator

plugin_name = "iomete"

# Flask ships with the Airflow 2.x webserver but was dropped in Airflow 3 (FastAPI-based UI).
# Register the blueprint only when Flask is importable so the plugin loads on both.
try:
    from flask import Blueprint

    _flask_blueprints = [
        Blueprint(
            plugin_name,
            __name__,
            template_folder="templates",  # registers airflow/plugins/templates as a Jinja template folder
            static_folder="static",
            static_url_path="/static/" + plugin_name,
        )
    ]
except ImportError:
    _flask_blueprints = []


class IometePlugin(AirflowPlugin):
    name = plugin_name
    operators = [IometeOperator]
    hooks = [IometeHook]
    executors = []
    macros = []
    admin_views = []
    flask_blueprints = _flask_blueprints
    menu_links = []
