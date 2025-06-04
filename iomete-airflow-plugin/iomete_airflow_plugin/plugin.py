from airflow.plugins_manager import AirflowPlugin
from flask import Blueprint

from iomete_airflow_plugin.hook import IometeHook
from iomete_airflow_plugin.iomete_operator import IometeOperator

plugin_name = 'iomete'

bp = Blueprint(
    plugin_name,
    __name__,
    template_folder='templates',  # registers airflow/plugins/templates as a Jinja template folder
    static_folder='static',
    static_url_path='/static/' + plugin_name,
)


class IometePlugin(AirflowPlugin):
    name = plugin_name
    operators = [IometeOperator]
    hooks = [IometeHook]
    executors = []
    macros = []
    admin_views = []
    flask_blueprints = [bp]
    menu_links = []
