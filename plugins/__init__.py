from __future__ import division, absolute_import, print_function
from operators.data_quality import PostgresOperator
from airflow.plugins_manager import AirflowPlugin
import sys
import operators
import helpers

# Defining the plugin class
class DataPlugin(AirflowPlugin):
    name = "data_plugin"
    operators = [
        operators.PostgresOperator
    ]
    # A list of class(es) derived from BaseHook
    hooks = []
    # A list of class(es) derived from BaseExecutor
    executors = []
    # A list of references to inject into the macros namespace
    macros = []
    # A list of objects created from a class derived
    # from flask_admin.BaseView
    admin_views = []
    # A list of Blueprint object created from flask.Blueprint
    flask_blueprints = []
    # A list of menu links (flask_admin.base.MenuLink)
    menu_links = []
