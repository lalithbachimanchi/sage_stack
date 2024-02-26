from airflow.plugins_manager import AirflowPlugin
from airflow.models.baseoperator import BaseOperatorLink
from flask import Blueprint
from flask_appbuilder import expose, BaseView as AppBuilderBaseView
from jinja2 import Template

import os

# define a Flask blueprint
my_blueprint = Blueprint(
    "test_plugin",
    __name__,
    # register airflow/plugins/templates as a Jinja template folder
    template_folder="templates",
)

# create a flask appbuilder BaseView
class MyBaseView(AppBuilderBaseView):
    default_view = "test"

    @expose("/")
    def test(self):
        # render the HTML file from the templates directory with content

        # Define the directory path where the HTML files are located
        directory_path = '/opt/airflow/plugins/templates/test_results'

        # Get the list of HTML files in the directory
        html_files = [f for f in os.listdir(directory_path) if f.endswith('.html')]

        # Read the Jinja template file
        # with open('html_list_template.html', 'r') as file:
        #     template_str = file.read()

        # Create a Jinja Template object
        # template = Template(template_str)

        # Render the template with the list of HTML files
        # rendered_html = template.render(files=html_files)
        # return rendered_html
        return self.render_template("html_list_template.html", context={"files": html_files})

    # @expose("/")
    # def test(self):
    #     # render the HTML file from the templates directory with content
    #     return self.render_template("test.html", content="awesome")

# instantiate MyBaseView
my_view = MyBaseView()

# define the path to my_view in the Airflow UI
my_view_package = {
    # define the menu sub-item name
    "name": "Test View",
    # define the top-level menu item
    "category": "Reports",
    "view": my_view,
}

# define the plugin class
class MyViewPlugin(AirflowPlugin):
    # name the plugin
    name = "My appbuilder view"
    # add the blueprint and appbuilder_views components
    flask_blueprints = [my_blueprint]
    appbuilder_views = [my_view_package]


class GlobalLink(BaseOperatorLink):
    # name the link button
    name = "Airflow docs"

    # function determining the link
    def get_link(self, operator, *, ti_key=None):
        return "https://airflow.apache.org/"


# add the operator extra link to a plugin
class MyGlobalLink(AirflowPlugin):
    name = "my_plugin_name"
    global_operator_extra_links = [
        GlobalLink(),
    ]