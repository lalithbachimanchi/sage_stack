from airflow.plugins_manager import AirflowPlugin
from airflow.models.baseoperator import BaseOperatorLink
from flask import Blueprint
from flask_appbuilder import expose, BaseView as AppBuilderBaseView

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
        return self.render_template("test.html", content="awesome")

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