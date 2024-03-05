from airflow.plugins_manager import AirflowPlugin
from airflow.models.baseoperator import BaseOperatorLink
from flask import Blueprint
from flask_appbuilder import expose, BaseView as AppBuilderBaseView
from jinja2 import Template
from flask import send_from_directory

import os

# define a Flask blueprint
my_blueprint = Blueprint(
    "test_plugin",
    __name__,
    # register airflow/plugins/templates as a Jinja template folder
    template_folder="templates",
    static_folder="templates/test_results",
    static_url_path='/reports/test_results'
)


def list_files_dict(directory, paths_to_consider):
    # Initialize an empty dictionary to store the results
    files_dict = {}
    fd = {}

    # List all files in the directory recursively
    for root, dirs, files in os.walk(directory):

        # Check if the current directory is in paths_to_consider and is not a subdirectory of any of the paths in paths_to_consider
        if root in paths_to_consider and not any(root.startswith(p) and len(root) > len(p) and root[len(p)] == os.sep for p in paths_to_consider):
            # Store the directory name as the key in the dictionary
            files_dict[root] = []

            # Append the list of file names to the value in the dictionary
            for file in files:
                full_path = os.path.join(root, file)
                files_dict[root].append((full_path, os.stat(full_path).st_ctime))

            # Sort the list of files based on their creation time
            files_dict[root] = sorted(files_dict[root], key=lambda x: x[1])

            # Convert the list of tuples back to a list of file paths
            files_dict[root] = [file[0] for file in files_dict[root]]

    for k, v in files_dict.items():
        sp_dire = k.split('/')[-1]
        fd[sp_dire] = []
        for each_f in v:
            fd[sp_dire].append(each_f.split('/')[-1])

    return fd



# create a flask appbuilder BaseView
class Reports(AppBuilderBaseView):
    default_view = "test"

    @expose("/")
    def test(self):
        # render the HTML file from the templates directory with content

        paths_to_consider = ['/opt/airflow/plugins/templates/test_results/extract-transform-load',
                             '/opt/airflow/plugins/templates/test_results/data-migration-from-mysql-to-postgres',
                             '/opt/airflow/plugins/templates/test_results/great_expectation_test_results']

        result = list_files_dict('/opt/airflow/plugins/templates/test_results', paths_to_consider)

        # Read the Jinja template file
        with open('/opt/airflow/plugins/templates/directory-list.html', 'r') as file:
            template_str = file.read()

        # Create a Jinja Template object
        template = Template(template_str)

        # Render the template with the list of HTML files
        rendered_html = template.render(files=result, directories=result)
        return rendered_html

    # @expose("/test")
    # def test(self):
    #     # render the HTML file from the templates directory with content
    #
    #     # Define the directory path where the HTML files are located
    #     directory_path = '/opt/airflow/plugins/templates/test_results'
    #
    #     # Get the list of HTML files in the directory
    #     html_files = [f for f in os.listdir(directory_path) if f.endswith('.html')]
    #
    #     # Read the Jinja template file
    #     with open('/opt/airflow/plugins/templates/html_list_template.html', 'r') as file:
    #         template_str = file.read()
    #
    #     # Create a Jinja Template object
    #     template = Template(template_str)
    #
    #     # Render the template with the list of HTML files
    #     rendered_html = template.render(files=html_files)
    #     return rendered_html
        # return self.render_template("html_list_template.html", context={"files": html_files})

    # @app.route('/static_html/<path:path>')
    # def serve_static_html(path):
    #     return send_from_directory('/opt/airflow/static_html', path)


    # @expose("/")
    # def test(self):
    #     # render the HTML file from the templates directory with content
    #     return self.render_template("test.html", content="awesome")

# instantiate MyBaseView
my_view = Reports()

# define the path to my_view in the Airflow UI
my_view_package = {
    # define the menu sub-item name
    "name": "Test Results",
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


# static_html_blueprint = Blueprint(
#     "static_html_blueprint",
#     __name__,
#     static_folder=os.path.join(os.path.dirname(__file__), "static"),
#     static_url_path="/static/html",
# )

# @static_html_blueprint.route("/<path:filename>")
# def serve_html(filename):
#     return send_from_directory(static_html_blueprint.static_folder, filename)
#
# def load_plugin(app):
#     app.register_blueprint(static_html_blueprint)

# class GlobalLink(BaseOperatorLink):
#     # name the link button
#     name = "Airflow docs"
#
#     # function determining the link
#     def get_link(self, operator, *, ti_key=None):
#         return "https://airflow.apache.org/"
#
#
# # add the operator extra link to a plugin
# class MyGlobalLink(AirflowPlugin):
#     name = "my_plugin_name"
#     global_operator_extra_links = [
#         GlobalLink(),
#     ]
# paths_to_consider = ['/opt/airflow/plugins/templates/test_results/etl-job',
#                      '/opt/airflow/plugins/templates/test_results/data-migration-from-mysql-to-postgres',
#                      '/opt/airflow/plugins/templates/test_results/great_expectation_test_results']
#
# result = list_files_dict('/opt/airflow/plugins/templates/test_results', paths_to_consider)
# print(result)