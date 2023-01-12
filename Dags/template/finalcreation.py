from Config.config import *
from jinja2 import Environment, FileSystemLoader


env=Environment(loader=FileSystemLoader("/Users/rakshitsaxena/Documents/AirflowDMS/Dags/template/"))
template=env.get_template("template_dag.jinja2")
for dag_config in GlueConfiguration:
    newfilname="/Users/rakshitsaxena/Documents/AirflowDMS/Dags/all_dags/glue_"+dag_config["dag_id"]+".py"
    with open(newfilname,"w") as f:
        f.write(template.render(dag_config))