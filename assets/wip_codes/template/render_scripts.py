####
## Python script to render DAGs and ETL scripts
## Tech Implementation Answer by Mario Caesar // caesarmario87@gmail.com
####

# Importing Libraries
from jinja2 import Environment, FileSystemLoader

import os
import click
import yaml


def get_config(path):
    """
    Load YAML configuration file.
    Args:
        path (str): Filesystem path to the YAML config.
    Returns:
        dict: Parsed config or empty dict if file is missing or invalid.
    """
    if not os.path.isfile(path):
        click.echo(f"[WARN] Config not found: {path}", err=True)
        return {}
    with open(path, "r") as f:
        return yaml.safe_load(f)


def get_template(search_path, name):
    """
    Retrieve a Jinja2 template by name.
    Args:
        search_path (str): Directory where templates reside.
        name (str): Template filename to load.
    Returns:
        jinja2.Template: Compiled template object.
    """
    env = Environment(
        loader = FileSystemLoader(os.path.abspath(search_path)),
        trim_blocks = True,
        lstrip_blocks = True,
    )
    return env.get_template(name)


@click.group()
def cli():
    """
    CLI group for rendering DAGs and scripts.
    """
    pass

# --------------------------------------------------------------------------------
# DAG rendering commands
# --------------------------------------------------------------------------------
@cli.command()
def gendag01():
    """
    Render the extract yelp file to MinIO DAG.
    """
    filename = "01_dag_extract_yelp_to_minio"
    config   = "extract_config"

    # Load DAG config
    cfg_path  = f"template/dag/config/{config}.yaml"
    cfg       = get_config(cfg_path)
    processes = cfg.get("extractor", {})

    # Load DAG template
    tpl_dir  = "template/dag"
    tpl_name = f"{filename}.py.j2"
    template = get_template(tpl_dir, tpl_name)

    # Render and write out
    out_path = f"airflow/dags/{filename}.py"
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    template.stream(processes=processes).dump(out_path)
    click.echo(f"Rendered DAG → {out_path}")

if __name__ == "__main__":
    cli()