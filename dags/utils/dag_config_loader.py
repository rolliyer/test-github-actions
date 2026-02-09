from __future__ import annotations

import os
import yaml
import logging

logger = logging.getLogger(__name__)


def load_config(dag_file_path: str, config_file_name: str) -> dict:
    """
    Loads a YAML configuration file from the 'config' subdirectory relative to the DAG file.

    Args:
        dag_file_path: The __file__ attribute of the calling DAG script.
        config_file_name: The name of the YAML config file.

    Returns:
        A dictionary with the loaded configuration, or an empty dict if loading fails.
    """
    dags_folder = os.path.dirname(os.path.realpath(dag_file_path))
    config_file_path = os.path.join(dags_folder, "config", config_file_name)

    try:
        with open(config_file_path, "r") as file:
            config = yaml.safe_load(file) or {}
        logger.info(f"Successfully loaded config from {config_file_path}")
        return config
    except (FileNotFoundError, Exception) as e:
        logger.error(f"Could not load or parse YAML config file at {config_file_path}. Error: {e}")
        return {}
