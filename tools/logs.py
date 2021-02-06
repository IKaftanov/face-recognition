import os
import yaml
import logging.config


def get_logger(module_name, path_to_logger=None, output_folder=None):
    """
    get logger configuration
    :param output_folder:
    :param path_to_logger: str
    :param module_name: str
    :return:
    """
    if not path_to_logger:
        path_to_logger = os.path.join(os.path.dirname(__file__), "logger.yaml")
    if not output_folder:
        output_folder = "logs"
        if not os.path.exists(output_folder):
            os.mkdir(output_folder)
    with open(path_to_logger, 'rt') as f:
        config = yaml.safe_load(f.read())
    config["handlers"]["info_file_handler"]["filename"] = os.path.join(output_folder, config["handlers"]["info_file_handler"]["filename"])
    config["handlers"]["error_file_handler"]["filename"] = os.path.join(output_folder, config["handlers"]["error_file_handler"]["filename"])
    logging.config.dictConfig(config)
    logger = logging.getLogger(module_name)
    return logger