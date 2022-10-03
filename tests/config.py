import yaml

config_path_cloud = "../../config/config_cloud.yaml"
config_path_local = "../../config/config_local.yaml"


def local_config():
    config = yaml.safe_load(open(config_path_local, "r"))
    return config

def cloud_config():
    config = yaml.safe_load(open(config_path_cloud, "r"))
    return config