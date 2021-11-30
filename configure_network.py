#!/usr/bin/python3
import sys
from yaml import SafeLoader
import yaml

network_name = sys.argv[1]
docker_compose_file = "docker-compose.yaml"

with open(docker_compose_file) as f:
    d = yaml.load(f, Loader=SafeLoader)
    d["networks"] = {
        network_name:
            {
                "external": True
            }
    }
    for service in d["services"]:
        d["services"][service]["networks"] = [network_name]

with open(docker_compose_file, "w") as res:
    yaml.dump(d, res, sort_keys=False)
