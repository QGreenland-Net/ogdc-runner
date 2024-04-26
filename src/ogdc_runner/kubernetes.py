# classmap = {
#     ("ConfigMap", "BatchV1"): configmap_api_instance,
# 
# 
# }
# 
# 
# def k8s_apply(
#     *,
#     yaml_file: Path,
# ):
#     yml = safe_load(yaml_file)
#     kind = yml.kind
#     api = yml.api
# 
#     api = classmap[(kind, api)]
# 
#     api.apply(yml)
# 
# 
# k8s_apply("/path/to/my_yaml.yml")
