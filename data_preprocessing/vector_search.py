def find_index(vsc, endpoint_name, index_name):
    all_indexes = vsc.list_indexes(name=endpoint_name).get("vector_indexes", [])
    return destination_tables_config["vectorsearch_index_name"] in map(lambda i: i.get("name"), all_indexes)