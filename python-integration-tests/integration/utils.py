def combine_dicts(list_of_dicts):
    whole_dict = {}
    for d in list_of_dicts:
        whole_dict.update(d)
    return whole_dict