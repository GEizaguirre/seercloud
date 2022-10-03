from lithops import Storage


def get_data_size(storage: Storage, bucket:str, path:str):
    return int(storage.head_object(bucket, path)['content-length'])