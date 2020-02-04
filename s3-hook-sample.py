import airflow.hooks.S3_hook

def upload_file_to_S3_with_hook(filename, key, bucket_name):
    hook = airflow.hooks.S3_hook.S3Hook('your_connection_id')
    hook.load_file(filename, key, bucket_name)