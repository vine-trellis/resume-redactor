import os


def get_current_redaction_version():
    version = int(os.environ.get("REDACTION_VERSION", 2))
    return version


def get_resume_s3_config():
    aws_region = os.environ.get("AWS_DEFAULT_REGION")
    bucket = os.environ.get("AWS_S3_RESUME_BUCKET_NAME")
    prefix = os.environ.get("AWS_S3_UPLOAD_PREFIX")
    return dict(aws_region=aws_region, prefix=prefix, bucket=bucket)


def get_rabbitmq_consumer_config():
    namespace = "resume"
    return dict(namespace=namespace)
