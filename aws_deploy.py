from shared import (
    get_lambda_client,
    get_logs_client,
    get_sns_client,
    get_iam_client,
    get_s3_client,
    get_logger
)
from time import sleep
import tempfile
import logging
import zipfile
import hashlib
import shutil
import sys
import os

logger = get_logger(__file__)

temp_dir = tempfile.mkdtemp(dir='/tmp/processing')


def reset_s3_trigger(
        function_name,
        bucket_name,
        prefix,
        add_new=True):
    s3_path = '{}/{}'.format(bucket_name, prefix)
    statement_id = hashlib.md5(s3_path.encode(
        'utf-8')).hexdigest()

    lambda_client = get_lambda_client()
    try:
        lambda_client.remove_permission(
            FunctionName=function_name,
            StatementId=statement_id,
        )
    except lambda_client.exceptions.ResourceNotFoundException:
        pass

    if not add_new:
        return

    lambda_client.add_permission(
        Action='lambda:InvokeFunction',
        FunctionName=function_name,
        Principal='s3.amazonaws.com',
        SourceArn='arn:aws:s3:::{}/*'.format(bucket_name),
        SourceAccount='XXXXXXXXXXXX',
        StatementId=statement_id,
    )
    resp = lambda_client.get_function(FunctionName=function_name)
    lambda_function_arn = resp['Configuration']['FunctionArn']
    config = {
        'LambdaFunctionConfigurations': [
            {
                'LambdaFunctionArn': lambda_function_arn,
                'Events': [
                    's3:ObjectCreated:*'
                ]
            }
        ]
    }

    s3_client = get_s3_client()
    s3_client.put_bucket_notification_configuration(
        Bucket=bucket_name,
        NotificationConfiguration=config)


def delete_lambda_functions(
        process_size=50):
    lambda_client = get_lambda_client()
    for idx in range(1, process_size + 1):
        function_name = 'lconvert-{}'.format(idx)
        try:
            lambda_client.delete_function(
                FunctionName=function_name)
            logger.debug('Deleted function: {}'.format(function_name))
        except lambda_client.exceptions.ResourceNotFoundException:
            break


def delete_sns_topics(
        topic_base_name,
        process_size=50):
    sns_client = get_sns_client()
    lambda_client = get_lambda_client()

    for idx in range(1, process_size + 1):
        topic_name = '{}-converter-{}'.format(topic_base_name, idx)
        function_name = 'lconvert-{}'.format(idx)
        statement_id = hashlib.md5(topic_name.encode('utf-8')).hexdigest()

        try:
            lambda_client.remove_permission(
                FunctionName=function_name,
                StatementId=statement_id,
            )
        except lambda_client.exceptions.ResourceNotFoundException:
            pass

        topic_arn = 'arn:aws:sns:us-east-2:XXXXXXXXXXXX:{}'.format(topic_name)
        sns_client.delete_topic(TopicArn=topic_arn)
        logger.debug('Deleted topic: {}'.format(topic_name))


def reset_sns_trigger(
        function_name,
        sns_topic_name,
        add_new=False):
    statement_id = hashlib.md5(sns_topic_name.encode(
        'utf-8')).hexdigest()

    lambda_client = get_lambda_client()
    try:
        lambda_client.remove_permission(
            FunctionName=function_name,
            StatementId=statement_id,
        )
    except lambda_client.exceptions.ResourceNotFoundException:
        pass

    sns_client = get_sns_client()
    resp = sns_client.create_topic(Name=sns_topic_name)
    logger.debug('Created SNS topic: {}'.format(sns_topic_name))
    topic_arn = resp['TopicArn']

    if not add_new:
        return

    lambda_client.add_permission(
        Action='lambda:InvokeFunction',
        FunctionName=function_name,
        Principal='sns.amazonaws.com',
        SourceArn=topic_arn,
        StatementId=statement_id,
    )

    resp = lambda_client.get_function(FunctionName=function_name)
    function_arn = resp['Configuration']['FunctionArn']
    logger.debug('Subscribing {} to {}'.format(function_name, sns_topic_name))
    sns_client.subscribe(
        TopicArn=topic_arn,
        Protocol='lambda',
        Endpoint=function_arn)


def compress(zipfile_path, src_files, site_packages_dir_path=None):
    zipf = zipfile.ZipFile(zipfile_path, 'w', zipfile.ZIP_DEFLATED)
    seperator_string = 'site-packages'
    len_seperator_string = len(seperator_string)

    for src_file in src_files:
        zipf.write(src_file, os.path.basename(src_file))

    if site_packages_dir_path:
        for root, dirs, files in os.walk(site_packages_dir_path):
            for file in files:
                file_path = os.path.join(root, file)
                site_packages_index = file_path.index(seperator_string)
                store_path = file_path[
                    site_packages_index + len_seperator_string:]
                zipf.write(os.path.join(root, file), store_path)


def recycle(
        function_name,
        compressed_code_path,
        description=None,
        handler=None,
        environment_variables=None,
        delete_logs=False):

    logs_client = get_logs_client()
    lambda_client = get_lambda_client()
    iam_client = get_iam_client()

    if delete_logs:
        try:
            logs_client.delete_log_group(
                logGroupName='/aws/lambda/{}'.format(function_name))
        except logs_client.exceptions.ResourceNotFoundException:
            pass
        else:
            logger.debug('Deleted log group for {}'.format(function_name))

    try:
        lambda_client.delete_function(
            FunctionName=function_name)
    except lambda_client.exceptions.ResourceNotFoundException:
        pass

    params = dict(FunctionName=function_name,
                  Code=dict(ZipFile=open(
                      compressed_code_path, 'rb').read()),
                  Handler=handler if handler else
                  '{}.lambda_handler'.format(
                      function_name),
                  Publish=True,
                  MemorySize=3008,
                  Runtime='python3.6',
                  Role=[role['Arn'] for role in iam_client.list_roles(
                  )['Roles'] if role['RoleName'
                                     ] == 'lambda-execution-role'][0])
    if description:
        params['Description'] = description

    if environment_variables:
        params['Environment'] = dict(
            Variables=environment_variables)

    lambda_client.create_function(**params)
    logger.debug('Created Function: {}'.format(function_name))

    return

    params = dict(FunctionName=function_name,
                  ZipFile=open(compressed_code_path, 'rb').read())
    lambda_client.update_function_code(**params)

    config_params = dict()

    if handler:
        config_params['Handler'] = handler

    if description:
        config_params['Description'] = description

    if environment_variables:
        config_params['Environment'] = dict(
            Variables=environment_variables)

    if config_params:
        config_params['FunctionName'] = function_name
        lambda_client.update_function_configuration(**config_params)


def deploy(source_name, process_date, processing_size):
    bucket_name = 'nexscope-lambda'
    root_dir = os.path.expanduser('~')
    safety_dir = os.path.join(root_dir, 'safety')
    site_packages_dir = os.path.join(root_dir,
                                     'venvs',
                                     'ldelta',
                                     'lib',
                                     'python3.6',
                                     'site-packages')
    ldelta_file = os.path.join(safety_dir, 'ldelta.py')
    lconvert_file = os.path.join(safety_dir, 'lconvert.py')
    lshared_file = os.path.join(safety_dir, 'lshared.py')
    ldelta_zipfile_path = os.path.join(temp_dir, 'ldelta.zip')
    logger.debug(ldelta_zipfile_path)
    lconvert_zipfile_path = os.path.join(temp_dir, 'lconvert.zip')

    # compress(
    #     ldelta_zipfile_path,
    #     [ldelta_file, lshared_file])

    compress(
        lconvert_zipfile_path,
        [lconvert_file, lshared_file],
        site_packages_dir
    )

    environment_variables = dict(
        LAMBDA_AWS_ACCESS_KEY_ID='XXXXXXXXXXXX',
        LAMBDA_AWS_SECRET_ACCESS_KEY='XXXXXXXXXXXX',
        LAMBDA_AWS_DEFAULT_REGION='us-east-2'
    )
    # recycle('ldelta', ldelta_zipfile_path, 'Delta Processor',
    #         None, environment_variables, True)

    source_processing_base_name = source_name if not process_date else \
        '{}{}'.format(source_name, process_date)
    delete_sns_topics(source_processing_base_name, processing_size)
    # delete_lambda_functions(processing_size)

    # lambda_function_name = 'ltests3'
    # recycle(lambda_function_name, lconvert_zipfile_path,
    #             'Conversion to JSON',
    #             'lconvert.lambda_handler', environment_variables, True)
    # reset_s3_trigger(
    #     lambda_function_name,
    #     bucket_name,
    #     'data/2018/04/04/structured/processed/pubmed/decompressed/pubmed18n0001',
    #     True)

    for idx in range(1, processing_size + 1):
        lambda_function_name = 'lconvert-{}'.format(idx)
        sns_topic_name = '{}-converter-{}'.format(
            source_processing_base_name, idx)
        # recycle(lambda_function_name, lconvert_zipfile_path,
        #         'Conversion to JSON',
        #         'lconvert.lambda_handler', environment_variables, False)
        reset_sns_trigger(lambda_function_name, sns_topic_name, True)


def main():
    # deploy('who', '20180509', 100)
    deploy('pubmed', '20180409', 100)
    # deploy('pubmed', '20180410', 900)
    # deploy('pubmed', '20180411', 900)
    # deploy('pubmed', '20180412', 900)
    # deploy('pubmed', '20180412', 900)
    shutil.rmtree(temp_dir)


if __name__ == '__main__':
    main()
