from boto3.session import Session
from functools import wraps
from itertools import chain
import rapidjson
import datetime
import logging
import zipfile
import pathlib
import shutil
import ntpath
import genson
import numpy
import uuid
import time
import gzip
import sys
import re
import os


def get_logger(filename, log_file=None):
    xlogger = logging.getLogger(filename)
    xlogger.setLevel(logging.DEBUG)

    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    channel = logging.FileHandler(log_file) if log_file else \
        logging.StreamHandler(sys.stdout)
    channel.setLevel(logging.DEBUG)
    channel.setFormatter(formatter)

    xlogger.addHandler(channel)

    return xlogger


logger = get_logger(__file__)


class ObjectDict(dict):
    """Provides dictionary with values also accessible by attribute

    """

    def __getattr__(self, attr):
        attr_reg_expr = re.compile(r'^(\w+)(\[)(\d+)(\])$')
        matches = re.findall(attr_reg_expr, attr)
        retval = self[attr]

        if matches:
            attr = matches[0][0]
            index = int(matches[0][2])
            retval = self[attr]

            if isinstance(retval, list):
                retval = retval[index]

        if isinstance(retval, dict):
            retval = ObjectDict(retval)

        return retval

    def __getitem__(self, item):
        try:
            return dict.__getitem__(self, item)
        except KeyError:
            return None


def dir_traverse(path):
    for root, subdirs, files in os.walk(path):
        for file in files:
            fpath = os.path.join(root, file)
            yield fpath


def read_json(fpath, mode='r'):
    with open(fpath, mode) as fp:
        return rapidjson.load(fp)


def transform_name_to_uuid(filename):
    return str(uuid.uuid5(uuid.NAMESPACE_DNS, filename))


def should_extract(filename, source_fileformat):
    if source_fileformat and source_fileformat != 'html':
        return not filename.startswith(
            '.') and filename.endswith(
            source_fileformat)
    return not filename.startswith('.')


def is_valid_format(filepath, fileformat):
    with open(filepath) as f:
        if fileformat == 'xml':
            return f.read(5) == '<?xml'
        elif fileformat == 'json':
            return f.read(1) == '{'
    return True


def get_dir_tree_size(path):
    """ Returns total size of files in a given path and subdirs

    """
    total = 0
    for entry in os.scandir(path):
        if entry.is_dir(follow_symlinks=False):
            total += get_dir_tree_size(entry.path)
        else:
            total += entry.stat(follow_symlinks=False).st_size
    return total


def size_formatter(value, unit, is_speed=False):
    out_unit = unit if not is_speed else '{}/s'.format(unit)

    return '{} {}'.format(value, out_unit)


def seconds_parser(seconds):
    minutes, seconds = divmod(seconds, 60)
    hours, minutes = divmod(minutes, 60)
    days, hours = divmod(hours, 24)

    return days, hours, minutes, seconds


def readable_time(value):
    days, hours, minutes, seconds = seconds_parser(value)

    readable_time_str = ''

    if days:
        days_str = 'days' if days > 1 else 'day'
        readable_time_str = '{}{} {} '.format(
            readable_time_str, int(days), days_str)
    if hours:
        hours_str = 'hrs' if hours > 1 else 'hr'
        readable_time_str = '{}{} {} '.format(
            readable_time_str, int(hours), hours_str)
    if minutes:
        minutes_str = 'mins' if minutes > 1 else 'min'
        readable_time_str = '{}{} {} '.format(
            readable_time_str, int(minutes), minutes_str)
    readable_time_str = '{}{} secs'.format(
        readable_time_str, round(seconds, 2))

    return readable_time_str


def readable_size(value, is_speed=False):
    kilos = 1024
    megs = kilos * 1024
    gigs = megs * 1024
    teras = gigs * 1024

    if value >= teras:
        return size_formatter(round(value/teras, 2), 'TB', is_speed)
    elif value < teras and value >= gigs:
        return size_formatter(round(value/gigs, 2), 'GB', is_speed)
    elif value < gigs and value >= megs:
        return size_formatter(round(value/megs, 2), 'MB', is_speed)
    elif value < megs and value >= kilos:
        return size_formatter(round(value/kilos, 2), 'KB', is_speed)
    else:
        return size_formatter(value, 'bytes', is_speed)


def get_timestamp(formatter_type=1):
    if formatter_type == 1:
        return datetime.datetime.now().strftime('%Y%m%d%H%M%S')
    elif formatter_type == 2:
        return datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')


def get_aws_session(old=False):
    if old:
        aws_access_key_id = 'XXXXXXXXXXXX'
        aws_secret_access_key = 'XXXXXXXXXXXX'
    else:
        aws_access_key_id = os.getenv(
            'LAMBDA_AWS_ACCESS_KEY_ID')
        aws_secret_access_key = os.getenv(
            'LAMBDA_AWS_SECRET_ACCESS_KEY')
    region_name = os.getenv(
        'LAMBDA_AWS_DEFAULT_REGION')

    session = Session(
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        region_name=region_name
    )

    return session


def get_s3_client(old=False):
    session = get_aws_session(old)

    return session.client('s3')


def get_sns_client():
    session = get_aws_session()

    return session.client('sns')


def get_sqs_client():
    session = get_aws_session()

    return session.client('sqs')


def get_lambda_client():
    session = get_aws_session()

    return session.client('lambda')


def get_iam_client():
    session = get_aws_session()

    return session.client('iam')


def get_logs_client():
    session = get_aws_session()

    return session.client('logs')


def get_athena_client():
    session = get_aws_session()

    return session.client('athena')


def s3_exists(client, bucket, key):
    resp = client.list_objects(
        Bucket=bucket,
        Prefix=key)
    expected_object = resp.get('Contents', [])

    return bool(expected_object)


def s3_download_file(s3_file_key, bucket_name=None, output_dir=None):
    s3_client = get_s3_client()
    if not bucket_name:
        bucket_name = bucket_1
    local_file = ntpath.basename(s3_file_key)
    if output_dir:
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        local_file = os.path.join(output_dir, local_file)

    logger.debug('Trying to download file to {}'.format(local_file))
    s3_client.download_file(
        bucket_name,
        s3_file_key,
        local_file)

    return local_file


def is_empty_file(xfile):
    return os.stat(xfile).st_size == 0


def fetch_filename_stem(filename):
    stem = filename
    suffix = True
    while suffix:
        resolver = pathlib.Path(stem)
        stem = resolver.stem
        suffix = resolver.suffix

    return stem


def zip_extractor(infile, out_dir):
    if not zipfile.is_zipfile(
            infile) or not infile.endswith('.zip'):
        return

    with zipfile.ZipFile(infile, 'r') as inzip:
        file_list = inzip.namelist()
        inzip.extractall(out_dir)
        file_list = [os.path.join(out_dir, f) for f in file_list]

    os.remove(infile)

    for filename in filter(zipfile.is_zipfile, file_list):
        zip_extractor(filename, out_dir)


def gzip_extractor(infile, outfile):
    with gzip.open(infile, 'rb') as inf:
        with open(outfile, 'wb') as otf:
            shutil.copyfileobj(inf, otf)


def bulk_delete_files(xfiles):
    if not isinstance(xfiles, list):
        xfiles = [xfiles]

    for xfile in xfiles:
        if os.path.exists(xfile):
            os.remove(xfile)


def list_dir(
        s3_client,
        bucket_name,
        prefix,
        page_limit,
        pages=None,
        delimiter='',
        file_writer=None,
        write_file_name_only=False,
        compare_hash=False,
        count_only=False,
        file_stem_only=False,
        write_full_key=False,
        delete_object=False,
        pick_random=False,
        random_size=3):
    params = dict(
        Bucket=bucket_name,
        Prefix=prefix,
        PaginationConfig=dict(PageSize=page_limit),
        Delimiter=delimiter)
    paginator = s3_client.get_paginator('list_objects')
    piterator = iter(paginator.paginate(**params))

    page_idx = 0
    all_keys = []
    random_keys = []
    still_more = True
    total_objects = 0
    total_size = 0

    iterate = page_idx < pages and still_more if isinstance(
        pages, int) else still_more

    while iterate:
        try:
            current_page = next(piterator)
        except StopIteration:
            still_more = False
        else:
            if file_writer:
                for item in current_page['Contents']:
                    total_size += item['Size']
                    if write_file_name_only:
                        if file_stem_only:
                            file_writer.write(fetch_filename_stem(
                                ntpath.basename(item['Key'])))
                        elif write_full_key:
                            file_writer.write(item['Key'])
                        else:
                            file_writer.write(ntpath.basename(item['Key']))
                    else:
                        if compare_hash:
                            file_writer.write('{}\t{}\t{}'.format(
                                ntpath.basename(item['Key']),
                                item['ETag'],
                                item['Size']))
                        else:
                            file_writer.write('{}\t{}'.format(
                                ntpath.basename(item['Key']),
                                item['Size']))
                    file_writer.write('\n')
                    total_objects += 1
            else:
                tot_items = len(current_page['Contents'])
                if pick_random:
                    contents_array = numpy.array(current_page['Contents'])
                    random_keys.append([item['Key'] for item in list(
                        contents_array[numpy.random.choice(
                            tot_items, size=random_size, replace=False)])])
                else:
                    total_objects += tot_items
                    for item in current_page['Contents']:
                        total_size += item['Size']
                        if not count_only:
                            all_keys.append(item['Key'])
                        if delete_object:
                            s3_client.delete_object(
                                Bucket=bucket_name,
                                Key=item['Key']
                            )
                            logger.debug('Deleted {}'.format(item['Key']))
            page_idx += 1
        iterate = page_idx < pages and still_more if isinstance(
            pages, int) else still_more

    if pick_random:
        # logger.debug(random_keys)
        return list(chain.from_iterable(iter(random_keys)))
    if count_only:
        return total_objects, readable_size(total_size)
    if not all_keys:
        return all_keys, total_objects, total_size
    return chain.from_iterable(all_keys), total_objects, total_size


def generate_field_definitions(schema, level=0, odd_ones=None):
    keywords = ['timestamp', 'date', 'datetime']
    tab = " "
    type_separator = " " if level == 0 else ": "
    field_separator = "\n" if level == 0 else ",\n"
    field_definitions = []
    new_level = level + 1
    indentation = new_level * tab
    for name, attributes in schema.items():
        cleaned_name = "`{}`".format(
            name) if name.lower() in keywords else name
        if isinstance(attributes['type'], list):
            if 'string' in attributes['type']:
                attributes['type'] = 'string'
            elif 'object' in attributes['type']:
                attributes['type'] = 'object'
        if attributes['type'] == 'object':
            field_definitions.append(
                "{indentation}{name}{separator}STRUCT<\n"
                "{definitions}\n{indentation}>".format(
                    indentation=indentation,
                    name=cleaned_name,
                    separator=type_separator,
                    definitions=generate_field_definitions(
                        attributes['properties'], new_level,
                        odd_ones)
                ))
        elif attributes['type'] == 'array':
            extra_indentation = (new_level + 1) * tab
            if isinstance(attributes['xitems']['type'], list):
                if 'string' in attributes['xitems']['type']:
                    attributes['xitems']['type'] = 'string'
                elif 'object' in attributes['xitems']['type']:
                    attributes['xitems']['type'] = 'object'
            if attributes['xitems']['type'] == 'object':
                closing_bracket = "\n" + indentation + ">"
                array_type = "STRUCT<\n{definitions}\n{indentation}>".format(
                    indentation=extra_indentation,
                    definitions=generate_field_definitions(
                        attributes['xitems']['properties'], new_level + 1,
                        odd_ones)
                )
            else:
                closing_bracket = ">"
                array_type = attributes['xitems']['type'].upper()
            field_definitions.append(
                "{indentation}{name}{"
                "separator}ARRAY<{definitions}{closing_bracket}".format(
                    indentation=indentation,
                    name=cleaned_name,
                    separator=type_separator,
                    definitions=array_type,
                    closing_bracket=closing_bracket
                ))
        else:
            if ':' in cleaned_name:
                odd_ones.add(cleaned_name)
            field_definitions.append("{indentation}"
                                     "{name}{separator}{type}".format(
                                         indentation=indentation,
                                         name=cleaned_name,
                                         separator=type_separator,
                                         type=attributes['type'].upper()
                                     ))

    return field_separator.join(field_definitions)


def generate_json_table_statement(table, schema,
                                  data_location='',
                                  database='default', managed=False):
    odd_ones = set()
    field_definitions = generate_field_definitions(
        schema['properties'], 0, odd_ones)
    logger.debug('Odd Ones: {}'.format(list(odd_ones)))
    external_marker = "EXTERNAL " if not managed else ""
    location = "\nLOCATION '{}'".format(data_location) if not managed else ''
    statement = """CREATE {external_marker}TABLE {table} (
{field_definitions}
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'{location}
WITH SERDEPROPERTIES (
  'serialization.format' = '1'
)
TBLPROPERTIES ('has_encrypted_data'='false')""".format(
        external_marker=external_marker,
        database=database,
        table=table,
        field_definitions=field_definitions,
        location=location
    )
    return statement


def infer_schema(json_objects):
    s = genson.Schema()
    s.add_schema({"type": "object", "properties": {}})
    for o in json_objects:
        s.add_object(o)
    return s.to_dict()


def infer_schema_from_file(json_file_path):
    with open(json_file_path, 'r') as f:
        objects = [rapidjson.loads(line) for line in f.readlines()]
    return infer_schema(objects)
