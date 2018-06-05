from shared import (
    generate_json_table_statement,
    infer_schema_from_file,
    get_s3_client,
    readable_time,
    dir_traverse,
    ObjectDict,
    get_logger,
    read_json
)
from math import ceil
import functools
import rapidjson
import itertools
import tempfile
import ntpath
import shutil
import ijson
import time
import sys
import os
import re
import io


logger = get_logger(__file__)


def get_sub_target_trail_end(sub_target_key):
    sub_target_array = sub_target_key.split('.')
    tot_items = sum([1 for i in sub_target_array if i == 'xitem'])

    if not tot_items:
        raise ValueError('No items found for {}'.format(sub_target_key))

    if tot_items == 1:
        return sub_target_key

    sub_target_array.reverse()
    item_occurences = [idx for idx, item in enumerate(
        sub_target_array) if item == 'xitem']

    final_array = sub_target_array[item_occurences[0]:item_occurences[1]]
    final_array.reverse()

    return '.'.join(final_array)


def target_finder(descriptor, target_key, target_type):
    parser = ijson.parse(descriptor)
    prefix_dict = dict()
    ordered_keys = target_key.split('.')
    target_prefix = '.'.join(ordered_keys[:-1])
    sub_target_keys = ['.'.join(ordered_keys[:idx + 1]
                                ) for idx, item in enumerate(
        ordered_keys) if item == 'xitem']

    target_found = 0
    for prefix, event, value in parser:
        if event == 'start_array':
            array_prefix = '{}.xitem'.format(prefix)
            prefix_dict[array_prefix] = {'counter': None}

        if prefix in prefix_dict:
            if event in ('null', 'string', 'start_map'):
                if prefix_dict[prefix]['counter'] == None:
                    prefix_dict[prefix]['counter'] = 0
                else:
                    prefix_dict[prefix]['counter'] += 1
        if prefix == target_key and event == target_type:
            try:
                final_target_keys = [get_sub_target_trail_end(
                    sub_target_key).replace('.xitem', '[{}]'.format(
                        prefix_dict[sub_target_key][
                            'counter'])) for sub_target_key in sub_target_keys]
            except KeyError:
                logger.debug(target_key)
                raise
            yield '.'.join(final_target_keys)


def set_multilevel_attr_val(object_dict, attribute_tree, value=None):
    attr_reg_expr = re.compile(r'^(\w+)(\[)(\d+)(\])$')
    temp_dict = object_dict
    attribute_tree_array = attribute_tree.split('.')
    for node in attribute_tree_array[:-1]:
        matches = re.findall(attr_reg_expr, node)
        if not matches:
            temp_dict = temp_dict.get(node)
            continue
        item = matches[0][0]
        index = int(matches[0][2])
        temp_list = temp_dict.get(item)
        if isinstance(temp_list, list):
            temp_dict = temp_list[index]
        else:
            raise IndexError('{} is not of type list'.format(item))
    target_node = attribute_tree_array[-1]
    matches = re.findall(attr_reg_expr, target_node)
    if matches:
        target_node = matches[0][0]
        index = int(matches[0][2])
        if isinstance(temp_dict[target_node][index],
                      dict) and 'text' in temp_dict[
                target_node][index] and isinstance(
                temp_dict[target_node][index]['text'], list):
            temp_dict[target_node][index]['text'].append(value['text'][0])
        else:
            temp_dict[target_node][index] = value
    else:
        if isinstance(temp_dict[target_node],
                      dict) and 'text' in temp_dict[
                target_node] and isinstance(
                temp_dict[target_node]['text'], list):
            temp_dict[target_node]['text'].append(value['text'][0])
        else:
            temp_dict[target_node] = value


def get_multilevel_attr_val(object_dict, attribute_tree):
    return functools.reduce(
        lambda x, y: getattr(x, y, ''), attribute_tree.split('.'),
        object_dict)


def schema_to_orig(prefix):
    return prefix.replace(
        'properties.', '').replace(
        '.properties', '').replace(
        '.type', '').replace('xitems', 'xitem')


def get_transformation_possibilities(filename):
    transformation_required_prefixes = []
    properties_id = 0

    with open(filename, 'rb') as fd:
        parser = ijson.parse(fd)
        running_prefix, found_object, found_string = None, False, False
        for prefix, event, value in parser:
            if running_prefix:
                if prefix == '{}.xitem'.format(running_prefix
                                               ) and value == 'object':
                    found_object = True
                elif prefix == '{}.xitem'.format(running_prefix
                                                 ) and value == 'string':
                    found_string = True
                if event == 'end_array':
                    if found_object and found_string:
                        transformation_required_prefixes.append(running_prefix)
                    (running_prefix, found_object, found_string) = (
                        None, False, False)
                continue
            if event == 'start_array' and prefix.endswith('type'):
                running_prefix = prefix

    return transformation_required_prefixes


def get_applicable_transformation_prefixes(
        filepath,
        all_transformation_prefixes):
    schema = infer_schema_from_file(filepath)
    schema_descriptor = io.StringIO(rapidjson.dumps(schema))

    parser = ijson.parse(schema_descriptor)
    available_prefixes = set([
        prefix for prefix, event, value in parser])

    applicable_transformation_prefixes = available_prefixes.intersection(
        set(all_transformation_prefixes))

    return list(applicable_transformation_prefixes)


def _transform(
        fpath,
        fpath_transformed,
        all_transformation_prefixes,
        s3_client,
        bucket_name,
        s3_base,
        upload=False):
    with open(fpath, 'rb') as fd:
        orig_json_object = ObjectDict(next(ijson.items(fd, '')))

    transformed = True
    applicable_transformation_prefixes = \
        get_applicable_transformation_prefixes(
            fpath,
            all_transformation_prefixes)

    for applicable_transformation_prefix in \
            applicable_transformation_prefixes:

        applicable_transformation_prefix = schema_to_orig(
            applicable_transformation_prefix)

        with open(fpath, 'rb') as fp:
            total_items = list(ijson.items(
                fp, applicable_transformation_prefix))
            tot_strings = sum([1 for item in total_items if isinstance(
                item, str)])

        if tot_strings:
            total_strings_processed = 0
            with open(fpath, 'rb') as fp:
                for strings_found_count, \
                        current_applicable_transformation_prefix \
                        in enumerate(target_finder(
                            fp,
                            applicable_transformation_prefix,
                            'string')):
                    try:
                        current_applicable_transformation_value = \
                            get_multilevel_attr_val(
                                orig_json_object,
                                current_applicable_transformation_prefix)
                    except IndexError:
                        logger.debug(
                            'Failed to find the index {}'.format(
                                current_applicable_transformation_prefix))
                    else:
                        if isinstance(
                                current_applicable_transformation_value,
                                str) and \
                                current_applicable_transformation_value:
                            replaced_value = \
                                {'text':
                                 [current_applicable_transformation_value]
                                 }
                            set_multilevel_attr_val(
                                orig_json_object,
                                current_applicable_transformation_prefix,
                                replaced_value
                            )
                            total_strings_processed += 1
                strings_found_count += 1
                if strings_found_count == tot_strings ==\
                        total_strings_processed:
                    pass
                else:
                    transformed = False
                    logger.debug(
                        '{} ** FAILED ** - Strings - Reported: {},'
                        ' Found: {}, Processed: {}'.format(
                            applicable_transformation_prefix,
                            tot_strings,
                            strings_found_count,
                            total_strings_processed))

    if transformed:
        with open(fpath_transformed, 'wb') as fd:
            rapidjson.dump(orig_json_object, fd)
        logger.debug(
            'Transformation SUCCESS. Transformed '
            'JSON written to {}'.format(fpath_transformed))
        if upload:
            file_base = ntpath.basename(fpath_transformed)
            s3_path = os.path.join(s3_base, file_base)
            s3_client.upload_file(
                fpath_transformed,
                bucket_name,
                s3_path)
            logger.debug('Transformed JSON {} uploaded to {}'.format(
                fpath_transformed, s3_path))
        return True

    logger.debug(
        'Transformation FAILED for: {}'.format(fpath))
    return False


def transform(
        source_dir,
        dest_dir,
        source_schema_path,
        s3_client,
        bucket_name,
        s3_base):
    success, failed = 0, 0

    transform_start = time.time()

    all_transformation_prefixes = get_transformation_possibilities(
        source_schema_path)

    if os.path.exists(dest_dir):
        shutil.rmtree(dest_dir)
    os.makedirs(dest_dir)

    for idx, fpath in enumerate(dir_traverse(source_dir)):
        logger.debug('Running applicable transformation logic for {}'.format(
            fpath))
        fpath_transformed = os.path.join(
            dest_dir, ntpath.basename(fpath))
        transformed = _transform(
            fpath, fpath_transformed,
            all_transformation_prefixes, s3_client,
            bucket_name, s3_base, True)
        if transformed:
            success += 1
        else:
            failed += 1

    logger.debug('Success: {}'.format(success))
    logger.debug('Failed: {}'.format(failed))

    transform_end = time.time()

    total_time = readable_time(transform_end - transform_start)
    logger.debug('total time: {}'.format(total_time))


def generate_schema(source_dir, schema_path):
    sys.setrecursionlimit(1600)
    json_objects = [read_json(fpath) for fpath in dir_traverse(source_dir)]
    logger.debug('Inferring schema from files in {}...'.format(source_dir))
    return infer_schema(json_objects)


def main():
    root_dir = os.path.expanduser('~')
    data_dir = os.path.join(root_dir, 'data', 'who')
    source_dir = os.path.join(data_dir, 'deltacon_athena')
    dest_dir = os.path.join(
        data_dir, 'deltacon_athena_transformed')
    source_schema_path = os.path.join(
        data_dir, 'deltacon_athena_schema.json')
    dest_schema_path = os.path.join(
        data_dir, 'deltacon_athena_schema_transformed.json')
    sql_statement_path = os.path.join(
        data_dir, 'deltacon_athena_schema_transformed.sql')
    s3_client = get_s3_client()
    s3_base = 'data/2018/05/09/structured/processed/'\
        'who/deltacon_athena_transformed'
    bucket_name = 'nexscope-safety'

    schema = generate_schema(source_dir, source_schema_path)

    transform(source_dir,
              dest_dir,
              schema, s3_client, bucket_name, s3_base)

    schema = generate_schema(source_dir, dest_schema_path)

    hive_sql_statement = generate_json_table_statement(
        'test', schema,
        data_location='s3://safety',
        database='safety')

    with open(sql_statement_path, 'w') as fp:
        fp.write(hive_sql_statement)

    logger.debug('Hive SQL statement available at {}'.format(sql_statement_path))



if __name__ == "__main__":
    main()
