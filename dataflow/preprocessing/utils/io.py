'''
Utility helpers for dataflow pipeline
'''

import apache_beam as beam
import yaml

def load_yaml(filename):
    ''' Loads YAML file.

    :param filename: `str` Path to YAML file.
    :return: `dict` Contents of YAML file.
    '''
    with open(filename, 'r') as fp:
        content = yaml.load(fp)

    return content


def load_sql_query(filename):
    ''' Loads query from `.sql` file.

    :param filename: `str` Path to SQL query file.
    :return: `str` SQL query string.
    '''

    with open(filename, 'r') as fp:
        query_string = fp.read()

    return query_string


class DictToCSV(beam.DoFn):
    ''' Converts a dictionary to a CSV string'''

    def __init__(self,
                 header):
        self.header = header

    def process(self,
                element):
        ''' Converts dictionary to CSV string.
        :param element: `dict` Row data.
        :return: `str` CSV string.
        '''

        # Lazily initiate column headers.
        if self.header is None:
            self.header = list(element.keys())

        values = []

        for key in self.header:
            # to avoid issues with a comma delimited being present in the string, encapsulate strings in double quotes
            if isinstance(element[key], str):
                values.append("\"{}\"".format(element[key]))
            else:
                values.append("{}".format(element[key]))

        yield ",".join(values)


class DumpToCSV(beam.PTransform):
    ''' Dumps PCollection to a CSV file'''

    def __init__(self,
                 header,
                 file_path_prefix,
                 file_name_suffix,
                 compression_type,
                 shard_name_template):

        self.header = header
        self.file_path_prefix = file_path_prefix
        self.file_name_suffix = file_name_suffix
        self.compression_type = compression_type
        self.shard_name_template = shard_name_template


    def expand(self,pcoll):

        return (pcoll
                | 'Convert rows to CSV strings' >>
                beam.ParDo(
                    DictToCSV(header=self.header)
                )
                | 'Write to `{}`'.format(self.file_path_prefix) >>
                beam.io.WriteToText(
                    file_path_prefix=self.file_path_prefix,
                    file_name_suffix=self.file_name_suffix,
                    append_trailing_newlines=True,
                    compression_type=self.compression_type,
                    shard_name_template=self.shard_name_template,
                    header=','.join(self.header).encode()
                )
                )
