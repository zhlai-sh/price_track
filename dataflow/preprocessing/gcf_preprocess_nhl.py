'''
Dataflow pipeline to retrieve and filter historical transaction data
'''

import utils.io as io
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.filesystem import CompressionTypes


def run(options):
    ''' Defines and executes apache beam pipeline

    :param options: `apache_beam.options.pipeline_options import PipelineOptions` object containing arguments that
    should be used for running the Beam job.
    '''
    with beam.Pipeline(options=options) as pipeline:

        # Load configs
        metadata = io.load_yaml(filename='configs/metadata_formatted_nhl.yml')

        # Specify query
        hist_trans_query = io.load_sql_query('sql/pull_training_data_formatted_nhl.sql').format(
            start_date=metadata['input_start_date'],
            end_date=metadata['input_end_date']
             )

        # Have dataflow pipeline pull the query, and write to CSV
        _ = (pipeline
             | 'Pull data for historical transactions' >>
             beam.io.Read(
                 beam.io.BigQuerySource(
                     query=hist_trans_query,
                     use_standard_sql=True
                     )
                )
             | 'Write data to CSV' >>
             io.DumpToCSV(
                 header=metadata['output_headers'],
                 file_path_prefix=metadata['output_file'],
                 file_name_suffix='',
                 compression_type=CompressionTypes.AUTO,
                 shard_name_template=''
                )
            )

if __name__=='__main__':

    # load input arguments and config from .yaml
    dataflow = io.load_yaml(filename='configs/dataflow.yml')

    # populate list from yaml
    l =[]
    for key in dataflow.keys():
        l.append(key)
        l.append(dataflow[key])

    # execute run
    run(options=PipelineOptions(flags=l, pipeline_type_check=True))