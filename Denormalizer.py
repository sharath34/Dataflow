import apache_beam as beam
import argparse
import json

from collections import OrderedDict
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions


def transform_stream(stream):
    stream_dict = OrderedDict()
    stream_dict['user_id'] = stream['user_id']
    stream_dict['cached'] = stream['cached']
    stream_dict['timestamp'] = stream['timestamp']
    stream_dict['source_uri'] = stream['source_uri']
    stream_dict['track_id'] = stream['track_id']
    stream_dict['source'] = stream['source']
    stream_dict['length'] = stream['length']
    stream_dict['version'] = stream['version']
    stream_dict['device_type'] = stream['device_type']
    stream_dict['message'] = stream['message']
    stream_dict['os'] = stream['os']
    stream_dict['stream_country'] = stream['stream_country']
    stream_dict['report_date'] = stream['report_date']
    return stream['track_id'], stream_dict


def transform_track(track):
    track_dict = OrderedDict()
    track_dict['isrc'] = track['isrc']
    track_dict['album_code'] = track['album_code']
    return track['track_id'], track_dict


def transform_user(user):
    user_dict = OrderedDict()
    user_dict['product'] = user['product']
    user_dict['country'] = user['country']
    user_dict['region'] = user['region']
    user_dict['zip_code'] = user['zip_code']
    user_dict['access'] = user['access']
    user_dict['gender'] = user['gender']
    user_dict['partner'] = user['partner']
    user_dict['referral'] = user['referral']
    user_dict['type'] = user['type']
    user_dict['birth_year'] = user['birth_year']
    return user['user_id'], user_dict


def join_track_with_pair(element):
    streams = element[1][0]
    track = element[1][1][0]
    join_pair = list()
    for stream in streams:
        stream.update(track)
        join_pair.append((stream['user_id'], stream))
    return join_pair


def join_user(element):
    twos = element[1][0]
    user = element[1][1][0]
    joined = list()
    for two in twos:
        two.update(user)
        joined.append(two)
    return joined

PROJECT = '*your_project*'
BUCKET = '*your bucket*'


def run(argv=None):
    argv = [
        '--project={0}'.format(PROJECT),
        '--job_name=streamjob',
        '--save_main_session',
        '--staging_location=gs://{0}/staging/'.format(BUCKET),
        '--temp_location=gs://{0}/staging/'.format(BUCKET),
        '--runner=DataflowRunner'
    ]
    parser = argparse.ArgumentParser()
    parser.add_argument('--streams',
                        dest='streams',
                        default='gs://***',
                        help='Streams file to process.')
    parser.add_argument('--tracks',
                        dest='tracks',
                        default='gs://***',
                        help='Tracks file to process.')
    parser.add_argument('--users',
                        dest='users',
                        default='gs://***',
                        help='Users file to process.')
    parser.add_argument('--output',
                        dest='output',
                        default='gs://***',
                        help='Output file to write results to.')

    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True
    p = beam.Pipeline(options=pipeline_options)

    stream_pairs = (p | 'read_streams' >> ReadFromText(known_args.streams)
                    | 'parse_streams_to_json' >> beam.Map(json.loads)
                    | 'pair_streams_with_user_id' >> beam.Map(transform_stream))

    track_pairs = (p | 'read_tracks' >> ReadFromText(known_args.tracks)
                   | 'parse_tracks_to_json' >> beam.Map(json.loads)
                   | 'pair_tracks_with_track_id' >> beam.Map(transform_track))

    user_pairs = (p | 'read_users' >> ReadFromText(known_args.users)
                  | 'parse_users_to_json' >> beam.Map(json.loads)
                  | 'pair_user_with_user_id' >> beam.Map(transform_user))
    # join streams with tracks, pair result with user_id
    intermediate_result_pairs = ([pair for pair in (stream_pairs, track_pairs)]
                                 | 'group_stream_with_track' >> beam.CoGroupByKey()
                                 | 'pair_join_two_with_user_id' >> beam.FlatMap(join_track_with_pair))
    # join intermediate result with users
    final_result = ([pair for pair in (intermediate_result_pairs, user_pairs)]
                    | 'group_intermediate_result_with_user' >> beam.CoGroupByKey()
                    | 'join_all' >> beam.FlatMap(join_user))
    (final_result | 'convert_json' >> beam.Map(json.dumps)
     | 'write' >> WriteToText(known_args.output, num_shards=1))
    p.run().wait_until_finish()


if __name__ == '__main__':
    run()
