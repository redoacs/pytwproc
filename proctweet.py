import gzip
import json
import sys
import multiprocessing
import os
from functools import reduce, partial


def get_tweets(lines):
    for line in filter(lambda l: l != "", map(str.strip, lines)):
        try:
            tweet = json.loads(line)
            yield tweet
        except json.JSONDecodeError:
            print("Ignoring malformed tweet", line, file=sys.stderr)


def process_tweets_file(filter_function, tweet_map_function, reduce_function, reduce_init, filename):
    print("Reading file", filename)
    with gzip.open(filename, "rt") as file:
        map_result = map(tweet_map_function, filter(filter_function, get_tweets(file.readlines())))
    return reduce(reduce_function, map_result, reduce_init)


def process_tweets_file_uc(filter_function, tweet_map_function, reduce_function, reduce_init, filename):
    print("Reading file", filename)
    with open(filename, "r") as file:
        map_result = map(tweet_map_function, filter(filter_function, get_tweets(file.readlines())))
    return reduce(reduce_function, map_result, reduce_init)


def get_absolute_paths(data_path, n_read_files):
    files = sorted(os.listdir(data_path))
    n_files = len(files)
    print(n_files, "compressed files.")
    absolute_paths = list(map(lambda fp: data_path + fp, files))
    return absolute_paths[:n_read_files]


def process_tweets(tweet_filter_func, tweet_map_func, reduce_file_func, reduce_file_init, reduce_total_func,
                   reduce_total_init, absolute_paths):
    file_map_function = partial(process_tweets_file, tweet_filter_func, tweet_map_func, reduce_file_func,
                                reduce_file_init)

    with multiprocessing.Pool() as pool:
        results_per_file = pool.map(file_map_function, absolute_paths)

    return reduce(reduce_total_func, results_per_file, reduce_total_init)


def process_tweets_uc(tweet_filter_func, tweet_map_func, reduce_file_func, reduce_file_init, reduce_total_func,
                   reduce_total_init, absolute_paths):
    file_map_function = partial(process_tweets_file_uc, tweet_filter_func, tweet_map_func, reduce_file_func,
                                reduce_file_init)

    with multiprocessing.Pool() as pool:
        results_per_file = pool.map(file_map_function, absolute_paths)

    return reduce(reduce_total_func, results_per_file, reduce_total_init)
