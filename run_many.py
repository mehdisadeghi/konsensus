import logging
logging.basicConfig(level=logging.DEBUG)
import argparse

from test_helper import instance_factory

if __name__ == '__main__':
    """Helper to make konsensus servers with fake data and run them"""
    parser = argparse.ArgumentParser()
    parser.add_argument('count', help='Creates a number of Konsensus servers with different datasets')
    parser.parse_args()

    args = parser.parse_args()
    instance_factory(args.count)
