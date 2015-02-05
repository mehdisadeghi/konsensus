import optparse

from konsensus.test_helper import instance_factory

if __name__ == '__main__':
    """Helper to make konsensus servers with fake data and run them"""
    parser = optparse.OptionParser()
    parser.add_option('-c', '--count', type='int', help='Number of Konsensus servers to be created')
    parser.set_defaults(count=1)
    parser.parse_args()

    opts, args = parser.parse_args()
    instance_factory(opts.count)
