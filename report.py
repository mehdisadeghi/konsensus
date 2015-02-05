import os
import optparse

import zerorpc

from konsensus.test_helper import random_instance_factory


if __name__ == '__main__':
    """Helper to make konsensus servers with fake data and run them"""
    parser = optparse.OptionParser('Usage %prog [-c | --count | -r | --repeat | -o | --operation] arguements')
    parser.add_option('-c', '--count', type='int', help='Number of Konsensus servers to be created')
    parser.add_option('-r', '--repeat', type='int', help='Number of times to repeat the operation')
    parser.add_option('-o', '--operation', help='Name of the operation')
    parser.parse_args()

    opts, args = parser.parse_args()

    if not opts.repeat:
        parser.error('Repeat count not given')

    if not opts.count:
        parser.error('Number of instances not given')

    if not opts.operation:
        parser.error('Operation not given')

    pid, configs = random_instance_factory(opts.count, log_level='ERROR')
    operation_ids = []
    # Run usecase1 any number of times that is given
    api = zerorpc.Client()
    api_endpoint = 'tcp://127.0.0.1:%s' % configs[0]['API_PORT']
    api.connect(api_endpoint)

    for i in xrange(opts.repeat):
        o_id = api.__call__(opts.operation, *args)
        operation_ids.append(o_id)

    # Let the operations to complete
    import gevent
    gevent.sleep(.3)

    average = 0
    for o in operation_ids:
        op = api.get_operation(o)
        average += op['duration']

    print average / len(operation_ids)

    for p in pid:
        os.kill(p, 9)