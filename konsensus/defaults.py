

class DefaultConfig(object):
    API_PORT = 4242
    HEARTBEAT_PORT = 4343
    HEARTBEAT_INTERVAL = 2500  # Heartbeat delay, mses

    ZMQ_HEARTBEAT_TOPIC = 10001

    BOOTSTRAP_HOST = "127.0.0.1"
    BOOTSTRAP_PORT = 8468

