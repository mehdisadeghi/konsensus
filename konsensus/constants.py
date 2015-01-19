"""
    konsensus
    ~~~~~~~~~

    This file is part of konsensus project.
"""

# For heartbeat messages
HEARTBEAT_TOPIC = 10010

# New message of any kind
NEW_MESSAGE_TOPIC = 10011

# New dataset available
NEW_DATASET_TOPIC = 10012

# New operation submitted
NEW_OPERATION_TOPIC = 10013
NEW_OPERATION_SIG = 'operation.new'

# Delegate
DELEGATE_TOPIC = 10014
DELEGATE_SIG = 'delegate'
DELEGATE_ACCEPTED_TOPIC = 10015
#DELEGATE_ACCEPTED_SIG = 'delegate.accepted'

# Peer accepting delegate
PEER_ACCEPTED_DELEGATE_SIG = 'peer.accepted.delegate'

# Shout over the network
PUBLISH = 'publish'