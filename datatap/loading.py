
DATATAP_REGISTRY = {}

def register_datatap(name, cls):
    DATATAP_REGISTRY[name] = cls
