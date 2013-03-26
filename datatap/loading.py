
DATATAP_REGISTRY = {}

def register_datatap(name, cls):
    cls.get_ident = classmethod(lambda *args: name)
    DATATAP_REGISTRY[name] = cls

def lookup_datatap(name):
    return DATATAP_REGISTRY[name]
