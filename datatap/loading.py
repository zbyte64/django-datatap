
DATATAP_REGISTRY = {}

def register_datatap(name, cls):
    cls.get_ident = classmethod(lambda *args: name)
    DATATAP_REGISTRY[name] = cls

def lookup_datatap(name):
    return DATATAP_REGISTRY[name]

def get_datatap_registry():
    return DATATAP_REGISTRY

def autodiscover():
    """
    Auto-discover INSTALLED_APPS datataps.py modules and fail silently when
    not present. This forces an import on them to register any datatap bits they
    may want.
    """

    from django.conf import settings
    from django.utils.importlib import import_module
    from django.utils.module_loading import module_has_submodule

    for app in settings.INSTALLED_APPS:
        mod = import_module(app)
        # Attempt to import the app's datataps module.
        try:
            import_module('%s.datataps' % app)
        except:
            # Decide whether to bubble up this error. If the app just
            # doesn't have an admin module, we can ignore the error
            # attempting to import it, otherwise we want it to bubble up.
            if module_has_submodule(mod, 'datataps'):
                raise
