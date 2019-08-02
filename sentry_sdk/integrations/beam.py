from __future__ import absolute_import

import sys
import types
import linecache
from functools import wraps

from sentry_sdk.hub import Hub
from sentry_sdk._compat import reraise
from sentry_sdk.utils import capture_internal_exceptions, event_from_exception
from sentry_sdk.integrations import Integration
from sentry_sdk.integrations.logging import ignore_logger

WRAPPED_FUNC = "_wrapped_{}_"
INSPECT_FUNC = "_inspect_{}"


class BeamIntegration(Integration):
    identifier = "beam"

    def __init__(self, send_source=False):
        self.cached_source = None
        self.cached_file = None
        if send_source:
            import __main__ as main

            filename = main.__file__
            if filename not in linecache.cache:
                linecache.getlines(filename)
            self.cached_source = linecache.cache[filename]
            self.cached_file = filename

    @staticmethod
    def setup_once():
        # type: () -> None
        from apache_beam.transforms.core import DoFn, ParDo  # type: ignore

        ignore_logger("root")
        ignore_logger("bundle_processor.create")

        function_patches = ["process", "start_bundle", "finish_bundle", "setup"]
        for func_name in function_patches:
            setattr(
                DoFn,
                INSPECT_FUNC.format(func_name),
                patched_inspect_process(DoFn, func_name),
            )

        old_init = ParDo.__init__

        def sentry_init_pardo(self, fn, *args, **kwargs):
            # Do not monkey patch init twice
            if not getattr(self, "_sentry_is_patched", False):
                for func_name in function_patches:
                    if not hasattr(fn, func_name):
                        continue
                    wrapped_func = WRAPPED_FUNC.format(func_name)

                    # Check to see if inspect is set and process is not
                    # to avoid monkey patching process twice.
                    # Check to see if function is part of object for
                    # backwards compatibility.
                    process_func = getattr(fn, func_name)
                    inspect_func = getattr(fn, INSPECT_FUNC.format(func_name))
                    if not getattr(inspect_func, "__used__", False) and not getattr(
                        process_func, "__used__", False
                    ):
                        setattr(fn, wrapped_func, process_func)
                        setattr(fn, func_name, _wrap_task_call(process_func))

                self._sentry_is_patched = True
            old_init(self, fn, *args, **kwargs)

        ParDo.__init__ = sentry_init_pardo


def patched_inspect_process(cls, func_name):
    from apache_beam.typehints.decorators import getfullargspec  # type: ignore

    if not hasattr(cls, func_name):
        return None

    def _inspect(self):
        """
        Inspect function overrides the way Beam gets argspec.
        """
        wrapped_func = WRAPPED_FUNC.format(func_name)
        if hasattr(self, wrapped_func):
            process_func = getattr(self, wrapped_func)
        else:
            process_func = getattr(self, func_name)
            setattr(self, func_name, _wrap_task_call(process_func))
            setattr(self, wrapped_func, process_func)
        return getfullargspec(process_func)

    setattr(_inspect, "__used__", True)
    return _inspect


def _wrap_task_call(func):
    """
    Wrap task call with a try catch to get exceptions.
    Pass the client on to raiseException so it can get rebinded.
    """
    client = Hub.current.client

    @wraps(func)
    def _inner(*args, **kwargs):
        try:
            gen = func(*args, **kwargs)
        except Exception:
            raiseException(client)

        if not isinstance(gen, types.GeneratorType):
            return gen
        return _wrap_generator_call(gen, client)

    setattr(_inner, "__used__", True)
    return _inner


def _capture_exception(exc_info, hub):
    """
    Send Beam exception to Sentry.
    """
    integration = hub.get_integration(BeamIntegration)
    if integration:
        client = hub.client
        event, hint = event_from_exception(
            exc_info,
            client_options=client.options,
            mechanism={"type": "beam", "handled": False},
        )
        hub.capture_event(event, hint=hint)


def raiseException(client):
    """
    Raise an exception. If the client is not in the hub, rebind it.
    """
    hub = Hub.current
    if hub.client is None:
        hub.bind_client(client)
    setup_file(hub)
    exc_info = sys.exc_info()
    with capture_internal_exceptions():
        _capture_exception(exc_info, hub)
    reraise(*exc_info)


def _wrap_generator_call(gen, client):
    """
    Wrap the generator to handle any failures.
    """
    while True:
        try:
            yield next(gen)
        except StopIteration:
            break
        except Exception:
            raiseException(client)


def setup_file(hub):
    integration = hub.get_integration(BeamIntegration)
    if integration:
        cached_source = integration.cached_source
        cached_file = integration.cached_file
        if cached_source is not None and cached_file not in linecache.cache:
            linecache.cache[cached_file] = cached_source
