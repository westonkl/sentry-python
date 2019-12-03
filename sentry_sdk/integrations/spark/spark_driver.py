from sentry_sdk.integrations.spark.listener import (
    SentryListener,
    SentryStreamingQueryListener,
)

from sentry_sdk import configure_scope
from sentry_sdk.hub import Hub
from sentry_sdk.integrations import Integration
from sentry_sdk.utils import capture_internal_exceptions, logger

from sentry_sdk._types import MYPY

if MYPY:
    from typing import Any
    from typing import Optional

    from sentry_sdk._types import Event, Hint


class SparkIntegration(Integration):
    identifier = "spark"

    @staticmethod
    def setup_once():
        # type: () -> None
        patch_spark_context_init()
        patch_spark_session_getOrCreate()


def _start_sentry_spark_listener(sc):
    # type: (Any) -> None
    """
    Start java gateway server to add custom `SparkListener`
    """
    from pyspark.java_gateway import ensure_callback_server_started

    gw = sc._gateway
    ensure_callback_server_started(gw)
    listener = SentryListener()
    sc._jsc.sc().addSparkListener(listener)


def _add_sentry_metadata(sc):
    # type: (Any) -> None
    with configure_scope() as scope:

        @scope.add_event_processor
        def process_event(event, hint):
            # type: (Event, Hint) -> Optional[Event]
            with capture_internal_exceptions():
                if Hub.current.get_integration(SparkIntegration) is None:
                    return event

                event.setdefault("user", {}).setdefault("id", sc.sparkUser())

                event.setdefault("tags", {}).setdefault(
                    "executor.id", sc._conf.get("spark.executor.id")
                )
                event["tags"].setdefault(
                    "spark-submit.deployMode", sc._conf.get("spark.submit.deployMode")
                )
                event["tags"].setdefault(
                    "driver.host", sc._conf.get("spark.driver.host")
                )
                event["tags"].setdefault(
                    "driver.port", sc._conf.get("spark.driver.port")
                )
                event["tags"].setdefault("spark_version", sc.version)
                event["tags"].setdefault("app_name", sc.appName)
                event["tags"].setdefault("application_id", sc.applicationId)
                event["tags"].setdefault("master", sc.master)
                event["tags"].setdefault("spark_home", sc.sparkHome)

                event.setdefault("extra", {}).setdefault("web_url", sc.uiWebUrl)

            return event


def patch_spark_context_init():
    # type: () -> None
    from pyspark import SparkContext

    sparkContext = SparkContext._active_spark_context
    if sparkContext:
        _start_sentry_spark_listener(sparkContext)
        _add_sentry_metadata(sparkContext)
        return

    spark_context_init = SparkContext._do_init

    def _sentry_patched_spark_context_init(self, *args, **kwargs):
        # type: (SparkContext, *Any, **Any) -> Optional[Any]
        init = spark_context_init(self, *args, **kwargs)

        if Hub.current.get_integration(SparkIntegration) is None:
            return init

        _start_sentry_spark_listener(self)
        _add_sentry_metadata(self)

        return init

    SparkContext._do_init = _sentry_patched_spark_context_init


def _start_sentry_streaming_query_listener(sparkSession):
    # type: (Any) -> None
    """
    Start java gateway server to add custom `StreamingQueryListener`
    """
    from py4j.protocol import Py4JJavaError, Py4JError
    from pyspark.java_gateway import ensure_callback_server_started

    gw = sparkSession.sparkContext._gateway
    ensure_callback_server_started(gw)
    listener = SentryStreamingQueryListener()
    try:  
        sparkSession.streams.addListener(listener)
    except (Py4JError, Py4JJavaError):
        logger.warning("Streaming Query Listener not added", exec_info=True)

def patch_spark_session_getOrCreate():
    # type: () -> None
    from pyspark.sql import SparkSession

    spark_session_getOrCreate = SparkSession.getOrCreate

    def _sentry_patched_spark_session_getOrCreate(self, *args, **kwargs):
        # type: (SparkSession, *Any, **Any) -> Optional[Any]
        session = spark_session_getOrCreate(self, *args, **kwargs)

        if Hub.current.get_integration(SparkIntegration) is None:
            return session

        _start_sentry_streaming_query_listener(self)

        return session

    SparkSession.getOrCreate = _sentry_patched_spark_session_getOrCreate
