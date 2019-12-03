from sentry_sdk import capture_message, push_scope
from sentry_sdk.hub import Hub
from sentry_sdk.utils import capture_internal_exceptions

from sentry_sdk._types import MYPY

if MYPY:
    from typing import Any


def _set_app_properties():
    # type: () -> None
    """
    Set properties in driver that propagate to worker processes, allowing for workers to have access to those properties.
    This allows worker integration to have access to app_name and application_id.
    """
    from pyspark import SparkContext

    sparkContext = SparkContext._active_spark_context
    if sparkContext:
        sparkContext.setLocalProperty("sentry_app_name", sparkContext.appName)
        sparkContext.setLocalProperty(
            "sentry_application_id", sparkContext.applicationId
        )


class SparkListener(object):
    def onApplicationEnd(self, applicationEnd):
        # type: (Any) -> None
        pass

    def onApplicationStart(self, applicationStart):
        # type: (Any) -> None
        pass

    def onBlockManagerAdded(self, blockManagerAdded):
        # type: (Any) -> None
        pass

    def onBlockManagerRemoved(self, blockManagerRemoved):
        # type: (Any) -> None
        pass

    def onBlockUpdated(self, blockUpdated):
        # type: (Any) -> None
        pass

    def onEnvironmentUpdate(self, environmentUpdate):
        # type: (Any) -> None
        pass

    def onExecutorAdded(self, executorAdded):
        # type: (Any) -> None
        pass

    def onExecutorBlacklisted(self, executorBlacklisted):
        # type: (Any) -> None
        pass

    def onExecutorBlacklistedForStage(self, executorBlacklistedForStage):
        # type: (Any) -> None
        pass

    def onExecutorMetricsUpdate(self, executorMetricsUpdate):
        # type: (Any) -> None
        pass

    def onExecutorRemoved(self, executorRemoved):
        # type: (Any) -> None
        pass

    def onJobEnd(self, jobEnd):
        # type: (Any) -> None
        pass

    def onJobStart(self, jobStart):
        # type: (Any) -> None
        pass

    def onNodeBlacklisted(self, nodeBlacklisted):
        # type: (Any) -> None
        pass

    def onNodeBlacklistedForStage(self, nodeBlacklistedForStage):
        # type: (Any) -> None
        pass

    def onNodeUnblacklisted(self, nodeUnblacklisted):
        # type: (Any) -> None
        pass

    def onOtherEvent(self, event):
        # type: (Any) -> None
        pass

    def onSpeculativeTaskSubmitted(self, speculativeTask):
        # type: (Any) -> None
        pass

    def onStageCompleted(self, stageCompleted):
        # type: (Any) -> None
        pass

    def onStageSubmitted(self, stageSubmitted):
        # type: (Any) -> None
        pass

    def onTaskEnd(self, taskEnd):
        # type: (Any) -> None
        pass

    def onTaskGettingResult(self, taskGettingResult):
        # type: (Any) -> None
        pass

    def onTaskStart(self, taskStart):
        # type: (Any) -> None
        pass

    def onUnpersistRDD(self, unpersistRDD):
        # type: (Any) -> None
        pass

    class Java:
        implements = ["org.apache.spark.scheduler.SparkListenerInterface"]


class SentryListener(SparkListener):
    def __init__(self):
        # type: () -> None
        self.hub = Hub.current
        _set_app_properties()

    def onJobStart(self, jobStart):
        # type: (Any) -> None
        message = "Job {} Started".format(jobStart.jobId())
        self.hub.add_breadcrumb(level="info", message=message)
        _set_app_properties()

    def onJobEnd(self, jobEnd):
        # type: (Any) -> None
        level = ""
        message = ""
        data = {"result": jobEnd.jobResult().toString()}

        if jobEnd.jobResult().toString() == "JobSucceeded":
            level = "info"
            message = "Job {} Ended".format(jobEnd.jobId())
        else:
            level = "warning"
            message = "Job {} Failed".format(jobEnd.jobId())

        self.hub.add_breadcrumb(level=level, message=message, data=data)

    def onStageSubmitted(self, stageSubmitted):
        # type: (Any) -> None
        stageInfo = stageSubmitted.stageInfo()
        message = "Stage {} Submitted".format(stageInfo.stageId())
        data = {"attemptId": stageInfo.attemptId(), "name": stageInfo.name()}
        self.hub.add_breadcrumb(level="info", message=message, data=data)
        _set_app_properties()

    def onStageCompleted(self, stageCompleted):
        # type: (Any) -> None
        from py4j.protocol import Py4JJavaError  # type: ignore

        stageInfo = stageCompleted.stageInfo()
        message = ""
        level = ""
        data = {"attemptId": stageInfo.attemptId(), "name": stageInfo.name()}

        # Have to Try Except because stageInfo.failureReason() is typed with Scala Option
        try:
            data["reason"] = stageInfo.failureReason().get()
            message = "Stage {} Failed".format(stageInfo.stageId())
            level = "warning"
        except Py4JJavaError:
            message = "Stage {} Completed".format(stageInfo.stageId())
            level = "info"

        self.hub.add_breadcrumb(level=level, message=message, data=data)


class SparkStreamingQueryListener(object):
    def onQueryProgress(self, event):
        # type: (Any) -> None
        pass

    def onQueryStarted(self, event):
        # type: (Any) -> None
        pass

    def onQueryTerminated(self, event):
        # type: (Any) -> None
        pass

    class Java:
        implements = ["org.apache.spark.sql.hive.thriftserver.DummyStreamingQueryListener"]


class SentryStreamingQueryListener(SparkStreamingQueryListener):
    def __init__(self):
        # type: () -> None
        self.hub = Hub.current
        self.name = None

    def onQueryStarted(self, event):
        # type: (Any) -> None
        self.name = event.name()

        message = "Query {} started".format(self.name)
        data = {"runId": event.runId()}
        self.hub.add_breadcrumb(message=message, data=data)

    def onQueryProgress(self, event):
        # type: (Any) -> None
        progress = event.progress()

        self.name = progress.name()

        message = "Query {} progressed".format(self.name)
        data = {
            "runId": progress.runId(),
            "timestamp": progress.timestamp(),
            "json": progress.json(),
        }
        self.hub.add_breadcrumb(message=message, data=data)

    def onQueryTerminated(self, event):
        # type: (Any) -> None
        from py4j.protocol import Py4JJavaError

        # Have to Try Except because event.exception() is typed with Scala Option
        try:
            with push_scope() as scope:
                with capture_internal_exceptions():
                    message = event.exception()
                    scope.set_tag("name", self.name)
                    scope.set_tag("runId", event.runId())
                    scope.set_tag("id", event.id())

                    capture_message(message=message, level="error")

        except Py4JJavaError:
            message = "Query {} terminated".format(self.name)
            data = {"runId": event.runId()}
            self.hub.add_breadcrumb(message=message, data=data)
