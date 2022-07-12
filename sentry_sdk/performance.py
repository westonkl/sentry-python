import hashlib

from sentry_sdk.utils import (
    current_stacktrace,
)

from datetime import timedelta


def analyze_duplicate_spans(span, options, span_durations, spans_involved, performance_exceptions):
    op = span.op
    if not span.description:
        return

    count_threshold = options.get('count')
    time_threshold = options.get('cumulative_time')
    allowed_span_ops = options.get('allowed_span_ops')
    ignored_span = op not in allowed_span_ops

    if count_threshold is None or time_threshold is None or ignored_span:
        return
    
    desc = span.description
    signature = (str(op) + str(desc)).encode('utf-8')
    hash = hashlib.sha1(signature).hexdigest()
    span_duration = span.timestamp - span.start_timestamp

    span_durations[hash] = span_durations.get(hash, timedelta(0)) + span_duration

    if hash not in spans_involved:
        spans_involved[hash] = []
    
    spans_involved[hash] += [span.span_id]
    span_counts = len(spans_involved[hash])


    if not performance_exceptions.get(hash, False):
        if (span_counts > count_threshold and span_durations[hash] > timedelta(milliseconds=time_threshold)):
            exception_string = f"Extraneous Spans: {op} - {desc[:48]}... | {hash[:16]}"
            performance_stacktrace = current_stacktrace(with_locals=False)
            try:
                raise Exception(exception_string)
            except Exception as e:
                e.performance_stacktrace = performance_stacktrace
                performance_exceptions[hash] = {'hash': hash, 'exception': e, 'op': op,'desc': desc, 'span': span}