import hashlib
import sys
import traceback

from sentry_sdk.utils import (
    current_stacktrace,
)

from datetime import datetime, timedelta


def check_span_performance(self, span, options, span_counts, span_times, span_exceptions, span_first, span_last, spans_involved):
    op = span.op
    if not span.description:
        return

    if op != 'snuba_snql.run':
        return

    count_threshold = options.get('count')
    time_threshold = options.get('cumulative_time')
    
    desc = span.description
    hash = hashlib.sha1((str(op) + str(desc)).encode('utf-8')).hexdigest()
    time = span.timestamp - span.start_timestamp

    span_counts[hash] = span_counts.get(hash, 0) + 1
    span_times[hash] = span_times.get(hash, timedelta(0)) + time

    if hash not in span_first:
        span_first[hash] = span.span_id

    if hash not in spans_involved:
        spans_involved[hash] = []
    
    spans_involved[hash] += [span.span_id]


    span_last[hash] = span.span_id
    stack = traceback.extract_stack()
    current_st = current_stacktrace(with_locals=False)

    if not span_exceptions.get(hash, False):
        if (span_counts[hash] > count_threshold and span_times[hash] > timedelta(milliseconds=time_threshold)):
            exception_string = f"Extraneous Spans:: {op} - {desc[:48]}... | {hash[:16]}"
            try:
                raise Exception(exception_string)
            except Exception as e:
                tb = sys.exc_info()[2] # For exc, use stack for app.
                e.replacement_frames = stack
                e.current_st = current_st
                span_exceptions[hash] = {'hash': hash, 'exception': e, 'op': op,'desc': desc, 'stack': stack, 'span': span, 'tb': tb, 'message': exception_string}