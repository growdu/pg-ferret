import json
import argparse
import time
from collections import defaultdict
from opentelemetry import trace
from opentelemetry.trace import SpanKind, SpanContext, TraceFlags, TraceState, NonRecordingSpan, set_span_in_context
from opentelemetry.sdk.trace import TracerProvider, Resource
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import SERVICE_NAME, Resource as SDKResource

def build_thread_traces(spans):
    """
    按 thread_id 分组 span，并按 startTimeUnixNano 排序
    """
    threads = defaultdict(list)
    for span in spans:
        threads[span["threadId"]].append(span)
    for t in threads:
        threads[t].sort(key=lambda x: x["startTimeUnixNano"])
    return threads

def send_span(span, tracer, parent_ctx=None):
    # 截取 traceId/spanId 长度，保证 OTLP 可用
    trace_id = int(span["traceId"][:32], 16)
    span_id = int(span["spanId"][:16], 16)

    current_ctx = set_span_in_context(
        NonRecordingSpan(
            SpanContext(
                trace_id=trace_id,
                span_id=span_id,
                is_remote=False,
                trace_flags=TraceFlags(1),
                trace_state=TraceState(),
            )
        )
    )

    # 使用当前时间生成 span 时间
    duration_ns = int(span["endTimeUnixNano"] - span["startTimeUnixNano"])
    now_ns = int(time.time() * 1e9)
    start_ns = now_ns
    end_ns = now_ns + duration_ns

    with tracer.start_span(
        span["name"],
        context=parent_ctx if parent_ctx else current_ctx,
        kind=SpanKind.INTERNAL,
        start_time=start_ns
    ) as ot_span:
        for attr in span.get("attributes", []):
            ot_span.set_attribute(attr["key"], attr["value"])
        ot_span.end(end_time=end_ns)
        return set_span_in_context(ot_span)

def import_spans(file_path, tracer):
    spans = []
    with open(file_path, "r") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            span_json = json.loads(line)
            if not span_json.get("parentSpanId"):
                span_json["parentSpanId"] = "0000000000000000"
            spans.append(span_json)

    threads = build_thread_traces(spans)

    # 遍历线程级 trace，自动生成调用树
    for thread_id, thread_spans in threads.items():
        stack = []
        for span in thread_spans:
            parent_ctx = stack[-1]["ctx"] if stack else None
            ctx = send_span(span, tracer, parent_ctx=parent_ctx)

            # push 当前 span 到 stack
            stack.append({
                "end_ns": int(span["endTimeUnixNano"]),
                "ctx": ctx
            })

            # pop 栈里已经结束的 span
            while stack and stack[-1]["end_ns"] <= span["startTimeUnixNano"]:
                stack.pop()

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--file", required=True, help="Path to spans.json")
    parser.add_argument("--endpoint", default="http://localhost:4317", help="OTLP endpoint")
    args = parser.parse_args()

    # 添加 service.name = "postgres"
    resource = SDKResource(attributes={SERVICE_NAME: "postgres"})
    provider = TracerProvider(resource=resource)
    exporter = OTLPSpanExporter(endpoint=args.endpoint)
    provider.add_span_processor(BatchSpanProcessor(exporter))
    trace.set_tracer_provider(provider)
    tracer = trace.get_tracer("ebpf-json-importer")

    import_spans(args.file, tracer)

    # flush 确保全部发送
    provider.shutdown()
    print("All spans successfully sent to Tempo with service.name='postgres'.")

if __name__ == "__main__":
    main()
