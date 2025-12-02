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


def build_tree(spans):
    """
    根据 parentSpanId 构建多叉树
    """
    by_id = {s["spanId"]: s for s in spans}
    children = defaultdict(list)

    roots = []

    for s in spans:
        parent = s.get("parentSpanId")
        if parent and parent != "0000000000000000" and parent in by_id:
            children[parent].append(s)
        else:
            roots.append(s)

    return roots, children


def send_span(span, tracer, children_map, parent_ctx, time_offset_ns):
    trace_id = int(span["traceId"], 16)
    span_id = int(span["spanId"], 16)

    # 构造 span context
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

    # 计算平移后的时间
    start_ns = span["startTimeUnixNano"] + time_offset_ns
    end_ns = span["endTimeUnixNano"] + time_offset_ns

    with tracer.start_span(
        span["name"],
        context=parent_ctx if parent_ctx else current_ctx,
        kind=SpanKind.INTERNAL,
        start_time=start_ns,
    ) as ot_span:

        # 设置属性
        for attr in span.get("attributes", []):
            ot_span.set_attribute(attr["key"], attr["value"])

        # 递归生成子 span
        for child in children_map.get(span["spanId"], []):
            send_span(child, tracer, children_map, set_span_in_context(ot_span), time_offset_ns)

        ot_span.end(end_time=end_ns)


def import_spans(file_path, tracer):
    spans = []
    with open(file_path, "r") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            s = json.loads(line)
            if not s.get("parentSpanId"):
                s["parentSpanId"] = "0000000000000000"
            spans.append(s)

    # 构建调用树
    roots, children_map = build_tree(spans)

    # 时间平移
    now_ns = int(time.time() * 1e9)
    first_ns = min(s["startTimeUnixNano"] for s in spans)
    time_offset_ns = now_ns - first_ns

    # 每个 root span 都独立生成子树
    for root in roots:
        send_span(root, tracer, children_map, None, time_offset_ns)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--file", required=True)
    parser.add_argument("--endpoint", default="http://localhost:4317")
    args = parser.parse_args()

    resource = SDKResource(attributes={SERVICE_NAME: "postgres"})
    provider = TracerProvider(resource=resource)
    exporter = OTLPSpanExporter(endpoint=args.endpoint)
    provider.add_span_processor(BatchSpanProcessor(exporter))
    trace.set_tracer_provider(provider)
    tracer = trace.get_tracer("ebpf-json-importer")

    import_spans(args.file, tracer)

    provider.shutdown()
    print("All spans successfully sent to Tempo with service.name='postgres'.")


if __name__ == "__main__":
    main()
