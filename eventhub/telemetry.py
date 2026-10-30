import json
import logging

from opentelemetry import trace
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor


def setup(role, config):
    provider = TracerProvider(
        resource=Resource.create({"service.name": "eventhub-" + role, "service.version": "0.1.0"})
    )
    if config.otel_exporter_otlp_endpoint:
        from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter

        provider.add_span_processor(
            BatchSpanProcessor(OTLPSpanExporter(endpoint=config.otel_exporter_otlp_endpoint, timeout=2))
        )
    trace.set_tracer_provider(provider)
    return provider


def log(action, **values):
    logging.getLogger("eventhub").info(json.dumps({"action": action, **values}, default=str))


def span(name, **attributes):
    return trace.get_tracer("eventhub").start_as_current_span(
        name, attributes={k: str(v) for k, v in attributes.items()}
    )
