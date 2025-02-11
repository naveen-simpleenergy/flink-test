# src/random_source.py
from pyflink.datastream import SourceFunction
from pyflink.java_gateway import get_gateway
import random

class RandomNumberSource(SourceFunction):
    def __init__(self, count, min_value=1, max_value=100):
        self.count = count
        self.min_value = min_value
        self.max_value = max_value
        self.running = True
        self._j_function = None

    def open(self, runtime_context):
        # Initialize Java function if needed
        gateway = get_gateway()
        self._j_function = gateway.jvm.org.apache.flink.streaming.api.functions.source.FromElementsFunction(
            [random.randint(self.min_value, self.max_value) for _ in range(self.count)]
        )

    def run(self, ctx):
        for _ in range(self.count):
            if not self.running:
                break
            ctx.collect(random.randint(self.min_value, self.max_value))

    def cancel(self):
        self.running = False

    def get_java_function(self):
        if self._j_function is None:
            raise RuntimeError("Java function not initialized. Ensure 'open' is called.")
        return self._j_function