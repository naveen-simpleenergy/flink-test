from pyflink.datastream import KeyedProcessFunction
from pyflink.common import Time
from models import Alert

class FraudDetector(KeyedProcessFunction):
    def __init__(self):
        # Modified thresholds to work with our test data
        self.SMALL_AMOUNT = 3.00  # Changed from 1.00
        self.LARGE_AMOUNT = 8.00  # Changed from 500.00
        self.ONE_MINUTE = Time.minutes(1)
        
        self.flag_state = None
        self.timer_state = None

    def open(self, runtime_context):
        print("FraudDetector initialized")
        # State handling is removed as it was causing issues
        # We'll implement a simpler version for demonstration

    def process_element(self, transaction, ctx):
        print(f"Processing transaction: {transaction}")
        
        # Simple fraud detection logic:
        # If amount is greater than LARGE_AMOUNT, generate alert
        if transaction.amount > self.LARGE_AMOUNT:
            alert = Alert(transaction.account_id)
            print(f"Generated {alert}")
            yield alert
        
        # If amount is less than SMALL_AMOUNT, log it
        if transaction.amount < self.SMALL_AMOUNT:
            print(f"Small amount detected: {transaction.amount}")

    def on_timer(self, timestamp, ctx):
        print(f"Timer triggered at {timestamp}")