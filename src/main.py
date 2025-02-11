from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import NumberSequenceSource
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.common.typeinfo import Types
from fraud_detector import FraudDetector
from models import Transaction
import os

def create_fraud_detection_job():
    # Create the execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    
    # Set up Python dependencies
    env.add_python_file(os.path.join(os.path.dirname(__file__), 'fraud_detector.py'))
    env.add_python_file(os.path.join(os.path.dirname(__file__), 'models.py'))
    
    # Set the parallelism
    env.set_parallelism(1)
    
    # Create a watermark strategy
    watermark_strategy = WatermarkStrategy.for_monotonous_timestamps()
    
    # Create a simple source with a range that will trigger our rules
    source = NumberSequenceSource(1, 10)
    
    # Create the transaction stream
    transactions = env.from_source(
        source=source,
        watermark_strategy=watermark_strategy,
        source_name="transaction-source"
    )

    # Transform the source into Transaction objects
    def create_transaction(x):
        transaction = Transaction(x, float(x))
        print(f"Created {transaction}")
        return transaction

    transaction_stream = transactions.map(
        create_transaction,
        output_type=Types.PICKLED_BYTE_ARRAY()
    ).name('create-transactions')

    # Apply fraud detection
    alerts = transaction_stream \
        .key_by(lambda x: x.account_id) \
        .process(FraudDetector()) \
        .name('fraud-detector')

    # Add sink with better formatting
    alerts.print().name('fraud-alerts')

    return env