import os
import sys
from pyflink.common.configuration import Configuration

# Add the src directory to Python path
current_dir = os.path.dirname(os.path.abspath(__file__))
src_dir = os.path.join(current_dir, 'src')
sys.path.append(src_dir)

def setup_environment():
    # Set environment variables
    os.environ['PYFLINK_CLIENT_EXECUTABLE'] = sys.executable
    
    # Create configuration
    config = Configuration()
    
    # Set necessary configuration parameters
    config.set_string("python.executable", sys.executable)
    
    return config

if __name__ == '__main__':
    try:
        # Setup environment
        config = setup_environment()
        
        # Import after environment setup
        from src.main import create_fraud_detection_job
        
        # Get the Flink job
        env = create_fraud_detection_job()
        
        # Execute the job
        env.execute("Fraud Detection")
        
    except Exception as e:
        print(f"Error executing Flink job: {str(e)}")
        raise