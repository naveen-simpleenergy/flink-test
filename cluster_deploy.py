import os
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common.configuration import Configuration

def deploy_to_cluster():
    # Create the Configuration object
    config = Configuration()
    
    # Set cluster-specific configurations
    config.set_string("jobmanager.rpc.address", "localhost")  # Change to your job manager address
    config.set_integer("jobmanager.rpc.port", 6123)  # Default Flink JobManager RPC port
    config.set_integer("rest.port", 8081)  # Default Flink REST port
    
    # Create the StreamExecutionEnvironment with remote configuration
    env = StreamExecutionEnvironment.get_execution_environment(config)
    
    # Set runtime configurations
    env.set_parallelism(2)  # Adjust based on your cluster resources
    
    # Add Python files that need to be shipped to the cluster
    current_dir = os.path.dirname(os.path.abspath(__file__))
    env.add_python_file(os.path.join(current_dir, 'src/fraud_detector.py'))
    env.add_python_file(os.path.join(current_dir, 'src/models.py'))
    env.add_python_file(os.path.join(current_dir, 'src/main.py'))
    
    # Import and create job
    from src.main import create_fraud_detection_job
    job_env = create_fraud_detection_job()
    
    # Execute job
    job_env.execute("Fraud Detection Cluster Job")

if __name__ == "__main__":
    deploy_to_cluster()