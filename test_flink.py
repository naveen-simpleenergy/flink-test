from pyflink.datastream import StreamExecutionEnvironment

def test_flink_installation():
    env = StreamExecutionEnvironment.get_execution_environment()
    
    # Create a simple source
    ds = env.from_collection(
        collection=[(1, 'hello'), (2, 'world')],
    )
    
    # Add a simple transformation
    ds.map(lambda x: (x[0], x[1].upper())).print()
    
    # Execute
    env.execute('test_job')

if __name__ == '__main__':
    test_flink_installation()
