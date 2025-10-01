from prefect import flow, task

@task
def hello_task():
    print("Hello from the task!")
    return "Task completed"

@flow(name="basic-test-flow")
def basic_test_flow():
    """A simple flow to test the work pool."""
    print("Starting basic test flow...")
    result = hello_task()
    print(f"Result: {result}")
    print("Flow completed successfully!")

if __name__ == "__main__":
    basic_test_flow()
