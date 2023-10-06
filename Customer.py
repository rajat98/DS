import multiprocessing
import sys

import grpc
import distributed_banking_system_pb2
import distributed_banking_system_pb2_grpc
import json
import time


class Customer:
    def __init__(self, id, events):
        # unique ID of the Customer
        self.id = id
        # events from the input
        self.events = events
        # a list of received messages used for debugging purpose
        self.recvMsg = list()
        # pointer for the stub
        self.stub = self.create_stub()
        # TODO: students are expected to create the Customer stub

    def create_stub(self):
        port = str(50050 + int(id))

        address = f"localhost:{port}"
        channel = grpc.insecure_channel(address)
        return distributed_banking_system_pb2_grpc.BankingServiceStub(channel)

    # TODO: students are expected to send out the events to the Bank

    def execute_events(self):
        banking_service_stub = self.stub
        events = self.events
        id = self.id
        response = banking_service_stub.MsgDelivery(
            distributed_banking_system_pb2.BankingOperationRequest(id=id, type="customer", events=events))
        return response


if __name__ == '__main__':
    res = []
    if len(sys.argv) != 2:
        input_file_path = "./Input/input.json"
    else:
        input_file_path = sys.argv[1]

    try:
        # Open and read the JSON file
        with open(input_file_path, 'r') as json_file:
            # Parse JSON data into a Python list of dictionaries
            # processes = []
            # result_queue = multiprocessing.Queue()
            parsed_data = json.load(json_file)
            for item in parsed_data:
                if item["type"] != "customer":
                    continue
                # branch_id_list.append(item["id"])
                id = item["id"]
                type = item["type"]
                events = item["events"]
                print("ID:", item["id"])
                print("Type:", item["type"])
                print("Events:", item["events"])
                print("Will try to greet world ...")
                customer = Customer(id, events)
                res.append(customer.execute_events())
                time.sleep(2)
                print(res)
                # Wait for all server processes to finish
            #     for process in processes:
            #         process.join()
            # server = result_queue.get()
    except FileNotFoundError:
        print(f"File not found: {input_file_path}")
    except json.JSONDecodeError as e:
        print(f"Error parsing JSON: {str(e)}")
    except Exception as e:
        print(f"An error occurred: {str(e)}")
