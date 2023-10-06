import json
import multiprocessing
import sys
import time
from concurrent import futures
import logging
import datetime

import os
import grpc
import distributed_banking_system_pb2
import distributed_banking_system_pb2_grpc

_ONE_DAY = datetime.timedelta(days=1)


class Branch(distributed_banking_system_pb2_grpc.BankingServiceServicer):

    def __init__(self, id, balance, branches):
        # unique ID of the Branch
        self.id = id
        # replica of the Branch's balance
        self.balance = balance
        # the list of process IDs of the branches
        self.branches = branches
        # the list of Client stubs to communicate with the branches
        self.stubList = list()
        # a list of received messages used for debugging purpose
        self.recvMsg = list()
        # iterate the processID of the branches
        self.branch_id_list = list()
        # TODO: students are expected to store the processID of the branches

    def update_branches(self, branches):
        self.balance = branches
        self.initialize_stubs()

    def initialize_stubs(self):
        # Initialize gRPC stubs for communication with other branches
        for branch_id in self.branches:
            if branch_id != self.id:
                port = str(50050 + int(branch_id))
                address = f"localhost:{port}"
                channel = grpc.insecure_channel(address)  # Replace with actual addresses
                stub = distributed_banking_system_pb2_grpc.BankingServiceStub(channel)
                self.stubList.append(stub)

    # TODO: students are expected to process requests from both Client and Branch
    def MsgDelivery(self, request, context):
        type = request.type
        response = []
        match type:
            case "customer":
                response = self.process_customer_events(request)
            case "branch":
                response = self.process_branch_events(request)

        # self.recvMsg.append(response)
        return distributed_banking_system_pb2.BankingOperationResponse(id=self.id, recv=response)

    def process_customer_events(self, request):
        response = list()
        for event in request.events:
            match event.interface:
                case "query":
                    response.append(self.query(event))
                case "deposit":
                    response.append(self.deposit(event))
                    # self.process_branch_events(event)
                case "withdraw":
                    response.append(self.withdraw(event))
        # self.process_branch_events(request)

        return response

    def query(self, request):
        return {'interface': 'query', 'result': None, 'balance': self.balance}

    def deposit(self, event):
        self.balance += event.money
        self.replicate_deposit(event)
        return {'interface': 'deposit', 'result': 'success'}

    def withdraw(self, event):
        result = "failed"
        if self.balance >= event.money:
            self.balance -= event.money
            self.replicate_withdraw(event)
            result = "success"
        return {'interface': 'withdraw', 'result': result}

    def process_branch_events(self, request):
        response = list()
        for event in request.events:
            match event.interface:
                case "withdraw":
                    response.append(self.propagate_withdraw(event))
                case "deposit":
                    response.append(self.propagate_deposit(event))
        return response

    def propagate_deposit(self, event):
        self.balance += event.money
        return {'interface': 'propagate_deposit', 'result': 'success'}

    def propagate_withdraw(self, request):
        self.balance -= request.money
        return {'interface': 'propagate_withdraw', 'result': 'success'}

    def replicate_deposit(self, event):
        for stub in self.stubList:
            response = stub.MsgDelivery(
                distributed_banking_system_pb2.BankingOperationRequest(id=self.id, type="branch", events=[event]))
            if response.recv[0].result != "success":
                print(f"Failed to replicate deposit to branch {self.stubList.index(stub) + 1}")

    def replicate_withdraw(self, event):
        for stub in self.stubList:
            response = stub.MsgDelivery(
                distributed_banking_system_pb2.BankingOperationRequest(id=id, type="branch", events=[event]))
            if response.recv[0].result != "success":
                print(f"Failed to replicate withdrawal to branch {self.stubList.index(stub) + 1}")


def serve(port, id, balance, branch_id_list, result_queue, process_id_list):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    process_id_list.append(os.getpid())
    distributed_banking_system_pb2_grpc.add_BankingServiceServicer_to_server(Branch(id, balance, branch_id_list),
                                                                             server)
    server.add_insecure_port("[::]:" + port)
    server.start()
    print("Server started, listening on " + port)
    _wait_forever(server)


def _wait_forever(server):
    try:
        while True:
            time.sleep(_ONE_DAY.total_seconds())
    except KeyboardInterrupt:
        server.stop(None)


def initialize_branch_servers(branch_id_list):
    input_file_path = get_input_file_path()
    try:
        # Open and read the JSON file
        with open(input_file_path, 'r') as json_file:
            # Parse JSON data into a Python list of dictionaries
            parsed_data = json.load(json_file)

        processes = []
        result_queue = multiprocessing.Queue()
        # Now, `parsed_data` is a Python list containing dictionaries
        for item in parsed_data:
            if item["type"] != "branch":
                continue
            # branch_id_list.append(item["id"])
            id = item["id"]
            type = item["type"]
            balance = item["balance"]
            print("ID:", item["id"])
            print("Type:", item["type"])
            print("Balance:", item["balance"])

            process = multiprocessing.Process(target=serve,
                                              args=(str(50050 + int(id)), id, balance, branch_id_list, result_queue))
            processes.append(process)
            process.start()

        # Wait for all server processes to finish
        for process in processes:
            process.join()

        # server = result_queue.get()
        # server.wait_for_termination()
    except FileNotFoundError:
        print(f"File not found: {input_file_path}")
    except json.JSONDecodeError as e:
        print(f"Error parsing JSON: {str(e)}")
    except Exception as e:
        print(f"An error occurred: {str(e)}")


def get_input_file_path():
    if len(sys.argv) != 2:
        input_file_path = "./Input/input.json"
    else:
        input_file_path = sys.argv[1]
    return input_file_path


def populate_branch_id_list(branch_id_list):
    input_file_path = get_input_file_path()
    try:
        # Open and read the JSON file
        with open(input_file_path, 'r') as json_file:
            # Parse JSON data into a Python list of dictionaries
            parsed_data = json.load(json_file)
            for item in parsed_data:
                if item["type"] != "branch":
                    continue
                branch_id_list.append(item["id"])
    except FileNotFoundError:
        print(f"File not found: {input_file_path}")
    except json.JSONDecodeError as e:
        print(f"Error parsing JSON: {str(e)}")
    except Exception as e:
        print(f"An error occurred: {str(e)}")

if __name__ == "__main__":
    logging.basicConfig()
    branch_id_list = []
    populate_branch_id_list(branch_id_list)
    initialize_branch_servers(branch_id_list)




