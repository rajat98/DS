import multiprocessing
import os
import sys

import grpc
import distributed_banking_system_pb2
import distributed_banking_system_pb2_grpc
import json
import time

OUTPUT_FILE_PATH = "./Output/output.json"


def dict_to_str(response):
    return str(protobuf_to_dict(response)) + "\n"


def protobuf_to_dict(message):
    result = {}
    for field, value in message.ListFields():
        if field.type == field.TYPE_MESSAGE:
            if field.label == field.LABEL_REPEATED:
                result[field.name] = [protobuf_to_dict(item) for item in value]
            else:
                result[field.name] = protobuf_to_dict(value)
        else:
            result[field.name] = value
    return result


class Customer:
    def __init__(self, id, customer_requests):
        # unique ID of the Customer
        self.id = id
        # events from the input
        self.customer_requests = customer_requests
        # a list of received messages used for debugging purpose
        self.recvMsg = list()
        # pointer for the stub
        self.stub = self.createStub()
        # logical clock
        self.logical_clock = 0


    def createStub(self):
        port = str(50050 + int(id))

        address = f"localhost:{port}"
        channel = grpc.insecure_channel(address)
        return distributed_banking_system_pb2_grpc.BankingServiceStub(channel)


    def executeEvents(self):
        banking_service_stub = self.stub
        customer_requests = self.customer_requests
        id = self.id
        event_response = []
        event_sent_ack = []
        for customer_request in customer_requests:
            customer_event, response = self.executeEvent(banking_service_stub, customer_request, id)
            event_response.append(response)
            event_sent_ack.append(customer_event)


        return event_response, event_sent_ack


    def increment_logical_clock(self):
        self.logical_clock += 1

    def executeEvent(self, banking_service_stub, customer_request, id):
        self.increment_logical_clock()
        customer_event = self.append_customer_event_to_recvMsg(customer_request, id)
        customer_request["logical_clock"] = self.logical_clock
        response = banking_service_stub.MsgDelivery(
            distributed_banking_system_pb2.BankingOperationRequest(id=id, type="customer",
                                                                   customer_requests=[customer_request]))
        return customer_event, response


    def update_recvMsg(self, branch_response):
        branch_dict_response = protobuf_to_dict(branch_response)
        self.recvMsg.append(branch_dict_response)
        print(branch_dict_response)

    def append_customer_event_to_recvMsg(self, customer_request, customer_id):
        customer_event = {"id": customer_id,
                          "customer_request_id": customer_request["customer_request_id"],
                          "type": "customer",
                          "logical_clock": self.logical_clock,
                          "interface": customer_request["interface"],
                          "comment": f"event_sent from customer {customer_id}"}
        self.recvMsg.append(customer_event)
        print(customer_event)
        return customer_event

def transform_branch_response_to_json(customer_response):
    result = []
    for response in customer_response:
        result.append(protobuf_to_dict(response)["event_result"])
    return result

def start_customer_process(id, events, result_queue):
    customer = Customer(id, events)
    branch_response, customer_response = customer.executeEvents()
    json_response = transform_branch_response_to_json(branch_response)
    json_response.extend(customer_response)
    print(branch_response)

    result_queue.put(json_response)

def generate_customer_output(results):
    filtered_results = [d for d in results if d['type'] == "customer"]
    for customer_event in filtered_results:


    customer_output = []
    for id, events in map.items():
        events = sorted(events, key=lambda d: d['customer_request_id'])
        customer_output.append({"id": id, "type": events[0]["type"], "events": events})

    return  customer_output

def generate_branch_output(results):
    filtered_results = [d for d in results if d['type'] == "branch"]
    map = dict(list)
    for entry in filtered_results:
        map[entry["id"]].append(entry)

    branch_output = []
    for id, events in map.items():
        events = sorted(events, key=lambda d: d['logical_clock'])
        branch_output.append({"id": id, "type": events[0]["type"], "events": events})

    return  branch_output
def generate_event_output(results):
    return []
def generate_output(results):
    directory = os.path.dirname(OUTPUT_FILE_PATH)
    if not os.path.exists(directory):
        os.makedirs(directory)
    output_file = open(OUTPUT_FILE_PATH, "a")
    output_file.write(f"// Part 1: List all the events taken place on each customer")
    customer_output = generate_customer_output(results)
    output_file.write(str(customer_output))

    output_file.write(f"// Part 2: List all the events taken place on each branch")
    branch_output = generate_branch_output(results)
    output_file.write(str(branch_output))

    output_file.write(f"// Part 3: List all the events (along with their logical times) triggered by each customer Deposit/Withdraw request")
    event_output = generate_event_output(results)
    output_file.write(str(event_output))

    output_file.close()

if __name__ == '__main__':
    res = []
    if len(sys.argv) != 2:
        input_file_path = "./Input/input.json"
    else:
        input_file_path = sys.argv[1]

    try:
        # Open and read the JSON file
        input_file = open(input_file_path, 'r')
        # Parse JSON data into a Python list of dictionaries
        processes = []
        result_queue = multiprocessing.Queue()
        parsed_data = json.load(input_file)
        customers = []
        for item in parsed_data:
            if item["type"] != "customer":
                continue

            id = item["id"]
            type = item["type"]
            customer_requests = item["customer-requests"]
            # print("ID:", item["id"])
            # print("Type:", item["type"])
            # print("Events:", item["events"])
            process = multiprocessing.Process(target=start_customer_process,
                                              args=(id, customer_requests, result_queue))
            processes.append(process)
            process.start()

            # sleep for 3 seconds for event propagation
            time.sleep(3)


        for process in processes:
            process.join()

            # Retrieve results from the queue
        results = []
        while not result_queue.empty():
            result = result_queue.get()
            results.append(result)
        generate_output(results)

        print("Customer process completed\n")
    except FileNotFoundError:
        print(f"File not found: {input_file_path}")
    except json.JSONDecodeError as e:
        print(f"Error parsing JSON: {str(e)}")
    except Exception as e:
        print(f"An error occurred: {str(e)}")
