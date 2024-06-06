import socket, threading, pickle
from flask import Flask, request
import time, sys
from config import Config

with open('systemconfig.cfg', 'r') as f:
    cfg = Config(f)
buffer = cfg.get('buffer')


# Load Balancer
class LoadBalancer:
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.server_socket = None
        self.leader_host = None
        self.leader_port = None
        self.leader_id = None
        try:
            self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.server_socket.bind((self.host, self.port))
            self.server_socket.listen(1)
            print(f"Load balancer listening on {self.host}:{self.port}")
        except socket.error:
            print("Socket not opened")

    def start_listening(self):
        while True:
            try:
                # print("Waiting for leader connection...")
                # client_socket, addr = self.server_socket.accept()
                connection, address = self.server_socket.accept()
                print("Connection accepted from address ", address)
                connection.settimeout(120)
                threading.Thread(target=self.connectionThread, args=(connection, address)).start()
                # self.leader_host, self.leader_port = leader_info.split(":")
                # print(f"New leader: {self.leader_host}:{self.leader_port}")
                # client_socket.close()
            except socket.error:
                print(socket.error)
                print("Error: Connection not accepted. Try again.")


    def connectionThread(self, connection, address):
        print("Connection thread called ", connection, address)
        '''
        received_file = b''
        while True:
            data = second_client_socket.recv(4096)
            if not data:
                break
            received_file += data
        '''
        rDataList = pickle.loads(connection.recv(buffer))
        connectionType = rDataList[0]
        print("Main function rDataList ", rDataList)
        if connectionType == 0:  # for leader announcement
            print("\nConnection type 1")
            print("\nConnection with:", address[0], ":", address[1])
            print(f"Leader Elected: {rDataList[1]} : {rDataList[2]}")
            self.leader_id = rDataList[1]
            self.leader_host = rDataList[2][0]
            self.leader_port = rDataList[2][1]
            print("Leader host : " + str(self.leader_host) + " Leader port : " + str(self.leader_port))
        elif connectionType == 3:  # check existing leader
            print("\nConnection type 3")
            if not self.leader_id:
                print("Leader not elected yet")
                data_list = [-1]
            else:
                print("Leader already exists")
                data_list = [1, (self.leader_host, self.leader_port), self.leader_id]
            connection.sendall(pickle.dumps(data_list))
            connection.close()
        elif connectionType == 1:  # for file upload
            upload_file_name = rDataList[2]
            received_file = b''
            count = 0
            while True:
                data = connection.recv(buffer)
                print("Data at load balancer ", count, data)
                if not data:
                    break
                received_file += data
                count += 1
                print("received_file", received_file)
            print("upload_file_name", upload_file_name)
            # load balancer connects to the leader node to get the target node
            leader_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            leader_datalist = [8, upload_file_name]
            leader_socket.connect((self.leader_host, self.leader_port))
            leader_socket.sendall(pickle.dumps(leader_datalist))

            # get response from leader node
            leader_response_list = pickle.loads(leader_socket.recv(buffer))
            leader_socket.close()
            target_address = leader_response_list[1]

            # load balancer connects to the target node to upload the file
            target_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            target_datalist = [1, 1, upload_file_name]
            target_socket.connect(target_address)
            target_socket.sendall(pickle.dumps(target_datalist))
            print("received_file size ", len(received_file))
            time.sleep(0.01)
            target_socket.sendall(received_file)
            '''
            total_sent = 0
            while total_sent < len(received_file):
                sent = target_socket.send(received_file[total_sent:total_sent + buffer])
                if sent == 0:
                    raise RuntimeError("Target Socket connection broken")
                total_sent += sent
            print("total_sent size ", total_sent)
            '''
            print("File uploaded to target node from lb")
            target_socket.close()
        elif connectionType == 2:  # for file download
            download_file_name = rDataList[2]
            # load balancer connects to the leader node to get the target node
            leader_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            leader_datalist = [8, download_file_name]
            leader_socket.connect((self.leader_host, self.leader_port))
            leader_socket.sendall(pickle.dumps(leader_datalist))
            # get response from leader node
            leader_response_list = pickle.loads(leader_socket.recv(buffer))
            leader_socket.close()
            target_address = leader_response_list[1]
            print("target_address ", target_address)
            # load balancer connects to the target node to download the file
            target_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            target_datalist = [1, 0, download_file_name]
            target_socket.connect(target_address)
            target_socket.sendall(pickle.dumps(target_datalist))
            # Receiving confirmation if file found or not
            confirmation = target_socket.recv(buffer)
            connection.sendall(confirmation)
            print("File exists or not confirmation ", confirmation)
            if confirmation == b"NotFound":
                print("File not found:", download_file_name)
            else:
                print("Receiving file:", download_file_name)
                received_file = b''
                count = 0

                while True:
                    count += 1
                    time.sleep(0.01)
                    data = target_socket.recv(buffer)
                    if not data:
                        break
                    # print("data from target ", count, data)
                    connection.sendall(data)
                    received_file += data
                    # print("data for client ", data)

                    # received_file += data
                print("file len of received_file at load balancer ", len(received_file))

                target_socket.close()
                connection.close()

    def start(self):
        # Accepting connections
        threading.Thread(target=self.start_listening(), args=()).start()

with open('systemconfig.cfg', 'r') as f:
    cfg = Config(f)

if __name__ == "__main__":
    lb_port = cfg.get('lb_port')
    lb_ip = "127.0.0.1"
    if len(sys.argv) < 2:
        print("IP not specified as arguments. Using defaults from config file.")
    else:
        lb_ip = sys.argv[1]
    lb = LoadBalancer(lb_ip, int(lb_port))
    lb.start()
    lb.ServerSocket.close()
