import socket, random
import threading
import pickle
import sys
import time
import hashlib
import os, traceback
from collections import OrderedDict
import config


def getHash(key):
    result = hashlib.sha1(key.encode())
    return int(result.hexdigest(), 16) % config.MAX_NODES

class Node:
    def __init__(self, ip, port):
        self.filenameList = []
        self.ip = ip
        self.port = port
        self.address = (ip, port)
        self.id = getHash(ip + ":" + str(port))
        self.predecessor = (ip, port)
        self.predecessorID = self.id
        self.successor = (ip, port)
        self.successorID = self.id
        self.fingerTable = OrderedDict()
        self.leader = self.address
        self.leaderID = self.id
        self.lb = [(config.LB1_IP, config.LB1_PORT)]
        # , (IP, config.LB2_PORT)]
        self.message_handler = {
            0: self.joinNetwork,
            1: self.fileRequest,
            2: self.pingRequest,
            3: self.lookup_ID,
            4: self.neighborUpdate,
            5: self.stabilize,
            6: self.forwardLeaderRequest,
            7: self.forwardLeaderAnnouncement,
            8: self.findNode,
            9: self.uploadToNode,
            10: self.downloadFromNode,
            11: lambda conn, addr, req: self.initiateLeaderElection()}

        try:
            self.ServerSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.ServerSocket.bind((IP, PORT))
            self.ServerSocket.listen()
        except socket.error:
            print("Could not open Socket.")
            sys.exit(1)

    '''..................................................FINGER TABLE HANDLING................................................'''

    #TODO check difference with send join request
    # Deals with join network request by other node
    def join_node(self, connection, address, request):
        if request:
            peerIPport = request[1]
            peerID = getHash(peerIPport[0] + ":" + str(peerIPport[1]))
            oldPred = self.predecessor
            oldPredID = self.predecessorID
            # Updating predecessor
            self.predecessor = peerIPport
            self.predecessorID = peerID
            # Sending new peer's predecessor back to it
            data = [oldPred, oldPredID]
            connection.sendall(pickle.dumps(data))
            # Updating finger table
            time.sleep(0.1)
            self.update_finger_table()
            # Then asking other peers to update their f table as well
            self.update_peer_finger_table()
            self.print_node_options()

    #TODO check the requirement
    def lookup_ID(self, connection, address, request):
        keyID = request[1]
        data = []
        if self.id == keyID:  # Case 0: If keyId is current node
            data = [0, self.address]
        elif self.successorID == self.id:  # Case 1: If only one node
            data = [0, self.address]
        elif self.id > keyID:  # Case 2: Node id greater than keyId, ask pred
            if self.predecessorID < keyID:  # If pred is higher than key, then self is the node
                data = [0, self.address]
            elif self.predecessorID > self.id:
                data = [0, self.address]
            else:  # Else send the pred back
                data = [1, self.predecessor]
        else:  # Case 3: node id less than keyId USE fingertable to search
            # IF last node before chord circle completes
            if self.id > self.successorID:
                data = [0, self.successor]
            else:
                value = ()
                for key, value in self.fingerTable.items():
                    if key >= keyID:
                        break
                value = self.successor
                data = [1, value]
        connection.sendall(pickle.dumps(data))

    def update_successor(self, request):
        newSucc = request[2]
        self.successor = newSucc
        self.successorID = getHash(newSucc[0] + ":" + str(newSucc[1]))

    def update_predecessor(self, request):
        newPred = request[2]
        self.predecessor = newPred
        self.predecessorID = getHash(newPred[0] + ":" + str(newPred[1]))

    # sendJoinRequest
    def network_join_request(self, ip, port):
        try:
            print("Distributed systems network join requested initiated...")
            # Finding target location for the given id
            succ_addr = self.find_successor((ip, port), self.id)
            peer_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            peer_socket.connect(succ_addr)
            data = [0, self.address]

            peer_socket.sendall(pickle.dumps(data))  # Sending self peer address to add to network
            node_info = pickle.loads(peer_socket.recv(config.BUFFER))  # Receiving new pred

            # Updating pred and succ
            self.predecessor = node_info[0]
            self.predecessorID = node_info[1]
            self.successor = succ_addr
            self.successorID = getHash(succ_addr[0] + ":" + str(succ_addr[1]))

            # Tell pred to update its successor which is now me
            data = [4, 1, self.address]
            p_socket2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            p_socket2.connect(self.predecessor)
            p_socket2.sendall(pickle.dumps(data))
            p_socket2.close()
            peer_socket.close()

            # Initiating Leader election after new node joins
            self.initiateLeaderElection()

        except socket.error:
            print("Socket error. Provide valid IP/Port.")

    def leave_network(self):

        # update the predecessor for current node's successor
        s_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s_socket.connect(self.successor)
        s_socket.sendall(pickle.dumps([4, 0, self.predecessor]))
        s_socket.close()
        # update the successor for current node's predecessor
        p_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        p_socket.connect(self.predecessor)
        p_socket.sendall(pickle.dumps([4, 1, self.successor]))
        p_socket.close()

        # Replicating files to successor as a client
        print("Replicating the files to neighboring nodes before leaving")
        print("File list for replication:", self.filenameList)
        for filename in self.filenameList:
            n_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            n_socket.connect(self.successor)
            data = [1, 1, filename]
            n_socket.sendall(pickle.dumps(data))
            self.retrieve_file(n_socket, filename)
            n_socket.close()
            print("File replicated")
            n_socket.close()
        # finger table update request sent to other nodes
        self.update_peer_finger_table()
        # if leader leaves then elect new leader
        if self.address == self.leader:
            self.updateLeader()

        # updating the successor and predeccor
        self.predecessor = (self.ip, self.port)
        self.predecessorID = self.id
        self.successor = (self.ip, self.port)
        self.successorID = self.id
        self.fingerTable.clear()
        print(f"Leaving the network... \nNode: {self.id}\n Address: {self.address}")

    # getSuccessor
    def find_successor(self, address, keyID):
        request = [1, address]
        node_ip_port = request[1]
        # Sending continous lookup requests till required peer ID found
        while request[0] == 1:
            peer_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                peer_socket.connect(node_ip_port)
                data = [3, keyID]
                peer_socket.sendall(pickle.dumps(data))
                request = pickle.loads(peer_socket.recv(config.BUFFER))
                node_ip_port = request[1]
                peer_socket.close()
            except socket.error:
                print("Attempt to find successor failed...")
                print(socket.error)

        return node_ip_port

    def update_finger_table(self):
        for i in range(config.MAX_BITS):
            entryId = (self.id + (2 ** i)) % config.MAX_NODES
            # If only one node in network
            if self.successor == self.address:
                self.fingerTable[entryId] = (self.id, self.address)
                continue

            # Find succ for each entryID and update table
            succ_addr = self.find_successor(self.successor, entryId)
            recvId = getHash(succ_addr[0] + ":" + str(succ_addr[1]))
            self.fingerTable[entryId] = (recvId, succ_addr)

    def update_peer_finger_table(self):
        my_successor = self.successor
        while True:
            if my_successor == self.address:
                break
            p_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                p_socket.connect(my_successor)
                p_socket.sendall(pickle.dumps([5]))
                my_successor = pickle.loads(p_socket.recv(config.BUFFER))
                p_socket.close()
                if my_successor == self.successor:
                    break
            except socket.error:
                print("Error in socket connection...")
                print("Connection denied...")

    '''......................................................FILE HANDLING...................................................'''
    # transferfile
    def file_request_handler(self, connection, address, rDataList):
        # Choice: 0 = download, 1 = upload
        action = rDataList[1]
        filename = rDataList[2]
        # IF client wants to download file
        if action == 0:
            print("Download request for file:", filename)
            try:
                # If file not found in filename list, file does not exist
                if filename not in self.filenameList:
                    connection.send("NotFound".encode('utf-8'))
                    print("File not found")
                else:  # If file exists in its directory   # Sending DATA LIST Structure (sDataList):
                    connection.send("Found".encode('utf-8'))
                    self.retrieve_file(connection, filename)
            except ConnectionResetError as error:
                print("Error in socket connection...")
                print(error, "\nClient disconnected\n\n")
        # if client wants to upload files to network
        elif action == 1 or action == -1:
            print("Saving file:", filename)
            fileID = getHash(filename)
            print("The generated File ID: ", fileID)
            self.filenameList.append(filename)
            self.save_file(connection, filename)
            print("Upload successful!")
            # Replicating file to successor node
            if action == 1:
                if self.address != self.successor:
                    self.replicate_uploaded_file(filename, self.successor, False)

    def download_file(self, filename):
        print("Download file initated...", filename)
        file_hash_key = getHash(filename)
        # finding node with the file
        recvIPport = self.find_successor(self.successor, file_hash_key)
        download_dataList = [1, 0, filename]
        download_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        download_socket.connect(recvIPport)
        download_socket.sendall(pickle.dumps(download_dataList))
        # Receiving confirmation if file found or not
        fileData = download_socket.recv(config.BUFFER)
        if fileData == b"NotFound":
            print("File not found:", filename)
        else:
            print("Receiving file:", filename)
            self.save_file(download_socket, filename)

    def replicate_uploaded_file(self, filename, recvIPport, replicate):
        print("File upload initiated : ", filename)
        # lookup request to get peer to upload file
        upload_datalist = [1]
        if replicate:
            upload_datalist.append(1)
        else:
            upload_datalist.append(-1)
        try:
            # Check if the file exists
            file = open(filename, 'rb')
            file.close()
            upload_datalist = upload_datalist + [filename]
            upload_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            upload_socket.connect(recvIPport)
            upload_socket.sendall(pickle.dumps(upload_datalist))
            self.retrieve_file(upload_socket, filename)
            upload_socket.close()
        except IOError:
            print("File not in directory")
        except socket.error:
            print("Error in uploading file")

    def retrieve_file(self, connection, filename):
        try:
            print("File retrieve in progress...")
            file_data = b''
            with open(filename, 'rb+') as download_file:
                download_file.seek(0)
                file_data = download_file.read()
            connection.sendall(file_data)
            connection.close()
        except Exception as e:
            print(f"An exception occurred: {type(e).__name__}")
            print(f"Reason: {str(e)}")
            print("Traceback:")
            traceback.print_exc()

    def save_file(self, connection, filename):
        file_exists = False
        try:
            with open(filename, 'rb+') as file1:
                data = file1.read()
                size = len(data)
                if size == 0:
                    print("Retransmission initiated...")
                    file_exists = False
                else:
                    print("File already present")
                    file_exists = True
                return
        except FileNotFoundError:
            pass
        if not file_exists:
            try:
                total_data = b''
                with open(filename, 'wb+') as new_file:
                    count = 0
                    while True:
                        file_data = connection.recv(config.BUFFER)
                        if not file_data:
                            break
                        total_data += file_data
                        count += 1
                    new_file.write(total_data)
            except ConnectionResetError:
                print("Data transfer interrupted\nWaiting for system to stabilize...")
                print("Trying again after 10 seconds")
                os.remove(filename)
                time.sleep(5)
                self.downloadFile(filename)

    '''...................................................LEADER ELECTION.....................................................'''

    def updateLeader(self):
        print("Initiating Leader Election for other nodes in the network")
        try:
            lSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            lSocket.connect(self.successor)
            lSocket.sendall(pickle.dumps([11]))
            lSocket.close()
        except socket.error:
            print("Error when initiating leader election for other nodes")

    def initiateLeaderElection(self):
        print("Initiating Leader Election")
        try:
            lSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            lSocket.connect(self.successor)
            lSocket.sendall(pickle.dumps([6, self.id]))
            lSocket.close()
        except socket.error:
            print("Error when initiating leader election")

    def forwardLeaderRequest(self, connection, address, request):
        print("Forwarding Leader Election Request...")
        if self.id == request[1]:
            print("I am elected as the leader")
            self.initiateLeaderAnnouncement()
        elif self.id > request[1]:
            request[1] = self.id
            self.sendLeaderRequest(request)
        else:
            self.sendLeaderRequest(request)

    def sendLeaderRequest(self, request):
        try:
            lSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            lSocket.connect(self.successor)
            lSocket.sendall(pickle.dumps(request))
            lSocket.close()
        except socket.error:
            print("Error when initiating leader election")

    def initiateLeaderAnnouncement(self):
        self.leaderID = self.id
        self.leader = self.address
        print("Initiating Leader announcement")
        try:
            lSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            lSocket.connect(self.successor)
            lSocket.sendall(pickle.dumps([7, self.leaderID, self.leader]))
            lSocket.close()
            for addr in self.lb:
                lSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                lSocket.connect(addr)
                print(f"Sending leader information to: {addr}")
                lSocket.sendall(pickle.dumps([0, self.leaderID, self.leader]))
                lSocket.close()
        except socket.error:
            print("Error when initiating leader election")

    def forwardLeaderAnnouncement(self, connection, address, request):
        if self.id == request[1]:
            print("Leader Election complete")
            return

        self.leaderID = request[1]
        self.leader = request[2]
        print(f"New Leader set to {self.leaderID}")
        try:
            lSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            lSocket.connect(self.successor)
            lSocket.sendall(pickle.dumps(request))
            lSocket.close()
        except socket.error:
            print("Error when initiating leader election")

    '''..................................................PRINT UTILITIES......................................................'''

    def print_node_options(self):
        print("\n1. Join Network\n2. Leave Network")
        print("3. Print Finger Table\n4. Print my predecessor and successor\n5. Print Leader")

    def print_finger_table(self):
        print("Printing Finger Table")
        for key, value in self.fingerTable.items():
            print("KeyID:", key, "Value", value)

    '''............................................BASE FUNCTIONALITIES HANDLING............................................'''

    def joinNetwork(self, connection, address, request):
        print("Connection with:", address[0], ":", address[1])
        print("Join network request recevied")
        self.joinNode(connection, address, request)
        return

    def fileRequest(self, connection, address, request):
        print("Connection with:", address[0], ":", address[1])
        print("Upload/Download request recevied")
        self.file_request_handler(connection, address, request)
        return

    def pingRequest(self, connection, address, request):
        connection.sendall(pickle.dumps(self.predecessor))

    def neighborUpdate(self, connection, address, request):
        if request[1] == 1:
            self.update_successor(request)
        else:
            self.update_predecessor(request)

    def stabilize(self, connection, address, request):
        self.update_finger_table()
        connection.sendall(pickle.dumps(self.successor))

    def findNode(self, connection, address, request):
        fileID = getHash(request[1])
        target_node_ip_port = self.find_successor(self.successor, fileID)
        connection.sendall(pickle.dumps([1, target_node_ip_port]))

    def uploadToNode(self, connection, address, request):
        filename = request[1]
        file_content = request[2]
        self.acceptFile(filename, file_content, True)

    def downloadFromNode(self, connection, address, request):
        filename = request[1]
        self.downloadFile(filename)

    def connection_thread(self, connection, address):
        request = pickle.loads(connection.recv(config.BUFFER))
        message_type = request[0]
        try:
            self.message_handler[message_type](connection, address, request)
            connection.close()
        except KeyError:
            print("Received an invalid message type")
            connection.close()

    def start_listening(self):
        while True:
            try:
                connection, address = self.ServerSocket.accept()
                connection.settimeout(120)
                threading.Thread(target=self.connection_thread, args=(connection, address)).start()
            except socket.error:
                pass

    def ping_successor(self):
        while True:
            # Pingin successor every 3 seconds
            time.sleep(3)
            # If only one node, no need to ping
            if self.address == self.successor:
                continue
            try:
                p_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                p_socket.connect(self.successor)
                p_socket.sendall(pickle.dumps([2]))  # Send ping request
                # recvPred = pickle.loads(p_socket.recv(config.BUFFER))
            except:
                # When not receiving ping ack from successor. Searching for the next successor from Finger table and stabilizing
                print("\nSuccesor node is offline!\nSelf Stabilization in progress...")
                new_successor_found = False
                value = ()
                failed_successor = self.successor
                for key, value in self.fingerTable.items():
                    if value[0] != self.successorID:
                        new_successor_found = True
                        break
                if new_successor_found:
                    # Updating successor and informing the successor of its predecessor
                    self.successor = value[1]
                    self.successorID = getHash(self.successor[0] + ":" + str(self.successor[1]))
                    p_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    p_socket.connect(self.successor)
                    p_socket.sendall(pickle.dumps([4, 0, self.address]))
                    p_socket.close()
                else:
                    # If none found in the fingertable updating both predecessor and successor as itself.
                    self.predecessor = self.address
                    self.predecessorID = self.id
                    self.successor = self.address
                    self.successorID = self.id
                self.update_finger_table()
                self.update_peer_finger_table()

                print(f"Failed successor: {failed_successor}")
                if failed_successor == self.leader:
                    print("Failed successor was a leader")
                    self.initiateLeaderElection()
                self.print_node_options()

    def configThread(self):
        self.print_node_options()
        action = input()
        match action:
            case '1':
                ip = input("Enter IP of node: ")
                port = input("Enter port: ")
                self.network_join_request(ip, int(port))
                return
            case '2':
                self.leave_network()
                return
            case '3':
                self.print_finger_table()
                return
            case '4':
                print("My ID:", self.id, "Predecessor:", self.predecessorID, "Successor:", self.successorID)
                return
            case '5':
                print("Leader: ", self.leaderID)
                return

    def spin_up(self):
        # start listening to requests
        threading.Thread(target=self.start_listening, args=()).start()
        #
        threading.Thread(target=self.ping_successor, args=()).start()
        while True:
            self.configThread()


IP = config.IP
PORT = config.PORT

if len(sys.argv) < 3:
    print("IP and PORT not specified as arguments. Using defaults from config file.")
else:
    IP = sys.argv[1]
    PORT = int(sys.argv[2])

chord_node = Node(IP, PORT)
print("Current chord node ID: ", chord_node.id)
chord_node.spin_up()
chord_node.ServerSocket.close()
