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
        self.pred = (ip, port)            
        self.predID = self.id
        self.succ = (ip, port)            
        self.succID = self.id
        self.fingerTable = OrderedDict()      
        self.leader = self.address
        self.leaderID = self.id
        self.lb = [(config.LB1_IP, config.LB1_PORT)]
        #, (IP, config.LB2_PORT)]
        self.message_handler = { 
            0: self.joinNetwork, 
            1: self.fileRequest, 
            2: self.pingRequest, 
            3: self.lookupID, 
            4: self.neighborUpdate, 
            5: self.stabilize,
            6: self.forwardLeaderRequest, 
            7: self.forwardLeaderAnnouncement, 
            8: self.findNode, 
            9: self.uploadToNode, 
            10: self.downloadFromNode,
            11: lambda conn, addr, req : self.initiateLeaderElection() }

        try:
            self.ServerSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.ServerSocket.bind((IP, PORT))
            self.ServerSocket.listen()
        except socket.error:
            print("Could not open Socket.")
            sys.exit(1)

    '''..................................................FINGER TABLE HANDLING................................................'''

    # Deals with join network request by other node
    def joinNode(self, connection, address, request):
        if request:
            peerIPport = request[1]
            peerID = getHash(peerIPport[0] + ":" + str(peerIPport[1]))
            oldPred = self.pred
            oldPredID = self.predID
            # Updating pred
            self.pred = peerIPport
            self.predID = peerID
            # Sending new peer's pred back to it
            data = [oldPred, oldPredID]
            connection.sendall(pickle.dumps(data))
            # Updating F table
            time.sleep(0.1)
            self.updateFingerTable()
            # Then asking other peers to update their f table as well
            self.updateOtherFingerTables()
            self.printOptions()


    def lookupID(self, connection, address, request):
        keyID = request[1]
        data = []
        # print(self.id, keyID)
        if self.id == keyID:  # Case 0: If keyId at self
            data = [0, self.address]
        elif self.succID == self.id:  # Case 1: If only one node
            data = [0, self.address]
        elif self.id > keyID:  # Case 2: Node id greater than keyId, ask pred
            if self.predID < keyID:  # If pred is higher than key, then self is the node
                data = [0, self.address]
            elif self.predID > self.id:
                data = [0, self.address]
            else:  # Else send the pred back
                data = [1, self.pred]
        else:  # Case 3: node id less than keyId USE fingertable to search
            # IF last node before chord circle completes
            if self.id > self.succID:
                data = [0, self.succ]
            else:
                value = ()
                for key, value in self.fingerTable.items():
                    if key >= keyID:
                        break
                value = self.succ
                data = [1, value]
        connection.sendall(pickle.dumps(data))

    def updateSucc(self, request):
        newSucc = request[2]
        self.succ = newSucc
        self.succID = getHash(newSucc[0] + ":" + str(newSucc[1]))

    def updatePred(self, request):
        newPred = request[2]
        self.pred = newPred
        self.predID = getHash(newPred[0] + ":" + str(newPred[1]))

    def sendJoinRequest(self, ip, port):
        try:
            #Finding target location for the given id
            succ_addr = self.getSuccessor((ip, port), self.id)
            peerSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            peerSocket.connect(succ_addr)
            data = [0, self.address]

            peerSocket.sendall(pickle.dumps(data))  # Sending self peer address to add to network
            node_info = pickle.loads(peerSocket.recv(config.BUFFER))  # Receiving new pred

            # Updating pred and succ
            self.pred = node_info[0]
            self.predID = node_info[1]
            self.succ = succ_addr
            self.succID = getHash(succ_addr[0] + ":" + str(succ_addr[1]))

            # Tell pred to update its successor which is now me
            data = [4, 1, self.address]
            pSocket2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            pSocket2.connect(self.pred)
            pSocket2.sendall(pickle.dumps(data))
            pSocket2.close()
            peerSocket.close()

            #Initiating Leader election after new node joins
            self.initiateLeaderElection()

        except socket.error:
            print("Socket error. Provide valid IP/Port.")

    def leaveNetwork(self):

        #Inform my succ to update its pred
        pSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        pSocket.connect(self.succ)
        pSocket.sendall(pickle.dumps([4, 0, self.pred]))
        pSocket.close()

        #Inform my pred to update its succ
        pSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        pSocket.connect(self.pred)
        pSocket.sendall(pickle.dumps([4, 1, self.succ]))
        pSocket.close()

        #Replicating its files to succ as a client
        print("Replicating below files to other nodes before leaving")
        print("File List:", self.filenameList)
        for filename in self.filenameList:
            pSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            pSocket.connect(self.succ)
            data = [1, 1, filename]
            pSocket.sendall(pickle.dumps(data))
            self.sendFile(pSocket, filename)
            pSocket.close()
            print("File replicated")
            pSocket.close()

        #Sending finger table update request to other nodes
        self.updateOtherFingerTables() 
        
        #Sending update of leader if I am the leader
        if self.address == self.leader:
            self.updateLeader()
        
        #Chaning succ and pred to myself
        self.pred = (self.ip, self.port)  
        self.predID = self.id
        self.succ = (self.ip, self.port)
        self.succID = self.id
        self.fingerTable.clear()
        print(f"I have left the network. Node: {self.id}\n Address: {self.address}")



    def getSuccessor(self, address, keyID):
        request = [1, address]
        node_addr = request[1]

        # Sending continous lookup requests till required peer ID found
        while request[0] == 1:
            peerSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                peerSocket.connect(node_addr)
                data = [3, keyID]
                peerSocket.sendall(pickle.dumps(data))
                request = pickle.loads(peerSocket.recv(config.BUFFER))
                node_addr = request[1]
                peerSocket.close()
            except socket.error:
                print(socket.error)
                print("Connection denied while getting Successor")
        return node_addr

    def updateFingerTable(self):
        for i in range(config.MAX_BITS):
            entryId = (self.id + (2 ** i)) % config.MAX_NODES
            # If only one node in network
            if self.succ == self.address:
                self.fingerTable[entryId] = (self.id, self.address)
                continue
            
            #Find succ for each entryID and update table
            succ_addr = self.getSuccessor(self.succ, entryId)
            recvId = getHash(succ_addr[0] + ":" + str(succ_addr[1]))
            self.fingerTable[entryId] = (recvId, succ_addr)

    def updateOtherFingerTables(self):
        tsucc = self.succ
        while True:
            if tsucc == self.address:
                break
            pSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                pSocket.connect(tsucc)
                pSocket.sendall(pickle.dumps([5]))
                tsucc = pickle.loads(pSocket.recv(config.BUFFER))
                pSocket.close()
                if tsucc == self.succ:
                    break
            except socket.error:
                print("Connection denied")
    '''......................................................FILE HANDLING...................................................'''

    def transferFile(self, connection, address, rDataList):
        # Choice: 0 = download, 1 = upload
        choice = rDataList[1]
        filename = rDataList[2]
        fileID = getHash(filename)
        # IF client wants to download file
        if choice == 0:
            print("Download request for file:", filename)
            try:
                # First it searches its own directory (fileIDList). If not found, send does not exist
                if filename not in self.filenameList:
                    connection.send("NotFound".encode('utf-8'))
                    print("File not found")
                else:  # If file exists in its directory   # Sending DATA LIST Structure (sDataList):
                    connection.send("Found".encode('utf-8'))
                    self.sendFile(connection, filename)
            except ConnectionResetError as error:
                print(error, "\nClient disconnected\n\n")
        # ELSE IF client wants to upload something to network
        elif choice == 1 or choice == -1:
            print("Receiving file:", filename)
            fileID = getHash(filename)
            print("Uploading file ID:", fileID)
            self.filenameList.append(filename)
            self.receiveFile(connection, filename)
            print("Upload complete")
            # Replicating file to successor as well
            if choice == 1:
                if self.address != self.succ:
                    self.uploadFile(filename, self.succ, False)
    
    def downloadFile(self, filename):
        print("Downloading file", filename)
        file_hash_key = getHash(filename)
        # First finding node with the file
        recvIPport = self.getSuccessor(self.succ, file_hash_key)
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
            self.receiveFile(download_socket, filename)
    
    def uploadFile(self, filename, recvIPport, replicate):
        print("File upload initiated : ", filename)
        # If not found send lookup request to get peer to upload file
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
            self.sendFile(upload_socket, filename)
            upload_socket.close()
            print("File uploaded")
        except IOError:
            print("File not in directory")
        except socket.error:
            print("Error in uploading file")
    
    def sendFile(self, connection, filename):
        print("Sending file: and self.filenameList ", filename, self.filenameList)
        print("Sending file:", filename)
        try:
            print("sending file with open type of filename ", type(filename))
            file_size = os.path.getsize(filename)
            print(f"The size of {filename} is {file_size} bytes.")
            filedata = b''
            print("filename ", filename)
            with open(filename, 'rb+') as download_file:
                download_file.seek(0)
                filedata = download_file.read()
            print("Sending file from target node fileData", filedata)
            file_size = os.path.getsize(filename)
            print(f"The size of {filename} is {file_size} bytes.")
            connection.sendall(filedata)
            connection.close()
        except Exception as e:
            print(f"An exception occurred: {type(e).__name__}")
            print(f"Reason: {str(e)}")
            print("Traceback:")
            traceback.print_exc()  # Prints full traceback
            

    def receiveFile(self, connection, filename):
        print("data received at target node receive func filename", filename)
        fileAlready = False
        try:
            with open(filename, 'rb+') as file1:
                data = file1.read()
                size = len(data)
                print("data received  ", data)
                print("data received at target node size", size)

                if size == 0:
                    print("Retransmission request sent")
                    fileAlready = False
                else:
                    print("File already present")
                    fileAlready = True
                return
        except FileNotFoundError:
            pass
        if not fileAlready:
            totalData = b''
            recvSize = 0
            print("file does not exist ")
            try:
                with open(filename, 'wb+') as new_file:
                    count = 0
                    while True:
                        print("file does not exist while true ", count)
                        fileData = connection.recv(config.BUFFER)
                        print("file data  ", fileData)
                        recvSize += len(fileData)
                        print("recvSize ", recvSize)
                        if not fileData:
                            break
                        totalData += fileData
                        count += 1
                    new_file.write(totalData)
                    print("new_file size ", new_file.tell())
            except ConnectionResetError:
                print("Data transfer interupted\nWaiting for system to stabilize")
                print("Trying again after 10 seconds")
                time.sleep(5)
                os.remove(filename)
                time.sleep(5)
                self.downloadFile(filename)
                # connection.send(pickle.dumps(True))


    '''...................................................LEADER ELECTION.....................................................'''

    def updateLeader(self):
        print("Initiating Leader Election for other nodes in the network")
        try:
            lSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            lSocket.connect(self.succ)
            lSocket.sendall(pickle.dumps([11]))
            lSocket.close()
        except socket.error:
            print("Error when initiating leader election for other nodes")

    def initiateLeaderElection(self):
        print("Initiating Leader Election")
        try:
            lSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            lSocket.connect(self.succ)
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
            lSocket.connect(self.succ)
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
            lSocket.connect(self.succ)
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
            lSocket.connect(self.succ)
            lSocket.sendall(pickle.dumps(request))
            lSocket.close()
        except socket.error:
            print("Error when initiating leader election")



    '''..................................................PRINT UTILITIES......................................................'''
    
    def printOptions(self):
        print("\n1. Join Network\n2. Leave Network")
        print("3. Print Finger Table\n4. Print my predecessor and successor\n5. Print Leader")

    def printFingerTable(self):
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
        self.transferFile(connection, address, request)
        return

    def pingRequest(self, connection, address, request):
        connection.sendall(pickle.dumps(self.pred))

    def neighborUpdate(self, connection, address, request):
        if request[1] == 1:
            self.updateSucc(request)
        else:
            self.updatePred(request)

    def stabilize(self, connection, address, request):
        self.updateFingerTable()
        connection.sendall(pickle.dumps(self.succ))

    def findNode(self, connection, address, request):
        fileID = getHash(request[1])
        target_node_ip_port = self.getSuccessor(self.succ, fileID)
        connection.sendall(pickle.dumps([1, target_node_ip_port]))
     
    def uploadToNode(self, connection, address, request):
        filename = request[1]
        file_content = request[2]
        self.acceptFile(filename, file_content, True)

    def downloadFromNode(self, connection, address, request):
        filename = request[1]
        self.downloadFile(filename)

    def connectionThread(self, connection, address):
        request = pickle.loads(connection.recv(config.BUFFER))
        message_type = request[0]
        try:
            self.message_handler[message_type](connection, address, request)
            connection.close()
        except KeyError:
            print("Recevied an invalid message type")
            connection.close()

    def listenThread(self):
        while True:
            try:
                connection, address = self.ServerSocket.accept()
                connection.settimeout(120)
                threading.Thread(target=self.connectionThread, args=(connection, address)).start()
            except socket.error:
                pass

    def pingSucc(self):
        while True:
            # Pingin successor every 3 seconds
            time.sleep(3)
            # If only one node, no need to ping
            if self.address == self.succ:
                continue
            try:
                pSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                pSocket.connect(self.succ)
                pSocket.sendall(pickle.dumps([2]))  # Send ping request
                recvPred = pickle.loads(pSocket.recv(config.BUFFER))
            except:
                #When not receiving ping ack from successor. Searching for the next successor from Finger table and stabilizing
                print("\nSuccesor is offline!\nStabilizing...")
                newSuccFound = False
                value = ()
                failed_succ = self.succ
                for key, value in self.fingerTable.items():
                    if value[0] != self.succID:
                        newSuccFound = True
                        break
                if newSuccFound:
                    #Updating successor and informing the successor of its predecessor
                    self.succ = value[1] 
                    self.succID = getHash(self.succ[0] + ":" + str(self.succ[1]))
                    pSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    pSocket.connect(self.succ)
                    pSocket.sendall(pickle.dumps([4, 0, self.address]))
                    pSocket.close()
                else:  
                    #If none found in the fingertable updating both predecessor and successor as itself.
                    self.pred = self.address  
                    self.predID = self.id
                    self.succ = self.address 
                    self.succID = self.id
                self.updateFingerTable()
                self.updateOtherFingerTables()

                print(f"Failed successor: {failed_succ}")
                if failed_succ == self.leader:
                    print("Failed successor was a leader")
                    self.initiateLeaderElection()
                self.printOptions()
            
    def configThread(self):
        self.printOptions()
        action = input()
        match action:
            case '1':
                ip = input("Enter IP of node: ")
                port = input("Enter port: ")
                self.sendJoinRequest(ip, int(port))
                return
            case '2':
                self.leaveNetwork()
                return
            case '3':
                self.printFingerTable()
                return
            case '4':
                print("My ID:", self.id, "Predecessor:", self.predID, "Successor:", self.succID)
                return
            case '5':
                print("Leader: ", self.leaderID)
                return
               

    def start(self):
        threading.Thread(target=self.listenThread, args=()).start()
        threading.Thread(target=self.pingSucc, args=()).start()
        while True:
            self.configThread()

IP = config.IP
PORT = config.PORT
if len(sys.argv) < 3:
    print("IP and PORT not specified as arguments. Using defaults from config file.")
else:
    IP = sys.argv[1]
    PORT = int(sys.argv[2])

myNode = Node(IP, PORT)
print("Node ID:", myNode.id)
myNode.start()
myNode.ServerSocket.close()