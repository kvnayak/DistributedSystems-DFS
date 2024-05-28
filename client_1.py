from flask import Flask, render_template, request, redirect, url_for
import os
import socket
import time
import pickle
import sys, traceback

buffer = 4096

app = Flask(__name__)
UPLOAD_FOLDER = 'uploads'
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

# Ensure the upload folder exists
if not os.path.exists(UPLOAD_FOLDER):
    os.makedirs(UPLOAD_FOLDER)

@app.route('/')
def upload_file():
    return render_template('index.html')

@app.route('/downloader', methods=['GET', 'POST'])
def downloader():
    recvIPport = ("127.0.0.1", 8030)
    filename = request.form['file_name']
    print(filename)
    print("Downloading file", filename)

    sDataList = [1, 0, filename]
    cSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    cSocket.connect(recvIPport)
    cSocket.sendall(pickle.dumps(sDataList))      
    # Receiving confirmation if file found or not
    fileData = cSocket.recv(buffer)
    if fileData == b"NotFound":
        print("File not found:", filename)
        return "File not found"
    else:
        print("Receiving file:", filename)
        #receiveFile(cSocket, filename)

        totalData = b''
        recvSize = 0
        try:
            with open(filename, 'wb') as file:
                while True:
                    fileData = cSocket.recv(buffer)
                    #print(fileData)
                    recvSize += len(fileData)
                    #print(recvSize)
                    if not fileData:
                        break
                    totalData += fileData
                file.write(totalData)
        except Exception as e:
            print(f"An exception occurred: {type(e)._name_}")
            print(f"Reason: {str(e)}")
            print("Traceback:")
            traceback.print_exc()  # Print the full traceback
        return "File downloaded"


@app.route('/uploader', methods=['GET', 'POST'])
def uploader():
    if request.method == 'POST':
        if 'file' not in request.files:
            return 'No file part'
        file = request.files['file']
        print(request)
        print("......")
        print(file.read())
        if file.filename == '':
            return 'No selected file'
        if file:

            #connecting to DNS server
            try:
                dns_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                print("\ndns socket socket ")
                #dns_socket.sendto(b"\x00\x00\x01\x00\x00\x01\x00\x00\x00\x00\x00\x00\x07example\x03com\x00\x00\x01\x00\x01", ("127.0.0.1", 53))
                # Construct a DNS request packet
                packet = b'\xaa\xbb'  # Transaction ID
                packet += b'\x01\x00'  # Standard query with recursion
                packet += b'\x00\x01'  # Questions: 1
                packet += b'\x00\x00'  # Answer RRs: 0
                packet += b'\x00\x00'  # Authority RRs: 0
                packet += b'\x00\x00'  # Additional RRs: 0
                for part in "example.com".split('.'):
                    packet += bytes([len(part)]) + part.encode()
                packet += b'\x00'  # End of domain name
                packet += b'\x00\x01'  # Type A
                packet += b'\x00\x01'  # Class IN

                dns_socket.sendto(packet, ("127.0.0.1", 53))

                print("\ndns socket send to ")
                data, addr = dns_socket.recvfrom(1024)
                print("\ndns socket recvfrom ")
                ip_address = socket.inet_ntoa(data[-4:])
                print("\ndns socket inet_ntoa ")
                dns_socket.close()
            except socket.error as e:
                print(f"Error resolving DNS name: {e}")
                exit(1)

            #Code to save the file in the local
            # print(f"File: {file}")
            # print(f"File name: {file.filename}")
            # filepath = os.path.join(app.config['UPLOAD_FOLDER'], file.filename)
            # file.save(filepath)
            # print("Uploading file", file.filename)
        
            print("Uploading file", file.filename)
            # If not found send lookup request to get peer to upload file
            sDataList = [1]
            if False:
                sDataList.append(1)
            else:
                sDataList.append(-1)
            try:
                filename = file.filename
                recvIPport = ("127.0.0.1", 8030)    #IP and port of the load balancer to send the file
                sDataList = sDataList + [filename]

                #sending the file_name to load_balancer
                cSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                cSocket.connect(recvIPport)
                cSocket.sendall(pickle.dumps(sDataList))

                while True:
                        fileData = file.read(buffer)
                        time.sleep(0.001)
                        #print(fileData)
                        if not fileData:
                            break
                        cSocket.sendall(fileData)
                        print("...........File sent")
                cSocket.close()
                print("File uploaded")
            except Exception as e:
                print(f"An exception occurred: {type(e)._name_}")
                print(f"Reason: {str(e)}")
                print("Traceback:")
                traceback.print_exc()  # Print the full traceback
            except socket.error:
                print("Error in uploading file")

            return 'File successfully uploaded'

if __name__ == '__main__':
    app.run(debug=True)
