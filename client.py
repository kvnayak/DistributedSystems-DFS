from flask import Flask, render_template, request, redirect, url_for, send_file, flash
import os
import socket
import time
import pickle
import sys, traceback
from config import Config

with open('systemconfig.cfg', 'r') as f:
    cfg = Config(f)


buffer = cfg.get('buffer')

app = Flask(__name__)
app.config['SECRET_KEY'] = 'DistributedSystems'


@app.route('/')
def upload_file():
    return render_template('index.html')


@app.route('/downloader', methods=['GET', 'POST'])
def downloader():
    print("Download request received ", request)
    lb_ip = get_loadbalancer_address()
    recvIPport = (lb_ip, int(cfg.get('lb_port')))
    filename = request.form['file_name']
    print(filename)
    print("Downloading file...", filename)

    sDataList = [2, 0, filename]
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
        # save_file(cSocket, filename)

        totalData = b''
        recvSize = 0
        try:
            with open(filename, 'wb') as file:
                count = 0
                while True:
                    fileData = cSocket.recv(buffer)
                    recvSize += len(fileData)
                    if not fileData:
                        break
                    totalData += fileData
                    count += 1
                file.write(totalData)
                file.close()
            print("File downloaded")
            cSocket.close()

        except Exception as e:
            print(f"An exception occurred: {type(e)._name_}")
            print(f"Reason: {str(e)}")
            print("Traceback:")
            traceback.print_exc()

        return send_file(
        filename,
        mimetype='text/plain',
        download_name=filename,
        as_attachment=True
        )


@app.route('/uploader', methods=['GET', 'POST'])
def uploader():
    if request.method == 'POST':
        print("Upload request received ",request)
        if 'file' not in request.files:
            return 'No file part'
        file = request.files['file']
        if file.filename == '':
            return 'No selected file'
        if file:
            # connecting to DNS server
            try:
                lb_ip = get_loadbalancer_address()
            except Exception as e:
                print("Error fetching lb ip")
            # Code to save the file in the local
            # print(f"File: {file}")
            # print(f"File name: {file.filename}")
            # filepath = os.path.join(app.config['UPLOAD_FOLDER'], file.filename)
            # file.save(filepath)
            # print("Uploading file", file.filename)

            print("Uploading file...", file.filename)
            # If not found send lookup request to get peer to upload file
            sDataList = [1]
            if False:
                sDataList.append(1)
            else:
                sDataList.append(-1)
            try:
                filename = file.filename
                # TODO recvIP port to be set by dns
                recvIPport = (lb_ip, int(cfg.get('lb_port')))  # IP and port of the load balancer to send the file
                sDataList = sDataList + [filename]
                # sending the file_name to load_balancer
                cSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                cSocket.connect(recvIPport)
                cSocket.sendall(pickle.dumps(sDataList))
                print("Upload File Name ", filename)
                file.seek(0)
                file_content = file.read()
                time.sleep(0.001)
                cSocket.sendall(file_content)
                print("Upload File Size ", file.tell())
                '''
                file.seek(0)
                while True:
                    fileData = file.read(buffer)
                    time.sleep(0.001)
                    print("Client side file data being sent", fileData)
                    if not fileData:
                        break
                    cSocket.sendall(fileData)
                    print("...........File sent")
                '''
                cSocket.close()
                print("File uploaded")
            except Exception as e:
                print(f"An exception occurred: {type(e).__name__}")
                print(f"Reason: {str(e)}")
                print("Traceback:")
                traceback.print_exc()  # Prints full traceback
                return 'Oh no! File upload unsuccessful :('
            except socket.error:
                print("Error in uploading file")
                return 'Socket error'

            flash('File uploaded')
            return redirect(url_for("upload_file"))


def get_loadbalancer_address():
    # connecting to DNS server
    print("Fetching the load balancer address...")
    try:
        dns_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        print("Local DNS initiated")
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

        local_dns_ip = cfg.get('local_dns_ip')
        local_dns_port = cfg.get('local_dns_port')

        dns_socket.sendto(packet, (local_dns_ip, int(local_dns_port)))
        data, addr = dns_socket.recvfrom(1024)
        ip_address = socket.inet_ntoa(data[-4:])

        print("Load Balancer IP: ", ip_address)
        dns_socket.close()
        return ip_address
    except socket.error as e:
        print(f"Error resolving DNS name: {e}")
        exit(1)

if __name__ == '__main__':
    client_ip = cfg.get('client_system_ip')
    client_port = cfg.get('client_system_port')
    debug_mode = cfg.get('debug_mode')
    print("Client System IP", client_ip)
    print("Client System Port", client_port)
    print("Client System Debug Mode", debug_mode)
    app.run(host = client_ip, port = client_port, debug = debug_mode)