import socket
import os

# DNS name for the file upload service
DNS_NAME = "example.com"

# File to be uploaded
FILE_PATH = "myfile.txt"

# Resolve the DNS name using the DNS server simulation
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

# Create a TCP socket
client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

# Connect to the load balancer
try:
    client_socket.connect((ip_address, 8080))  # Assuming the server listens on port 8080
except socket.error as e:
    print(f"Error connecting to the load balancer: {e}")
    exit(1)

# Send the file upload request
try:
    with open(FILE_PATH, "rb") as file:
        file_data = file.read()
        client_socket.sendall(file_data)
except IOError as e:
    print(f"Error reading the file: {e}")
    exit(1)

print("File uploaded successfully!")

# Close the socket
client_socket.close()