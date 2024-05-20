import dnslib
import socketserver, traceback

# Define the IP addresses of your two load balancers
load_balancer_ips = ["127.0.0.1", "192.168.1.101"]

# Define the domain name and record type
domain = "example.com."
record_type = "A"

class DNSRequestHandler(socketserver.BaseRequestHandler):
    def handle(self):
        try:
            data = self.request[0].strip()
            request = dnslib.DNSRecord.parse(data)
            print("\n DNS request before if "+ str(request.q.qname) + " "+ domain +" "+ str(request.q.qtype)+ " "+str(getattr(dnslib.QTYPE, record_type)))
            # Check if the request is for the defined domain and record type
            if str(request.q.qname) == domain and request.q.qtype == getattr(dnslib.QTYPE, record_type):
                # Rotate the IP addresses for round-robin load balancing
                print("\n DNS request match found")
                load_balancer_ips.append(load_balancer_ips.pop(0))

                # Create the DNS response with the rotated IP address
                reply = dnslib.DNSRecord(dnslib.DNSHeader(id=request.header.id, qr=1, aa=1, ra=1), q=request.q)
                reply.add_answer(
                    dnslib.RR(
                        rname=request.q.qname,
                        rtype=getattr(dnslib.QTYPE, record_type),
                        rdata=dnslib.A(load_balancer_ips[0]),
                        ttl=60,
                    )
                )
                self.request[1].sendto(reply.pack(), self.client_address)
        except Exception as e:
            print(f"An exception occurred: {type(e).__name__}")
            print(f"Reason: {str(e)}")
            print("Traceback:")
            traceback.print_exc()  # Print the full traceback


# Start the DNS server
server = socketserver.UDPServer(("0.0.0.0", 53), DNSRequestHandler)
print(f"DNS server started on 0.0.0.0:53")
server.serve_forever()