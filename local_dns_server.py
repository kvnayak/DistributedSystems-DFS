import dnslib
import socketserver, traceback
from config import Config

with open('systemconfig.cfg', 'r') as f:
    cfg = Config(f)
# Define the IP addresses of your two load balancers
load_balancer_ips = [cfg.get('lb1_ip'), cfg.get('lb2_ip')]
# Define the domain name and record type
domain = "example.com."
record_type = "A"

class DNSRequestHandler(socketserver.BaseRequestHandler):
    def handle(self):
        try:
            data = self.request[0].strip()
            request = dnslib.DNSRecord.parse(data)
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
                        rdata=dnslib.A((load_balancer_ips[0])),
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
if __name__ == '__main__':
    local_dns_ip = cfg.get('local_dns_ip')
    local_dns_port = cfg.get('local_dns_port')
    server = socketserver.UDPServer((local_dns_ip, int(local_dns_port)), DNSRequestHandler)
    print(f"DNS server started on ", local_dns_ip, ":", local_dns_port)
    server.serve_forever()