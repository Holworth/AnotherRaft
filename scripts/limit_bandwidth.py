import subprocess
import time
import threading
import sys

class Server:
    def __init__(self, ip, port, username, passwd, id) -> None:
        self.ip = ip
        self.port = port
        self.username = username
        self.passwd = passwd
        self.id = id

def LimitBandwidth (server: Server, bandwidth: str): 
    while True:
        cmd = "tc qdisc del dev eth0 root; tc qdisc add dev eth0 root handle 1:  htb default 11; tc class add dev eth0 parent 1: classid 1:11 htb rate {} ceil {}".format(bandwidth, bandwidth)

        ssh_cmd = "ssh -i ~/.ssh/FlexibleK_Experiment.pem {}@{}".format(server.username, server.ip) + " \"" + cmd + "\""
        pr = subprocess.run(ssh_cmd, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, shell=True)

        if pr.returncode != 0:
            print("[Limit Bandwidth Server{} Failed, retry]".format(server.id))
            time.sleep(5)
        else: 
            print("[Limit Bandwidth Server{} succeed]".format(server.id))
            break


if __name__ == "__main__":
    band = ""
    if len(sys.argv) < 2:
        band = "1Gbit"
    else:
        band = sys.argv[1]
    print("Limit bandwidth = {}".format(band))
    cloud_servers = [
        Server("172.20.126.134", "22", "root", "", 0),
        Server("172.20.126.135", "22", "root", "", 1),
        Server("172.20.126.136", "22", "root", "", 2),
        Server("172.20.126.137", "22", "root", "", 3),
        Server("172.20.126.138", "22", "root", "", 4),
        Server("172.20.126.139", "22", "root", "", 5),
        Server("172.20.126.140", "22", "root", "", 6),
    ]
    for server in cloud_servers:
        LimitBandwidth(server, band)

