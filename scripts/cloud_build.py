import time
from typing import List
import threading
import subprocess
import os
import sys


class Server:
    def __init__(self, ip, port, username, passwd, id) -> None:
        self.ip = ip
        self.port = port
        self.username = username
        self.passwd = passwd
        self.id = id

def build_executable(server: Server, type: str):
    commands = [
        "cd /home/kangqihan",
        "rm -rf AnotherRaft",
        "git clone git@github.com:Holworth/AnotherRaft.git -b {}".format(type),
        "cd AnotherRaft",
        "bash scripts/build.sh"
    ]
    ssh_cmd = ""
    for cmd in commands:
        ssh_cmd = ssh_cmd + cmd + ";"
    # print(ssh_cmd)

    ssh_cmd = "ssh -i /root/.ssh/FlexibleK_Experiment.pem {}@{}".format(server.username, server.ip) + " \"" + ssh_cmd + "\""
    print(ssh_cmd)
    # omit output
    while True:
        pr = subprocess.run(ssh_cmd, shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
        if pr.returncode == 0:
            break
        else:
            print("Execution Wrong, do it again")
            time.sleep(5)

    print("Finish Build Executable File on Server {}".format(server.ip))

if __name__ == "__main__":
    v = ""
    if len(sys.argv) < 2:
        v = "main"
    else:
        v = sys.argv[1]
    if v != "main" and v != "FlexibleK" and v != "CRaft":
        print("Invalid version parameter {}".format(v))
        exit(1)

    cloud_servers = [
        Server("172.20.126.134", "22", "root", "", 0),
        Server("172.20.126.135", "22", "root", "", 1),
        Server("172.20.126.136", "22", "root", "", 2),
        Server("172.20.126.137", "22", "root", "", 3)
    ]

    threads = []
    for server in cloud_servers:
        t = threading.Thread(target=build_executable, args=(server, v))
        t.start()
        threads.append(t)
        # build_executable(server, "main")

    for t in threads:
        if t.is_alive():
            t.join()
