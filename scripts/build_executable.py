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
    # We have to dealing with the FlexRaft branch individually 
    if str != 'FlexRaft': 
        commands = [
            "cd /home/kangqihan",
            "rm -rf AnotherRaft",
            "git clone kqh:Holworth/AnotherRaft.git -b {}".format(type),
            "cd AnotherRaft",
            "bash scripts/build.sh"
        ]
    else:
        commands = [
            "cd /home/kangqihan",
            "rm -rf FlexRaft",
            "git clone kqh:Holworth/FlexRaft.git -b main",
            "cd FlexRaft",
            "bash scripts/build.sh"
        ]
    ssh_cmd = ""
    for cmd in commands:
        ssh_cmd = ssh_cmd + cmd + ";"
    # print(ssh_cmd)

    ssh_cmd = "sshpass -p {} ssh {}@{}".format(server.passwd, server.username, server.ip) + " \"" + ssh_cmd + "\""
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
    if len(sys.argv) < 2:
        print("Expect at least one parameter, got {}".format(len(sys.argv) - 1))
        exit(1)
    v = sys.argv[1]
    if v != "main" and v != "FlexibleK":
        print("Invalid version parameter {}".format(v))
        exit(1)

    servers = [
        Server("10.118.0.18", "22", "root", "ict#96", 0),
        Server("10.118.0.40", "22", "root", "ict#96", 1),
        Server("10.118.0.42", "22", "root", "ict#96", 2),
        Server("10.118.0.43", "22", "root", "ict#96", 3),
        Server("10.118.0.48", "22", "root", "1357246$", 4),
        Server("10.118.0.49", "22", "root", "1357246$", 5)
    ]

    threads = []
    for server in servers:
        t = threading.Thread(target=build_executable, args=(server, v))
        t.start()
        threads.append(t)
        # build_executable(server, "main")

    for t in threads:
        if t.is_alive():
            t.join()
