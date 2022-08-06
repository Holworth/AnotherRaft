import paramiko
import time
from typing import List

class Server:
    def __init__(self, ip, port, username, passwd, id) -> None:
        self.ip = ip
        self.port = port
        self.username = username
        self.passwd = passwd
        self.id = id

def build_executable(servers: List[Server], type: str):
    commands = [
        "cd /home/kangqihan",
        "rm -rf AnotherRaft",
        "git clone kqh:Holworth/AnotherRaft.git -b {}".format(type),
        "cd AnotherRaft",
        "CMAKE=/usr/bin/cmake3 scl enable devtoolset-10 \"make build\""
    ]
    ssh_cmd = ""
    for cmd in commands:
        ssh_cmd = ssh_cmd + cmd + ";"
    # print(ssh_cmd)

    for server in servers:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(server.ip,22,server.username,server.passwd,timeout=5, banner_timeout=300)
        stdin, stdout, stderr = ssh.exec_command(ssh_cmd)
        stdout.read()
        ssh.close()
    print("Finish Build Executable File on Servers")

if __name__ == "__main__":
    servers = [
        Server("10.118.0.40", "22", "root", "ict#96", 0),
        Server("10.118.0.42", "22", "root", "ict#96", 1),
        Server("10.118.0.48", "22", "root", "1357246$", 2),
        Server("10.118.0.49", "22", "root", "1357246$", 3)
    ]

    build_executable(servers, "main")
