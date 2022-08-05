import paramiko
from typing import List

class Server:
    def __init__(self, ip, port, username, passwd) -> None:
        self.ip = ip
        self.port = port
        self.username = username
        self.passwd = passwd

def ssh2(ip,username,passwd,cmd):
    try:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(ip,22,username,passwd,timeout=5)
        stdin,stdout,stderr = ssh.exec_command(cmd)
        # stdin.write("Y") #interact with server, typing Y 
        print(stdout.read())
        # for x in stdout.readlines():
        # print x.strip("n")
        print('%s OK\n'%(ip))
        ssh.close()
    except :
        print('%s Error\n' %(ip))
        

def build_executable(servers: List[Server]):

    commands = [
        "cd /home/kangqihan",
        "rm -rf AnotherRaft",
        "git clone kqh:Holworth/AnotherRaft.git",
        "cd AnotherRaft",
        "CMAKE=/usr/bin/cmake3 scl enable devtoolset-10 \"make build\""
        # "cd AnotherRaft",
        # "mkdir build && cd build",
        # "cmake3 .. -DCMAKE_BUILD_TYPE=Debug -DLOG=off",
        # "make bench_server && make bench_client"
    ]

    ssh_cmd = ""
    for cmd in commands:
        ssh_cmd = ssh_cmd + cmd + ";"
    print(ssh_cmd)

    for server in servers:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(server.ip,22,server.username,server.passwd,timeout=5)
        stdin, stdout, stderr = ssh.exec_command(ssh_cmd)
        print(stdout.readlines())
        ssh.close()

if __name__ == "__main__":
    build_executable([Server("10.118.0.40", "22", "root", "ict#96")])
        
    
    
