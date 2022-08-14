import subprocess
import time
import threading

class Server:
    def __init__(self, ip, port, username, passwd, id) -> None:
        self.ip = ip
        self.port = port
        self.username = username
        self.passwd = passwd
        self.id = id

def ClearTestContext (server: Server): 
    while True:
        cmd = "killall bench_server; killall bench_client; rm -rf /tmp/testdb*; rm -rf /tmp/raft_log*"

        ssh_cmd = "sshpass -p {} ssh {}@{}".format(server.passwd, server.username, server.ip) + " \"" + cmd + "\""
        pr = subprocess.run(ssh_cmd, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, shell=True)

        if pr.returncode != 0:
            print("[Clear TestContext Server{} Failed, retry]".format(server.id))
            time.sleep(5)
        else: 
            print("[Clear TestContext Server{} succeed]".format(server.id))
            break


if __name__ == "__main__":
    servers = [
        Server("10.118.0.40", "22", "root", "ict#96", 0),
        Server("10.118.0.42", "22", "root", "ict#96", 1),
        Server("10.118.0.48", "22", "root", "1357246$", 2),
        Server("10.118.0.49", "22", "root", "1357246$", 3)
    ]

    cloud_servers = [
        Server("172.20.126.134", "22", "root", "", 0),
        Server("172.20.126.135", "22", "root", "", 0),
        Server("172.20.126.136", "22", "root", "", 0),
        Server("172.20.126.137", "22", "root", "", 0)
    ]
    threads = []
    for server in cloud_servers:
        t = threading.Thread(target=ClearTestContext, args=(server,))
        t.start()
        threads.append(t)
        # build_executable(server, "main")

    for t in threads:
        if t.is_alive():
            t.join()

