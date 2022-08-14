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

        ssh_cmd = "ssh -i ~/.ssh/FlexibleK_Experiment.pem {}@{}".format(server.passwd, server.username, server.ip) + " \"" + cmd + "\""
        pr = subprocess.run(ssh_cmd, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, shell=True)

        if pr.returncode != 0:
            print("[Clear TestContext Server{} Failed, retry]".format(server.id))
            time.sleep(5)
        else: 
            print("[Clear TestContext Server{} succeed]".format(server.id))
            break


if __name__ == "__main__":
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
        # ClearTestContext(server)

    for t in threads:
        if t.is_alive():
            t.join()
