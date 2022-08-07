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

class BenchmarkConfiguration:
    def __init__(self, client_id, value_size, put_cnt, servers) -> None:
        self.client_id = client_id
        self.value_size = value_size
        self.put_cnt = put_cnt
        self.servers = servers



def run_kv_server(server: Server) -> int:
    cmd = "cd /home/kangqihan/AnotherRaft/build; bench/bench_server ../bench/cluster.cfg " + str(server.id) + "&"
    ssh_cmd = "sshpass -p {} ssh {}@{}".format(server.passwd, server.username, server.ip) + " \"" + cmd + "\""
    pr = subprocess.run(ssh_cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, shell=True)
    if pr.returncode != 0:
        return pr.returncode
    else: 
        print("[KvServer {}] starts up".format(server.id))
        return 0

def run_kv_client(server: Server, clientid: int, valueSize: str, putCnt:int) -> int:
    cmd = "cd /home/kangqihan/AnotherRaft/build; \
           bench/bench_client ../bench/cluster.cfg {} {} {}".format(clientid, valueSize, putCnt)
    ssh_cmd = "sshpass -p {} ssh {}@{}".format(server.passwd, server.username, server.ip) + " \"" + cmd + "\""
    pr = subprocess.run(ssh_cmd, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, shell=True)

    if pr.returncode != 0:
        return pr.returncode
    else: 
        print("Execute client command {}".format(cmd))
        return 0

    # with open("./results", "a") as res_file:
    #     res_file.write(">>> [Benchmark: ValueSize={} PutCnt={}] <<<\n".format(valueSize, putCnt))
    #     for l in res:
    #         res_file.write(l)
    # res_file.close()


def stop_kv_servers(servers: List[Server]):
    for server in servers:
        stop_kv_server(server)

def stop_kv_server(server: Server):
    while True:
        cmd = "killall bench_server; cd /home/kangqihan/AnotherRaft/build; rm -rf testdb*"

        ssh_cmd = "sshpass -p {} ssh {}@{}".format(server.passwd, server.username, server.ip) + " \"" + cmd + "\""
        pr = subprocess.run(ssh_cmd, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, shell=True)

        if pr.returncode != 0:
            print("[Stop KvServer {} Failed, retry]".format(server.id))
            time.sleep(5)
        else: 
            print("[KvServer {} successfully shut down]".format(server.id))
            break

def run_benchmark(config: BenchmarkConfiguration) -> int:
    for server in config.servers[:-1]:
        r = run_kv_server(server)
        if r != 0:
            stop_kv_servers(config.servers[:-1])
            sys.stderr.write("[Failed to launch server{}, kill all servers, Retry]".format(server.id))
            return r

    r = run_kv_client(config.servers[-1], config.client_id, config.value_size, config.put_cnt)
    stop_kv_servers(config.servers[:-1])
    if r != 0:
        sys.stderr.write("[Failed to execute client, kill all servers, Retry]")
        return r
    else:
        sys.stdout.write("[KvServer successfully exit]")
        return 0

def run_benchmark_succ(config: BenchmarkConfiguration):
    while True:
        r = run_benchmark(config)
        time.sleep(10)
        if r == 0:
            return
    


if __name__ == "__main__":
    servers = [
        Server("10.118.0.40", "22", "root", "ict#96", 0),
        Server("10.118.0.42", "22", "root", "ict#96", 1),
        Server("10.118.0.48", "22", "root", "1357246$", 2),
        Server("10.118.0.49", "22", "root", "1357246$", 3)
    ]

    cfgs = [
        BenchmarkConfiguration(0, "4K", 1000, servers),
        # BenchmarkConfiguration(0, "8K", 1000, servers),
        # BenchmarkConfiguration(0, "16K", 100000, servers),
        # BenchmarkConfiguration(0, "32K", 100000, servers),
        # BenchmarkConfiguration(0, "64K", 100000, servers),
        # BenchmarkConfiguration(0, "128K", 100000, servers),
        # BenchmarkConfiguration(0, "256K", 100000, servers),
        # BenchmarkConfiguration(0, "512K", 100000, servers),
        # BenchmarkConfiguration(0, "1M", 10000, servers),
        # BenchmarkConfiguration(0, "2M", 10000, servers),
    ]

    for cfg in cfgs:
        run_benchmark_succ(cfg)
