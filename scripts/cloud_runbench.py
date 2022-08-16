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
    def __init__(self, client_id, value_size, put_cnt, servers, cfg_file, fail_num) -> None:
        self.client_id = client_id
        self.value_size = value_size
        self.put_cnt = put_cnt
        self.servers = servers
        self.cfg_file = cfg_file
        self.fail_num = fail_num



def run_kv_server(server: Server, cfg: BenchmarkConfiguration) -> int:
    cmd = "cd /home/kangqihan/AnotherRaft/build; nohup bench/bench_server ../bench/{} ".format(cfg.cfg_file) + str(server.id) + " > /dev/null 2> /dev/null &"
    ssh_cmd = "ssh -i /root/.ssh/FlexibleK_Experiment.pem {}@{}".format(server.username, server.ip) + " \"" + cmd + "\""
    pr = subprocess.run(ssh_cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, shell=True)
    if pr.returncode != 0:
        print("[KvServer {}] start failed, clear benchmark".format(server.id))
        return pr.returncode
    else: 
        print("[KvServer {}] starts up".format(server.id))
        return 0

def run_kv_client(server: Server, clientid: int, valueSize: str, putCnt:int, cfg: BenchmarkConfiguration) -> int:
    cmd = "cd /home/kangqihan/AnotherRaft/build; \
           bench/bench_client ../bench/{} {} {} {}".format(cfg.cfg_file, clientid, valueSize, putCnt)
    ssh_cmd = "ssh -i /root/.ssh/FlexibleK_Experiment.pem {}@{}".format(server.username, server.ip) + " \"" + cmd + "\""

    f = open("./results", "a")
    f.write("------------------ [Benchmark: ValueSize={} PutCnt={}] ------------------- <<<\n".format(valueSize, putCnt))
    pr = subprocess.run(ssh_cmd, stdout=f, shell=True)

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
        cmd = "killall bench_server; killall bench_client; rm -rf /tmp/testdb*; rm -rf /tmp/raft_log*"

        ssh_cmd = "ssh -i ~/.ssh/FlexibleK_Experiment.pem {}@{}".format(server.username, server.ip) + " \"" + cmd + "\""
        pr = subprocess.run(ssh_cmd, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, shell=True)

        if pr.returncode != 0:
            print("[Stop KvServer {} Failed, retry]".format(server.id))
            time.sleep(5)
        else: 
            print("[KvServer {} successfully shut down]".format(server.id))
            break

def run_benchmark(config: BenchmarkConfiguration) -> int:
    kv_servers = config.servers[:-1]
    alive_num = len(kv_servers) - config.fail_num
    kv_servers = kv_servers[0: alive_num]
    kv_client = config.servers[-1]

    print("Alive Server Number = {}".format(alive_num))
    if alive_num <= 0:
        print("Invalid Alive number, check input fail server number")
        exit(1)

    for server in kv_servers:
        r = run_kv_server(server, config)
        if r != 0:
            stop_kv_servers(config.servers[:-1])
            print("[Failed to launch server{}, kill all servers, Retry]".format(server.id))
            return r

    r = run_kv_client(kv_client, config.client_id, config.value_size, config.put_cnt, config)
    stop_kv_servers(kv_servers)
    if r != 0:
        print("[Failed to execute client, kill all servers, Retry]")
        return r
    else:
        print("[KvServer successfully exit]")
        return 0

def run_benchmark_succ(config: BenchmarkConfiguration):
    while True:
        r = run_benchmark(config)
        time.sleep(30)
        if r == 0:
            return
    


if __name__ == "__main__":
    cfg_file = ""
    if len(sys.argv) < 2:
        cfg_file = "cloud_cluster5.cfg"
        print("[Expect one configuration file as parameter, got none, use default: {}]".format(cfg_file))
    else:
        cfg_file = sys.argv[1]
        print("[Use input configuration file: {}]".format(cfg_file))

    servers = [
        Server("172.20.126.134", "22", "root", "", 0),
        Server("172.20.126.135", "22", "root", "", 1),
        Server("172.20.126.136", "22", "root", "", 2),
        Server("172.20.126.137", "22", "root", "", 3),
        Server("172.20.126.138", "22", "root", "", 4),
        Server("172.20.126.139", "22", "root", "", 5)
    ]

    cfgs = [
        BenchmarkConfiguration(0, "4K", 10000, servers, cfg_file, 0),
        BenchmarkConfiguration(0, "8K", 10000, servers, cfg_file, 0),
        BenchmarkConfiguration(0, "16K", 10000, servers, cfg_file, 0),
        BenchmarkConfiguration(0, "32K", 10000, servers, cfg_file, 0),
        BenchmarkConfiguration(0, "64K", 10000, servers, cfg_file, 0),
        BenchmarkConfiguration(0, "128K", 10000, servers, cfg_file, 0),
        BenchmarkConfiguration(0, "256K", 10000, servers, cfg_file, 0),
        BenchmarkConfiguration(0, "512K", 10000, servers, cfg_file, 0),
        BenchmarkConfiguration(0, "1M", 10000, servers, cfg_file, 0),
        BenchmarkConfiguration(0, "2M", 8000, servers, cfg_file, 0),
    ]

    fail_cfgs = [
        BenchmarkConfiguration(0, "4K", 10000, servers, cfg_file, 1),
        BenchmarkConfiguration(0, "8K", 10000, servers, cfg_file, 1),
        BenchmarkConfiguration(0, "16K", 10000, servers, cfg_file, 1),
        BenchmarkConfiguration(0, "32K", 10000, servers, cfg_file, 1),
        BenchmarkConfiguration(0, "64K", 10000, servers, cfg_file, 1),
        BenchmarkConfiguration(0, "128K", 10000, servers, cfg_file, 1),
        BenchmarkConfiguration(0, "256K", 10000, servers, cfg_file, 1),
        BenchmarkConfiguration(0, "512K", 10000, servers, cfg_file, 1),
        BenchmarkConfiguration(0, "1M", 10000, servers, cfg_file, 1),
        BenchmarkConfiguration(0, "2M", 7000, servers, cfg_file, 1),
    ]

    fail_cfgs2 = [
        BenchmarkConfiguration(0, "4K", 10000, servers, cfg_file, 2),
        BenchmarkConfiguration(0, "8K", 10000, servers, cfg_file, 2),
        BenchmarkConfiguration(0, "16K", 10000, servers, cfg_file, 2),
        BenchmarkConfiguration(0, "32K", 10000, servers, cfg_file, 2),
        BenchmarkConfiguration(0, "64K", 10000, servers, cfg_file, 2),
        BenchmarkConfiguration(0, "128K", 10000, servers, cfg_file, 2),
        BenchmarkConfiguration(0, "256K", 10000, servers, cfg_file, 2),
        BenchmarkConfiguration(0, "512K", 10000, servers, cfg_file, 2),
        BenchmarkConfiguration(0, "1M", 10000, servers, cfg_file, 2),
        BenchmarkConfiguration(0, "2M", 7000, servers, cfg_file, 2),
    ]

    for cfg in cfgs:
        run_benchmark_succ(cfg)
