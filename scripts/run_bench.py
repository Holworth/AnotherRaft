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

class BenchmarkConfiguration:
    def __init__(self, client_id, value_size, put_cnt, servers) -> None:
        self.client_id = client_id
        self.value_size = value_size
        self.put_cnt = put_cnt
        self.servers = servers



def run_kv_server(server: Server):
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(server.ip,22,server.username,server.passwd,timeout=5, banner_timeout=300)

    cmd = "cd /home/kangqihan/AnotherRaft/build; bench/bench_server ../bench/cluster.cfg " + str(server.id) + "&"
    stdin, stdout, stderr = ssh.exec_command(cmd);
    ssh.close()

    print("[KvServer {}] starts up".format(server.id))

def run_kv_client(server: Server, clientid: int, valueSize: str, putCnt:int):
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(server.ip,22,server.username,server.passwd,timeout=5,banner_timeout=300)
    cmd = "cd /home/kangqihan/AnotherRaft/build; \
           bench/bench_client ../bench/cluster.cfg {} {} {}".format(clientid, valueSize, putCnt)
    print("Execute client command {}".format(cmd))
    stdin, stdout, stderr = ssh.exec_command(cmd)
    # write the results
    res = stdout.readlines()
    ssh.close()

    with open("./results", "a") as res_file:
        res_file.write(">>> [Benchmark: ValueSize={} PutCnt={}] <<<\n".format(valueSize, putCnt))
        for l in res:
            res_file.write(l)
    res_file.close()


def stop_kv_server(server: Server):
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(server.ip,22,server.username,server.passwd,timeout=5, banner_timeout=300)

    cmd = "killall bench_server; cd /home/kangqihan/AnotherRaft/build; rm -rf testdb*"
    stdin, stdout, stderr = ssh.exec_command(cmd);
    print(stdout.readlines())
    ssh.close()

    print("[KvServer {}] stop".format(server.id))

def run_benchmark(config: BenchmarkConfiguration):
    for server in config.servers[:-1]:
        run_kv_server(server)

    run_kv_client(config.servers[-1], config.client_id, config.value_size, config.put_cnt)

    for server in config.servers[:-1]:
        stop_kv_server(server)
    time.sleep(5)
    


if __name__ == "__main__":
    servers = [
        Server("10.118.0.40", "22", "root", "ict#96", 0),
        Server("10.118.0.42", "22", "root", "ict#96", 1),
        Server("10.118.0.48", "22", "root", "1357246$", 2),
        Server("10.118.0.49", "22", "root", "1357246$", 3)
    ]

    cfgs = [
        # BenchmarkConfiguration(0, "4K", 1000, servers),
        # BenchmarkConfiguration(0, "8K", 1000, servers),
        BenchmarkConfiguration(0, "16K", 100000, servers),
        # BenchmarkConfiguration(0, "32K", 100000, servers),
        # BenchmarkConfiguration(0, "64K", 100000, servers),
        # BenchmarkConfiguration(0, "128K", 100000, servers),
        # BenchmarkConfiguration(0, "256K", 100000, servers),
        # BenchmarkConfiguration(0, "512K", 100000, servers),
        # BenchmarkConfiguration(0, "1M", 10000, servers),
        # BenchmarkConfiguration(0, "2M", 10000, servers),
    ]

    for cfg in cfgs:
        run_benchmark(cfg)
