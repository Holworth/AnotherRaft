import subprocess
import os

def install_rocksdb():
    if not os.path.exists("v7.4.5.tar.gz"):
        subprocess.run("wget https://github.com/facebook/rocksdb/archive/refs/tags/v7.4.5.tar.gz", shell=True)
    if not os.path.exists("rocksdb-7.4.5"):
        subprocess.run("tar -zxvf v7.4.5.tar.gz", shell=True)
    os.chdir("rocksdb-7.4.5")
    subprocess.run("make static_lib -j 8", shell=True)
    subprocess.run("sudo make install", shell=True)
    os.chdir("..")
    subprocess.run("rm -rf rocksdbv7.4.5 && rm -rf v7.4.5.tar.gz", shell=True)

def install_rocksdb_dependencies():
    subprocess.run("sudo yum makecache", shell=True)
    subprocess.run("sudo yum -y install zlib-devel", shell=True)
    subprocess.run("sudo yum -y install bzip2", shell=True)
    subprocess.run("sudo yum -y install lz4", shell=True)
    subprocess.run("sudo yum -y install snappy", shell=True)
    subprocess.run("sudo yum -y install zstd", shell=True)


if __name__ == "__main__":
    install_rocksdb_dependencies()
    install_rocksdb()


