import subprocess
import os

def install_gtest():
    subprocess.run("git clone git@github.com:google/googletest.git", shell=True)
    os.chdir("googletest")
    subprocess.run("cmake3 -B build", shell=True)
    subprocess.run("cmake3 --build build", shell=True)
    os.chdir("build")
    subprocess.run("make install", shell=True)

def install_rocksdb_dependencies():
    subprocess.run("sudo yum makecache", shell=True)
    subprocess.run("sudo yum -y install uuid uuid-devel libuuid libuuid-devel", shell=True)
    subprocess.run("sudo yum -y install zlib-devel", shell=True)
    subprocess.run("sudo yum -y install bzip2-devel", shell=True)
    subprocess.run("sudo yum -y install lz4-devel", shell=True)
    subprocess.run("sudo yum -y install snappy-devel", shell=True)
    subprocess.run("sudo yum -y install libzstd-devel", shell=True)

def install_rocksdb():
    if not os.path.exists("v7.4.5.tar.gz"):
        subprocess.run("wget https://github.com/facebook/rocksdb/archive/refs/tags/v7.4.5.tar.gz", shell=True)
    if not os.path.exists("rocksdb-7.4.5"):
        subprocess.run("tar -zxvf v7.4.5.tar.gz", shell=True)
    os.chdir("rocksdb-7.4.5")
    subprocess.run("make static_lib -j 8", shell=True)
    subprocess.run("sudo make install", shell=True)
    os.chdir("..")
    subprocess.run("rm -rf rocksdb-7.4.5 && rm -rf v7.4.5.tar.gz", shell=True)


if __name__ == "__main__":
    install_gtest()
    install_rocksdb_dependencies()
    install_rocksdb()


