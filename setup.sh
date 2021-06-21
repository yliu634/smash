# From Ubuntu 18.04
echo 'net.core.wmem_max=4194304' >> /etc/sysctl.conf
echo 'net.core.rmem_max=12582912' >> /etc/sysctl.conf
echo 'net.ipv4.tcp_rmem = 4096 87380 4194304' >> /etc/sysctl.conf
echo 'net.ipv4.tcp_wmem = 4096 87380 4194304' >> /etc/sysctl.conf
sysctl -p

sudo apt update
sudo apt-mark hold grub*
sudo apt-get install -y librados-dev maven mlocate python3-pip google-perftools libgoogle-perftools-dev cmake build-essential gdb libssl-dev pkgconf tmux clang liblua5.3-dev libboost-all-dev
sudo python3 -m pip install requests ceph-deploy pytz

rm Smash -rf
git clone http://gitlab.lun-ucsc.space/Shouqian/Smash.git
cd ~/Smash ; mkdir release ; cd release ; cmake -DCMAKE_BUILD_TYPE=RelWithDebInfo -DCMAKE_C_COMPILER=gcc -DCMAKE_CXX_COMPILER=g++ -G "CodeBlocks - Unix Makefiles" .. ; make -j8
cd ~/Smash ; mkdir debug ; cd debug ; cmake -DCMAKE_BUILD_TYPE=Debug -DCMAKE_C_COMPILER=gcc -DCMAKE_CXX_COMPILER=g++ -G "CodeBlocks - Unix Makefiles" .. ; make -j8

exit




cd ~ ; python3 ~/net-watcher.py sync cd ~ ; \
python3 ~/net-watcher.py sync wget https://test.littledream.ltd/smash.sh -O smash.sh ; \
python3 ~/net-watcher.py sync sudo -u Shouqian bash smash.sh ; \
python3 ~/net-watcher.py sync cd ~/Smash ; cd ~/Smash ; \
python3 ~/net-watcher.py sync ls -l release/function ; \
python3 ~/net-watcher.py sync ls -l debug/function ; \
vi config.txt

python3 ~/net-watcher.py sync send config.txt
python3 ~/net-watcher.py sync mkdir dist
cp ~/YCSB/keys.txt ./dist/
python3 ~/net-watcher.py sync send dist/keys.txt
python3 ~/net-watcher.py sync ps aux | grep function

python3 ~/net-watcher.py sync cd ~/Smash ; cd ~/Smash ; \
python3 ~/net-watcher.py sync sudo killall -SIGKILL function ; \
python3 ~/net-watcher.py sync rm dist/log.txt ; \
python3 ~/net-watcher.py 0 async release/function ns ; sleep 1; \
python3 ~/net-watcher.py !!!<7 8 9> async release/function master ; \
python3 ~/net-watcher.py async release/function lookup ; \
python3 ~/net-watcher.py async release/function storage !!!<768> dist/store ; \
python3 ~/net-watcher.py sync ps aux | grep function
 # 768 is 3GiB, but will fail. 2560 is 10 GiB
python3 ~/net-watcher.py 0 1 2 3 4 sync ./release/function loadS a 5

./release/function cmd
./release/function loadS a 1

python3 ~/net-watcher.py sync sudo killall -SIGKILL function ; \
python3 ~/net-watcher.py sync rm dist/log.txt ; \
python3 ~/net-watcher.py sync ps aux | grep function

python3 ~/net-watcher.py sync cd ~/Smash ; cd ~/Smash ; \
python3 ~/net-watcher.py sync sudo killall -SIGKILL function ; \
python3 ~/net-watcher.py sync rm dist/log.txt ; \
python3 ~/net-watcher.py 0 async release/function ns ; sleep 1; \
python3 ~/net-watcher.py 8 9 async release/function master ; \
python3 ~/net-watcher.py async release/function lookup ; \
python3 ~/net-watcher.py async release/function storage 512 dist/store ; \
python3 ~/net-watcher.py sync ps aux | grep function

#./debug/function cmd
#./debug/function loadS a 1
python3 ~/net-watcher.py 0 1 2 3 4 sync ./debug/function loadS a 5
gdb --args ./debug/function runS a 1
python3 ~/net-watcher.py 0 1 2 3 4 sync ./debug/function runS a 5

python3 ~/net-watcher.py sync ps aux | grep function
python3 ~/net-watcher.py 0 1 2 3 4 sync ./release/function loadS a 5
python3 ~/net-watcher.py 0 1 2 3 4 sync ./release/function runS a 5
python3 ~/net-watcher.py sync ps aux | grep function

python3 ~/net-watcher.py sync fetch dist/log.txt

python3 ~/net-watcher.py sync cd ~ ; cd ~
python3 ~/net-watcher.py sync send ceph.conf
python3 ~/net-watcher.py sync sudo cp ceph.conf /etc/ceph/ceph.conf
ceph-deploy --overwrite-conf admin node-0 node-1 node-2 node-3 node-4 node-5 node-6 node-7 node-8 node-9
python3 ~/net-watcher.py sync sudo chmod 644 /etc/ceph/*
python3 ~/net-watcher.py sync cd ~/Smash ; cd ~/Smash
mkdir; cp ~/YCSB/keys.txt ./dist/
python3 ~/net-watcher.py sync mkdir dist
python3 ~/net-watcher.py sync send dist/keys.txt
python3 ~/net-watcher.py 0 1 2 3 4 sync ./release/function loadC a 5

`python3 ~/net-watcher.py sync cd ~/Smash ; cd ~/Smash ; \
python3 ~/net-watcher.py sync sudo killall -SIGKILL function ; \
python3 ~/net-watcher.py sync rm dist/log.txt ; \
python3 ~/net-watcher.py 0 async debug/function ns ; sleep 1; \
python3 ~/net-watcher.py 8 9 async debug/function master ; \
python3 ~/net-watcher.py async debug/function lookup ; \
python3 ~/net-watcher.py async debug/function storage 512 dist/store ; \
python3 ~/net-watcher.py sync ps aux | grep function
`
python3 ssh.py ulimit -c unlimited
ps aux | grep function


sudo gdbserver 0.0.0.0:8341 --attach $(ps aux | grep 'function master' | head -1 | awk '{print $2}')

sudo gdb attach $(ps aux | grep 'function master' | head -1 | awk '{print $2}')


iperf -s &

iperf -c node-0

python3 ~/net-watcher.py 5 sync iperf -c node-0 &
python3 ~/net-watcher.py 6 sync iperf -c node-1 &
python3 ~/net-watcher.py 7 sync iperf -c node-2 &
python3 ~/net-watcher.py 8 sync iperf -c node-3 &
python3 ~/net-watcher.py 9 sync iperf -c node-4 &

wget https://test.littledream.ltd/common.sh ; bash ./common.sh

sudo cp -r /opt/.ssh .
sudo chown Shouqian:othello-PG0 -R .ssh/

ceph-deploy --username=Shouqian new node-0
ceph-deploy --username=Shouqian install node-0
#ceph-deploy --username=Shouqian install node-0 node-1 node-2 node-3 node-4 node-5 node-6 node-7 node-8 node-9
ceph-deploy --overwrite-conf mon create-initial
ceph-deploy --overwrite-conf admin node-0 node-1 node-2 node-3 node-4 node-5 node-6 node-7 node-8 node-9
ceph-deploy --overwrite-conf mgr create node-0
ceph-deploy --overwrite-conf osd create --data /dev/sda2 node-0  # sudo fdisk -l
ceph-deploy --overwrite-conf osd create --data /dev/sda2 node-1
ceph-deploy --overwrite-conf osd create --data /dev/sda2 node-2
ceph-deploy --overwrite-conf osd create --data /dev/sda2 node-3
ceph-deploy --overwrite-conf osd create --data /dev/sda2 node-4
ceph-deploy --overwrite-conf osd create --data /dev/sda2 node-5
ceph-deploy --overwrite-conf osd create --data /dev/sda2 node-6
ceph-deploy --overwrite-conf osd create --data /dev/sda2 node-7
ceph-deploy --overwrite-conf osd create --data /dev/sda2 node-8
ceph-deploy --overwrite-conf osd create --data /dev/sda2 node-9

cd
git clone http://github.com/brianfrankcooper/YCSB.git
cd YCSB
mvn -pl site.ycsb:rados-binding -am clean package
vi workloads/workloada
sudo ./bin/ycsb load rados -s -P workloads/workloada -p "rados.configfile=/etc/ceph/ceph.conf" -p "rados.id=admin" -p "rados.pool=test" > outputLoad.txt
#sudo ./bin/ycsb run rados -s -P workloads/workloada -p "rados.configfile=/etc/ceph/ceph.conf" -p "rados.id=admin" -p "rados.pool=test" > outputRun.txt

python3 ~/net-watcher.py sync cd ~ ; cd ~
python3 ~/net-watcher.py sync send ceph*
python3 ~/net-watcher.py sync sudo cp ceph* /etc/ceph/
python3 ~/net-watcher.py sync sudo chown Shouqian:othello-PG0 /etc/ceph/*

sudo cp *.keyring /etc/ceph/
sudo chmod 666 /etc/ceph/*.keyring
ceph status
ceph osd pool create test 500
#ceph osd  pool  rm test test  --yes-i-really-really-mean-it
ceph status

echo "mon_allow_pool_delete = true " | sudo tee -a /etc/ceph/ceph.conf
ceph tell mon.\* injectargs '--mon-allow-pool-delete=true'


#rados -p test get user50460765512123115 user50460765512123115.txt  # see obj content
rados -p test ls --output keys.txt  # list all objects in pg

ceph pg dump
ceph-deploy disk list node-0

ceph osd  pool  rm test test  --yes-i-really-really-mean-it ; \
ceph osd pool create test 500 ; sleep 3; \
ceph status

ceph osd in 0
ceph osd in 1
ceph osd in 2
ceph osd in 3
ceph osd in 4
ceph osd in 5
ceph osd in 6
ceph osd in 7
ceph osd in 8
ceph osd in 9

ceph osd out 0
ceph osd out 1
ceph osd out 2
ceph osd out 3
ceph osd out 4
ceph osd out 5
ceph osd out 6
ceph osd out 7
ceph osd out 8
ceph osd out 9

ceph osd down 0
ceph osd down 1
ceph osd down 2
ceph osd down 3
ceph osd down 4
ceph osd down 5
ceph osd down 6
ceph osd down 7
ceph osd down 8
ceph osd down 9

ceph osd destroy 0 --yes-i-really-mean-it
ceph osd destroy 1 --yes-i-really-mean-it
ceph osd destroy 2 --yes-i-really-mean-it
ceph osd destroy 3 --yes-i-really-mean-it
ceph osd destroy 4 --yes-i-really-mean-it
ceph osd destroy 5 --yes-i-really-mean-it
ceph osd destroy 6 --yes-i-really-mean-it
ceph osd destroy 7 --yes-i-really-mean-it
ceph osd destroy 8 --yes-i-really-mean-it
ceph osd destroy 9 --yes-i-really-mean-it
ceph osd rm all

ceph-deploy purge node-0
ceph-deploy purgedata node-0
rm ceph.*

sudo systemctl start ceph-osd@6.service  # change 6 to any

python3 ~/net-watcher.py sync sudo systemctl stop ceph\*.service ceph\*.target


#python3 ~/net-watcher.py sync mkdir ~/Smash
#python3 ~/net-watcher.py sync cd ~/Smash ; cd ~/Smash
#python3 net-watcher.py sync unzip ../75*
#python3 ~/net-watcher.py sync mkdir release debug
#python3 ~/net-watcher.py sync cd release
#python3 ~/net-watcher.py sync cmake -DCMAKE_BUILD_TYPE=Release -DCMAKE_C_COMPILER=gcc -DCMAKE_CXX_COMPILER=g++ -G "CodeBlocks - Unix Makefiles" ..
#python3 ~/net-watcher.py sync make -j8
#python3 ~/net-watcher.py sync cd debug
#python3 ~/net-watcher.py sync cmake -DCMAKE_BUILD_TYPE=Debug -DCMAKE_C_COMPILER=clang -DCMAKE_CXX_COMPILER=clang++ -G "CodeBlocks - Unix Makefiles" ..
#python3 ~/net-watcher.py sync make -j8