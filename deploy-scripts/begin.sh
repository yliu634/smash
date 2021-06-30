sudo echo 'net.core.wmem_max=4194304' >> /etc/sysctl.conf
sudo echo 'net.core.rmem_max=12582912' >> /etc/sysctl.conf
sudo echo 'net.ipv4.tcp_rmem = 4096 87380 4194304' >> /etc/sysctl.conf
sudo echo 'net.ipv4.tcp_wmem = 4096 87380 4194304' >> /etc/sysctl.conf;
sysctl -p;
sudo apt update
sudo apt-mark hold grub*
sudo apt-get install -y librados-dev maven mlocate python3-pip google-perftools libgoogle-perftools-dev cmake build-essential gdb libssl-dev pkgconf tmux clang liblua5.3-dev libboost-all-dev
sudo python3 -m pip install requests ceph-deploy pytz
# rm /etc/hosts
# echo '127.0.0.1       localhost loghost localhost.xmash.edgecut-pg0.wisc.cloudlab.us' >> /etc/hosts
# echo '128.105.145.205 machine1' >> /etc/hosts
echo '128.105.145.211 machine2' >> /etc/hosts
echo '128.105.145.206 machine3' >> /etc/hosts
echo '128.105.145.220 machine4' >> /etc/hosts
service ssh start;
git clone https://github.com/yliu634/Smash.git;
cd Smash; mkdir release; cd release;
cmake -DCMAKE_BUILD_TYPE=Release -DCMAKE_C_COMPILER=clang -DCMAKE_CXX_COMPILER=clang++ -G "CodeBlocks - Unix Makefiles" ..;
make -j8
#echo "" >> id_rsa_key.pub
#cat id_rsa_key.pub >> ~/.ssh/authorized_keys
#if [! -z $1]; 
#then
#ssh-keygen -t  rsa
#
#
#
#cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
#cat ~/.ssh/id_rsa.pub
#fi


