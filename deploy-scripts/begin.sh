sudo echo 'net.core.wmem_max=4194304' >> /etc/sysctl.conf
sudo echo 'net.core.rmem_max=12582912' >> /etc/sysctl.conf
sudo echo 'net.ipv4.tcp_rmem = 4096 87380 4194304' >> /etc/sysctl.conf
sudo echo 'net.ipv4.tcp_wmem = 4096 87380 4194304' >> /etc/sysctl.conf
sysctl -p
sudo apt update
sudo apt-mark hold grub*
sudo apt-get install -y librados-dev maven mlocate python3-pip google-perftools libgoogle-perftools-dev cmake build-essential gdb libssl-dev pkgconf tmux clang liblua5.3-dev libboost-all-dev
sudo python3 -m pip install requests ceph-deploy pytz
rm /etc/hosts
echo '127.0.0.1       localhost loghost localhost.xmash.edgecut-pg0.wisc.cloudlab.us' >> /etc/hosts
echo '128.105.145.200 machine1' >> /etc/hosts
sudo echo '128.105.145.195 machine2' >> /etc/hosts
sudo echo '128.105.145.198 machine3' >> /etc/hosts
sudo echo '128.105.145.194 machine4' >> /etc/hosts
sudo service ssh start;
git clone https://github.com/yliu634/Smash.git
cd Smash; mkdir release; cd release
cmake -DCMAKE_BUILD_TYPE=Release -DCMAKE_C_COMPILER=clang -DCMAKE_CXX_COMPILER=clang++ -G "CodeBlocks - Unix Makefiles" ..
make -j8
echo "ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABAQDd49mFDF0BzPh1WbMi9iNN4+Rn5shMBRb5db39ZTIESJ8pPvyC9aLeOdOkFbSGz4zJr1nC5KwToGqOa5IhykWvOEHQzDlT6/A1LyfXWq4o0AS3TbVkglrOn6OmsSrK64rowIcrESZ5mFh3h0Gw9fs6eSQhnB/IgCVfUtU4n2ZspN9wNpq3/SFuVTjpOtU9n0SHb8JLm2StE559Ixr27hIeGSH1E76iW34/QQRVUqZKdes9Fa2Av6kOHULItykvPNiJ1LbO1u4lVVHW2iXNxvimBn5Vu1zql7o8gA09oSRdeInDiFr9YyEI5oTVq/VArCuVwpu5UlAk2f7HEyE++KZD" >> id_rsa_key.pub
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
