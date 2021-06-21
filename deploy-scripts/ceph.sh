
sudo cp -r /opt/.ssh .
sudo chown $USER:othello-PG0 -R .ssh/
sudo chmod 600 .ssh/*

ceph-deploy --username=$USER new node-0
ceph-deploy --username=$USER install node-0
ceph-deploy --overwrite-conf mon create-initial
ceph-deploy --overwrite-conf mgr create node-0

ceph-deploy --overwrite-conf admin node-0 node-1 node-2 node-3 node-4 node-5 node-6 node-7 node-8 node-9
ceph-deploy --username=Shouqian install node-0 node-1 node-2 node-3 node-4 node-5 node-6 node-7 node-8 node-9

ceph-deploy osd create --data /dev/sda2 node-0  # sudo fdisk -l
ceph-deploy osd create --data /dev/sda2 node-1
ceph-deploy osd create --data /dev/sda2 node-2
ceph-deploy osd create --data /dev/sda2 node-3
ceph-deploy osd create --data /dev/sda2 node-4
ceph-deploy osd create --data /dev/sda2 node-5
ceph-deploy osd create --data /dev/sda2 node-6
ceph-deploy osd create --data /dev/sda2 node-7
ceph-deploy osd create --data /dev/sda2 node-8
ceph-deploy osd create --data /dev/sda2 node-9

echo "mon_allow_pool_delete = true " | sudo tee -a /etc/ceph/ceph.conf
ceph tell mon.\* injectargs '--mon-allow-pool-delete=true'

sudo cp *.keyring /etc/ceph/
sudo chmod 666 /etc/ceph/*.keyring
ceph status
ceph osd pool create test 500
#ceph osd  pool  rm test test  --yes-i-really-really-mean-it
ceph status

ceph-deploy disk list node-0

ceph osd df tree