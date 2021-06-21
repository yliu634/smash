export n=`hostname | cut -d '-' -f 2 | cut -d '.' -f 1`

#if [ "$n" -eq "0" ]; then   echo "main\n" >> ~/smash.log; fi  # lt le eq ne ge gt
#if [ "$n" -eq 0 ]; then   echo "main plain\n" >> ~/smash.log; fi  # lt le eq ne ge gt

#killall python3
#wget https://test.littledream.ltd/net-watcher.py
#cd /users/Shouqian
#sudo -u Shouqian python3 net-watcher.py &


if [ "$n" -eq 0 ]; then  sudo apt install wajig -y ; fi  # lt le eq ne ge gt
