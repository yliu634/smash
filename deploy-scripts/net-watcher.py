import subprocess
import sys, os
import pwd
import base64

import datetime, pytz
from xmlrpc.server import SimpleXMLRPCServer
import xmlrpc.client
import threading, stat

port = 8000


def now():
  now = datetime.datetime.now(tz=pytz.timezone('America/Los_Angeles'))
  return now.strftime("%Y-%m-%d %H:%M:%S-%f")


def cwd():
  return os.getcwd()


def cd(path):
  os.chdir(path)
  return os.getcwd()


def exec(command, sync):
  if sync:
    result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    return result.stdout
  else:
    t = threading.Thread(target=lambda: subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT))
    t.start()
    return ""


exec(["sudo", "pip3", "install", "psutil", "elevate"], True)
from psutil import process_iter
from signal import SIGTERM  # or SIGKILL
from elevate import elevate


def write(fileName, fileContent, ele):
  if ele: elevate(graphical=False)
  f = open(fileName, "wb")
  f.write(base64.b64decode(fileContent))
  f.close()
  os.chmod(fileName, stat.S_IRUSR | stat.S_IRGRP | stat.S_IROTH |
           stat.S_IWUSR | stat.S_IWGRP | stat.S_IWOTH |
           stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)
  if ele:
    uid = pwd.getpwnam('yiliu124')[2]
    os.setuid(uid)
  return ""


def read(fileName):
  elevate(graphical=False)
  f = open(fileName, "rb")
  s = base64.b64encode(f.read()).decode('utf-8')
  f.close()
  uid = pwd.getpwnam('yiliu124')[2]
  os.setuid(uid)
  return s


def quit():
  os._exit(0)


def clean():
  for proc in process_iter():
    elevate(graphical=False)
    for conns in proc.connections(kind='inet'):
      if port == conns.laddr.port:
        proc.send_signal(SIGTERM)
  
  uid = pwd.getpwnam('yiliu124')[2]
  os.setuid(uid)


class MyThread(threading.Thread):
  def __init__(self, host, offset, sync, ele):
    super().__init__()
    self.host = host
    self.offset = offset
    self.sync = sync
    self.ele = ele
  
  def run(self):
    host = self.host
    offset = self.offset
    sync = self.sync
    ele = self.ele
    
    try:
      if ele: elevate(graphical=False)
      
      with xmlrpc.client.ServerProxy("http://%s:%d/" % (host, port)) as proxy:
        if sys.argv[offset] == "now":
          n = proxy.now()
          print("[" + now() + "] " + host + " time: " + n)
        elif sys.argv[offset] == "send":
          if (host == 'machine1'): return
          for i in range(offset + 1, len(sys.argv)):
            fileName = sys.argv[i]
            s = read(fileName)
            proxy.write(fileName, s, ele)
            print("[" + now() + "] " + host + " write done: " + fileName)
        elif sys.argv[offset] == "cd":
          dirName = sys.argv[offset + 1]
          dirName = proxy.cd(dirName)
          print("[" + now() + "] " + host + " cd: " + dirName)
        elif sys.argv[offset] == "cwd":
          dirName = proxy.cwd()
          print("[" + now() + "] " + host + " cd: " + dirName)
        elif sys.argv[offset] == "fetch":
          fileName = sys.argv[offset + 1]
          s = proxy.read(fileName, ele)
          name = host + "/" + fileName
          try:
            os.mkdir(os.path.dirname(name))
          except:
            pass
          f = open(name, "w")
          f.write(s)
          f.close()
          print("[" + now() + "] " + host + " fetch done: " + fileName)
        elif sys.argv[offset] == "quit":
          proxy.quit()
        else:
          commandParts = sys.argv[offset:]
          if commandParts[-1] != '`': commandParts.append('`')

          command = []
          for p in commandParts:
            if p == "`":
              output = proxy.exec(command, sync)
              output = ("\n[%s]" % host).join(str(output).split('\n'))
              print("------------[%s] %s %s------------\n%s" % (now(), host, command, output))
              command = []
            else:
              command.append(p)
          
    except Exception as e:
      print("------------Error on %s: ------------\n%s\n" % (host, str(e)))
    
    uid = pwd.getpwnam('yiliu124')[2]
    os.setuid(uid)


if __name__ == "__main__":
  if len(sys.argv) == 1:
    clean()
    
    server = SimpleXMLRPCServer(("0.0.0.0", port))
    server.register_function(now, "now")
    server.register_function(exec, "exec")
    server.register_function(write, "write")
    server.register_function(read, "read")
    server.register_function(cwd, "cwd")
    server.register_function(cd, "cd")
    server.register_function(quit, "quit")  # dangerous
    server.serve_forever()
  else:  # has a command to run
    nodes = []
    offset = 1
    try:
      while True:
        nodes.append(int(sys.argv[offset]))
        offset += 1
    except:
      pass
    
    if not nodes:
      nodes = list(range(10))
    
    if sys.argv[1] == 'quit':
      nodes = [0]  # always only quit node 0
    
    if sys.argv[offset] == 'elevate':
      offset += 1
      ele = True
    else:
      ele = False
    
    if sys.argv[offset] == 'sync':
      offset += 1
      sync = True
    else:
      sync = False
      if sys.argv[offset] == 'async': offset += 1
    
    threads = []
    for i in nodes:
      host = "node-%d" % i
      threads.append(MyThread(host, offset, sync, ele))
    
    for t in threads:
      t.start()
    
    for t in threads:
      t.join()
