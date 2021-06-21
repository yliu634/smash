import subprocess, sys
import threading


class MyThread(threading.Thread):
  def __init__(self):
    super().__init__()
  
  def run(self):
    subprocess.check_output(["ssh", "node-%d" % i, " ".join(sys.argv[1:])])


threads = []
for i in range(0, 10):
  host = "node-%d" % i
  threads.append(MyThread())

for t in threads:
  t.start()

for t in threads:
  t.join()