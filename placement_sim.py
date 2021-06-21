import functools
import math, sys, random
from dataclasses import dataclass, field
import queue

import numpy as np
import multiprocessing as mp

from pyhuman import *

sys.path.append('../plot')
from plot import Ploter

import platform

if platform.system() == "Windows":
  scaling = 1
else:
  scaling = 10

nStorage = 100 * scaling
nRacks = 10 * scaling

nStoragePerRack = nStorage // nRacks
nCapacity = 100 * scaling  # for each node

nRackReplica = 3
nReplicaInRack = 1


@dataclass
class Bucket:
  id: int
  type: str = "device"
  children: MyList = field(default_factory=MyList)  # of Nodes
  capacity: float = nCapacity
  _weight: float = 1.0
  failed: bool = False
  # contents: MySet = field(default_factory=MySet)  # only exists when type == "device"
  load: int = 0
  parent = None
  
  def __getattr__(self, item):
    if item == "load":
      if self.type == "device":
        # return len(self.contents)
        return self.load
      else:
        # return self.children.fold(0, lambda a, n: a + len(n.contents))
        return self.children.fold(0, lambda a, n: a + n.load)
    else:
      return super().__getattr__(item)
  
  def __hash__(self):
    return hash((self.type, self.id))
  
  def __str__(self):
    return "%s#%d %s" % (
      self.type, self.id, ("O" if self.load > nCapacity else "") + ("F" if self.failed else ""))
  
  def __eq__(self, other):
    return other | instanceof | Bucket and other.id == self.id
  
  def __setattr__(self, key, value):
    super().__setattr__(key, value)
    
    if self.parent is not None and key == "_weight":
      rack_ = self.parent
      rack_._weight = rack_.children.fold(0, lambda a, n: a + n.adjustedCapacity()) / rack_.capacity
  
  def adjustedCapacity(self):
    return self.capacity * self._weight
    # if self.type == "device":
    #   return self.capacity * self._weight
    # else:
    #   return self.children.fold(0, lambda a, n: a + n.adjustedCapacity())


root = Bucket(-1, "root", range(nRacks).toList().map(
  lambda ri: Bucket(ri, "rack", range(nStoragePerRack).toList().map(
    lambda di: Bucket(ri * nStoragePerRack + di, "device", capacity=nCapacity)), nCapacity * nStoragePerRack)),
              nCapacity * nStorage)

racks = root.children
devices = root.children.map(lambda rack: rack.children).flatten()

for rack in racks:
  rack.parent = root
  
  for device in rack.children:
    device.parent = rack


def mostLoaded():
  return devices.fold((-1, 0), lambda m, d: (d, d.load) if d.load > m[1] else m)


def leastLoaded():
  return devices.fold((-1, 0), lambda m, d: (d, d.load) if d.load < m[1] else m)


def mostLoadedK(k: int = 1):
  return devices.map(lambda d: (d, d.load)).sortBy(lambda t: -t[1]).take(k).toList()


def leastLoadedK(k: int = 1):
  return devices.map(lambda d: (d, d.load)).sortBy(lambda t: t[1]).take(k).toList()


def loads():
  return devices.map(lambda d: d.load)


firstN = True
algo = 0


def shash(k):
  return abs(hash(k)) / 2 ** 63
  # random.seed(hash(k))
  # return random.random()


# select *n* buckets/devices to place data *k*, in each of the *buckets*
# do not go to descent, which is different from CRUSH
def select(k, buckets: Iterable[Bucket], n: int, t: str):
  res = listOf()
  for bucket in buckets:
    if bucket.children[0].type != t:  # if it is a device or it does not have children, auto raise
      raise Exception("wrong root type")
    
    fail = 0
    for r in range(n):
      failr = 0
      
      while True:
        r_ = r + (fail if firstN else failr * n)
        
        if algo == 0:
          c = bucket.children[abs(hash((r_, k))) % len(bucket.children)]
        elif algo == 1:
          acc = 0
          w = shash((r_, k))
          for item in bucket.children:
            acc += item.adjustedCapacity()
            if w <= acc / bucket.adjustedCapacity():
              c = item
              break
          assert (c is not None)
        elif algo == 2:
          raise Exception("not implemented")
        elif algo == 3:
          max_x = float("-inf")
          for item in bucket.children:
            w = shash((r_, k, item.id))
            x = math.log(w) / item.adjustedCapacity()
            if x > max_x:
              max_x = x
              c = item
        else:
          raise Exception("not existing")
        
        if c in res or c.failed:
          failr += 1
          fail += 1
          if failr >= 30:
            raise Exception("failed")
        else:
          res.append(c)
          break
  return res


test1 = []
test2 = []


def locate(k):
  global test1, test2
  racks = select(k, listOf(root), nRackReplica, "rack")
  test1 += racks.map(lambda d: d.id)
  
  devices = select(k, racks, nReplicaInRack, "device")
  test2 += devices.map(lambda d: d.id/10)
  
  return devices.toSet()


redistributed = MyList()


# assign k to a storage node
def insert(k):
  devices = locate(k)
  for d in devices:
    # d.contents.add(k)
    locOfKeys[k].add(d.id)
    d.load += 1
  
  return True


# remove k from the system
def remove(k):
  devices = locate(k)
  for d in devices:
    # d.contents.remove(k)
    d.load -= 1
    locOfKeys[k].remove(d.id)


locOfKeys = MyMap(default=lambda k: MySet())


# lower the weights of overloaded osds, and raise the weights of underloaded osds.
def reweight_by_utilization(avgLoad, oload, max_change, max_osds):
  toLower = mostLoadedK(max_osds).filter(lambda t: t[1] / avgLoad > oload)
  for d, load in toLower:
    new_weight = max(d._weight - max_change, d._weight * avgLoad / load)  # starts from >= 0 and never goes to < 0
    print("osd#%d overloaded %f->%f" % (d.id, d._weight, new_weight))
    d._weight = new_weight
  
  toRaise = leastLoadedK(max_osds - len(toLower)).filter(lambda t: t[1] / avgLoad < 1 and t[0]._weight < 1)
  for d, load in toRaise:
    new_weight = min(1.0, min(d._weight + max_change, d._weight * avgLoad / load))
    print("osd#%d underloaded %f->%f" % (d.id, d._weight, new_weight))
    d._weight = new_weight
  
  moved = False
  for k, locs in locOfKeys.items():
    newLocs = locate(k).map(lambda d: d.id).toSet()
    if newLocs != locs:
      moved = True
      # print("redist " + str(k) + " " + str(locs) + " -> " + str(newLocs))
      locOfKeys[k] = newLocs
      redistributed.append((k, locs, newLocs))
      
      for id in newLocs.difference(locs):
        # devices[id].contents.add(k)
        devices[id].load += 1
      
      for id in locs.difference(newLocs):
        # devices[id].contents.remove(k)
        devices[id].load -= 1
  return moved


plotData = []


def drawDist(data, name, yTitle='Relative load'):
  plotData.append({
    'type': "violin",
    'figWidth': 600,
    'figHeight': 350,
    'mainColors': ['#0072bc'],
    
    'environmentList': list("{:d}%".format(i * 10) for i in range(1, 11)),
    
    'xFontSize': 16,
    'yFontSize': 20,
    
    'sameColor': True,
    'children': [
      {
        'name': name,
        'xTickRotate': True,
        'xTitle': 'Overall load',
        'yTitle': yTitle,
        # 'yLimit': [0, lambda u: u],
        'samples': data
      },
    ]
  })


def drawRedist(redistData, name):
  plotData.append({
    'type': "line",
    'figWidth': 600,
    'figHeight': 350,
    
    'xLog': False,
    'xGrid': False,
    'yLog': False,
    'yGrid': False,
    
    'xFontSize': 20,
    'xTickRotate': False,
    'yFontSize': 20,
    'legendFontSize': 20,
    
    'solutionList': ('DKV',),
    'legendLoc': 'best',
    'legendColumn': 1,
    
    'markerSize': 8,
    'lineWidth': 2,
    
    'children': [
      {
        'name': name,
        'xTickRotate': True,
        'xTitle': 'Overall load',
        'x': list(i / 10 for i in range(11)),
        'xTicks&Labels': [list(i / 10 for i in range(11)), list("{:d}%".format(i * 10) for i in range(11))],
        'yTitle': '# of redistributions',
        'y': [redistData],
      },
    ]
  })


def clear():
  redistributed.clear()
  locOfKeys.clear()
  for d in devices:
    # d.contents.clear()
    d.load = 0
    d._weight = 1
  for rack in racks:
    rack._weight = 1


def testNoBalancing():
  clear()
  percent = nCapacity * nStorage // 100
  
  distData = []
  for ii in range(nCapacity * nStorage):
    i = random.randrange(2 ** 32)
    while i in locOfKeys:
      i = random.randrange(2 ** 32)
    
    insert(i)
    
    cnt = ii + 1
    if cnt % 1000 == 0:
      print("{:d} keys".format(cnt), end='...')
    
    if cnt % (10 * percent) == 0:
      print()
      avgLoad = cnt * nRackReplica * nReplicaInRack / nStorage
      distData.append(devices.map(lambda d: d.load / avgLoad))
  
  drawDist(distData, "no-balancing")


def testBalanceDaemon(oload, max_change, max_osds):
  clear()
  
  percent = nCapacity * nStorage // 100
  
  loadDistribution = []
  weightDistribution = []
  effectivenessDistribution = []
  redistData = [0]
  for ii in range(nCapacity * nStorage):
    i = random.randrange(2 ** 32)
    while i in locOfKeys:
      i = random.randrange(2 ** 32)
    
    insert(i)
    
    cnt = ii + 1
    if cnt % 1000 == 0:
      print("{:d} keys".format(cnt), end='...')
    
    if cnt % (10 * percent) == 0:
      print()
      avgLoad = cnt * nRackReplica * nReplicaInRack / nStorage
      t = 0
      while reweight_by_utilization(avgLoad, oload, max_change, max_osds) and t < 5: t += 1
      # print("balance syncAllocDaemon {:f}% #relocations {:d}".format(cnt / percent, redistributed.size()))
      
      redistData.append(redistData[-1] + redistributed.fold(0, lambda a, t: a + len(t[1].difference(t[2]))))
      loadDistribution.append(devices.map(lambda d: d.load / avgLoad))
      weightDistribution.append(devices.map(lambda d: d._weight))
      effectivenessDistribution.append(devices.map(lambda d: d.load / avgLoad / d._weight))
  
  drawDist(loadDistribution, "syncAllocDaemon-balancing-load")
  drawDist(weightDistribution, "syncAllocDaemon-balancing-weight", "Weight Adj")
  drawDist(effectivenessDistribution, "syncAllocDaemon-balancing-effectiveness", "Relative load/Weight")
  
  drawRedist(redistData, "redist-syncAllocDaemon-balancing-%s-%s-%d" % (
    str(oload).replace(".", "-"), str(max_change).replace(".", "-"), max_osds))


def testBalanceOnce(oload, max_change, max_osds):
  clear()
  percent = nCapacity * nStorage // 100
  
  loadDistribution = []
  weightDistribution = []
  effectivenessDistribution = []
  redistData = [0]
  for ii in range(nCapacity * nStorage):
    i = random.randrange(2 ** 32)
    while i in locOfKeys:
      i = random.randrange(2 ** 32)
    
    insert(i)
    
    cnt = ii + 1
    if cnt % 1000 == 0:
      print("{:d} keys".format(cnt), end='...')
    
    if cnt % (10 * percent) == 0:
      print()
      avgLoad = cnt * nRackReplica * nReplicaInRack / nStorage
      reweight_by_utilization(avgLoad, 10000, 1000, nStorage)  # back to no reweight
      redistributed.clear()
      t = 0
      while reweight_by_utilization(avgLoad, oload, max_change, max_osds) and t < 5: t += 1
      # print("balance once {:f}% #relocations {:d}".format(cnt / percent, redistributed.size()))
      
      redistData.append(redistData[-1] + redistributed.fold(0, lambda a, t: a + len(t[1].difference(t[2]))))
      loadDistribution.append(devices.map(lambda d: d.load / avgLoad))
      weightDistribution.append(devices.map(lambda d: d._weight))
      effectivenessDistribution.append(devices.map(lambda d: d.load / avgLoad / d._weight))
  
  drawDist(loadDistribution, "once-balancing-load")
  drawDist(weightDistribution, "once-balancing-weight", "Weight Adj")
  drawDist(effectivenessDistribution, "once-balancing-effectiveness", "Relative load/Weight")
  
  drawRedist(redistData, "redist-once-balancing-%s-%s-%d" % (
    str(oload).replace(".", "-"), str(max_change).replace(".", "-"), max_osds))


def testBalanceInsertDelete(oload, max_change, max_osds):
  clear()
  percent = nCapacity * nStorage // 100
  
  loadDistribution = []
  weightDistribution = []
  effectivenessDistribution = []
  redistData = [0]
  for ii in range(nCapacity * nStorage):
    i = random.randrange(2 ** 32)
    while i in locOfKeys:
      i = random.randrange(2 ** 32)
    
    insert(i)
    
    cnt = ii + 1
    if cnt % 1000 == 0:
      print("{:d} keys".format(cnt), end='...')
    
    if cnt % (10 * percent) == 0:
      print()
      avgLoad = cnt * nRackReplica * nReplicaInRack / nStorage
      t = 0
      while reweight_by_utilization(avgLoad, oload, max_change, max_osds) and t < 5: t += 1
      # print("balance once {:f}% #relocations {:d}".format(cnt / percent, redistributed.size()))
      
      redistData.append(redistData[-1] + redistributed.fold(0, lambda a, t: a + len(t[1].difference(t[2]))))
      loadDistribution.append(devices.map(lambda d: d.load / avgLoad))
      weightDistribution.append(devices.map(lambda d: d._weight))
      effectivenessDistribution.append(devices.map(lambda d: d.load / avgLoad / d._weight))
  
  drawDist(loadDistribution, "once-balancing-load")
  drawDist(weightDistribution, "once-balancing-weight", "Weight Adj")
  drawDist(effectivenessDistribution, "once-balancing-effectiveness", "Relative load/Weight")
  
  drawRedist(redistData, "redist-once-balancing-%s-%s-%d" % (
    str(oload).replace(".", "-"), str(max_change).replace(".", "-"), max_osds))


# def testRandom():
#   oneLevel = [[] for i in range(11)]
#   for j in range(11):
#     for i in range(100000):
#       k = hash((0, i, j))
#       oneLevel[j].append(k % 1000)
#
#   plotData.append({
#     'type': "violin",
#     'figWidth': 600,
#     'figHeight': 350,
#     'mainColors': ['#0072bc'],
#
#     'environmentList': list(str(i) for i in range(1, 11)),
#
#     'xFontSize': 16,
#     'yFontSize': 20,
#
#     'sameColor': True,
#     'children': [
#       {
#         'name': 'one-lv-test',
#         'xTickRotate': True,
#         'xTitle': 'Test #',
#         'yTitle': 'Dist',
#         # 'yLimit': [0, lambda u: u],
#         'samples': oneLevel
#       },
#     ]
#   })
#
#   twoLevel = [[] for i in range(11)]
#   for j in range(11):
#     for i in range(100000):
#       lv1 = random.randint(0, 4294967295) % 10
#       lv2 = random.randint(0, 4294967295) % 10
#
#       twoLevel[j].append(lv1 * 10 + lv2)
#
#   plotData.append({
#     'type': "violin",
#     'figWidth': 600,
#     'figHeight': 350,
#     'mainColors': ['#0072bc'],
#
#     'environmentList': list(str(i) for i in range(1, 11)),
#
#     'xFontSize': 16,
#     'yFontSize': 20,
#
#     'sameColor': True,
#     'children': [
#       {
#         'name': 'two-lv-test',
#         'xTickRotate': True,
#         'xTitle': 'Test #',
#         'yTitle': 'Dist',
#         # 'yLimit': [0, lambda u: u],
#         'samples': twoLevel
#       },
#     ]
#   })
#
#   Ploter().plot(plotData)
#

if __name__ == '__main__':
  # testBalanceDaemon(1.1, 0.05, 10)
  testNoBalancing()
  
  plotData.append({
    'type': "violin",
    'figWidth': 600,
    'figHeight': 350,
    'mainColors': ['#0072bc'],
    
    'environmentList': ['lv1', 'lv2'],
    
    'xFontSize': 16,
    'yFontSize': 20,
    
    'sameColor': True,
    'children': [
      {
        'name': 'test',
        'xTickRotate': True,
        'xTitle': 'Tests',
        'yTitle': 'Dist',
        # 'yLimit': [0, lambda u: u],
        'samples': [test1, test2]
      },
    ]
  })
  
  # testBalanceOnce(1.1, 0.05, 10)
  # testBalanceInsertDelete(1.2, 0.05, 10)
  Ploter().plot(plotData)
  
  # testRandom()
