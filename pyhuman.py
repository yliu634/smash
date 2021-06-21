from __future__ import annotations

from functools import partial
from copy import copy


class Infix(object):
  def __init__(self, func):
    self.func = func
  
  def __or__(self, other):
    return self.func(other)
  
  def __ror__(self, other):
    return Infix(partial(self.func, other))
  
  def __call__(self, v1, v2):
    return self.func(v1, v2)


instanceof = Infix(isinstance)
curry = Infix(partial)

add = Infix(lambda a, b: a + b)
sub = Infix(lambda a, b: a - b)
mul = Infix(lambda a, b: a * b)
div = Infix(lambda a, b: a / b)

from typing import TypeVar, Iterable, Callable, Dict, List, Tuple, Set, Any

S = TypeVar('S')  # Declare type variable
T = TypeVar('T')  # Declare type variable
K = TypeVar('K')  # Declare type variable
V = TypeVar('V')  # Declare type variable


class MyIterable(Iterable[T]):
  def map(self: MyIterable[T], f: Callable[[T], S]) -> MyList[S]:
    return MyList([f(e) for e in self])
  
  def zip(self: MyIterable[T], another: Iterable[S]) -> MyList[Tuple[T, S]]:
    return MyList(zip(self, another))
  
  def zip3(self: MyIterable[T], u: Iterable[S], v: Iterable[V]) -> MyList[Tuple[T, S, V]]:
    return MyList(zip(self, u, v))
  
  def zipMany(self: MyIterable[T], *others: Iterable) -> MyList[Tuple]:
    return MyList(zip(self, *others))
  
  def fold(self: MyIterable[T], init: S, f: Callable[[S, T], S]) -> S:
    acc = init
    for e in self: acc = f(acc, e)
    return acc
  
  def forEach(self: MyIterable[T], f: Callable[[T], None]) -> None:
    for e in self:
      f(e)
  
  def toList(self: MyIterable[T]) -> MyList[T]:
    return MyList(self)
  
  def toSet(self: MyIterable[T]) -> MySet[T]:
    return MySet(self)
  
  def toMap(self: MyIterable[Tuple[K, V]]) -> MyMap[K, V]:
    return MyMap(self)
  
  def find(self: MyIterable[T], f: Callable[[T], bool]) -> T:
    for e in self:
      if f(e): return e
    return None
  
  def rfind(self: MyIterable[T], f: Callable[[T], bool]) -> T:
    return MyList(reversed(self)).find(f)
  
  def forAll(self: MyIterable[T], f: Callable[[T], bool]) -> bool:
    return self.fold(True, lambda acc, e: acc and f(e))
  
  def contains(self: MyIterable[T], f: Callable[[T], bool]) -> bool:
    return self.find(f) is not None
  
  def take(self: MyIterable[T], n: int) -> MyList[T]:
    return MyList(self)[:n]
  
  def takeWhile(self: MyIterable[T], f: Callable[[T], bool]) -> MyList[T]:
    l = MyList()
    for e in self:
      if f(e):
        l.append(e)
      else:
        break
    return l
  
  def drop(self: MyIterable[T], n: int) -> MyList[T]:
    return MyList(self)[n:]
  
  def dropWhile(self: MyIterable[T], f: Callable[[T], bool]) -> MyList[T]:
    l = MyList()
    met = False
    for e in self:
      if f(e) and not met:
        continue
      met = True
      l.append(e)
    
    return l
  
  def groupBy(self: MyIterable[T], f: Callable[[T], K]) -> MyMap[K, MyList[T]]:
    m = MyMap()
    
    for e in self:
      k = f(e)
      if k not in m: m[k] = MyList()
      m[k].append(e)
    return m
  
  def flatten(self: MyIterable[Iterable[T]]) -> MyList[T]:
    return self.fold(MyList(), lambda acc, e: acc + e).toList()
  
  def reversed(self: MyIterable[Iterable[T]]):
    return reversed(self)
  
  def sortBy(self: MyIterable[Iterable[T]], c: Callable[[T], Any]) -> MyList[T]:
    return sorted(self, key=c).toList()
  
  def size(self: MyIterable[Iterable[T]]) -> int:
    return len(self)
  
  def contentStr(self):
    return ", ".join(self.map(lambda e: str(e)))


class MyList(List[T], MyIterable[T]):
  def filter(self: List[T], f: Callable[[T], bool]) -> MyList[T]:
    return MyList([e for e in self if f(e)])
  
  def indexOf(self: MyIterable[T], f: Callable[[T], bool]) -> int:
    index = -1
    for e in self:
      index += 1
      if f(e): return index
    
    return -1
  
  def rindexOf(self: MyIterable[T], f: Callable[[T], bool]) -> int:
    index = -1
    for e in reversed(self):
      index += 1
      if f(e): return index
    
    return -1
  
  def __str__(self):
    return "[%s]" % self.contentStr()


class MyRange(MyList):
  def __init__(self, *t):
    MyList.__init__(list(range(*t)))


def listOf(*args: T) -> MyList[T]:
  return MyList[T](args)


def toList(self: Iterable[T]) -> MyList[T]:
  if self | instanceof | dict:
    self = self.items()
  elif self | instanceof | range:
    self = list(self)
  
  return MyList(self)


class MySet(Set[T], MyIterable[T]):
  def filter(self: Set[T], f: Callable[[T], bool]) -> MySet[T]:
    return MySet([e for e in self if f(e)])
  
  def __add__(self: Set[T], other: Set[T]) -> Set[T]:
    return self.union(other)
  
  def __str__(self):
    return "{%s}" % self.contentStr()


def setOf(*args: T) -> MySet[T]:
  return MySet[T](args)


def toSet(self: Iterable[T]) -> MySet[T]:
  if self | instanceof | dict:
    self = self.items()
  return MySet(self)


class MyMap(Dict[K, V]):
  def __init__(self, **kwargs):  # known special case of dict.__init__
    """
    dict() -> new empty dictionary
    dict(mapping) -> new dictionary initialized from a mapping object's
        (key, value) pairs
    dict(iterable) -> new dictionary initialized as if via:
        d = {}
        for k, v in iterable:
            d[k] = v
    dict(**kwargs) -> new dictionary initialized with the name=value pairs
        in the keyword argument list.  For example:  dict(one=1, two=2)
    # (copied from class doc)
    """
    if "default" in kwargs.keys():
      self.default = kwargs["default"]
    
    super().__init__()
  
  def __getitem__(self, item):
    if item in self:
      val = super().__getitem__(item)
    else:
      val = self.default(item) if callable(self.default) else copy(self.default)
      self[item] = val
    
    return val
  
  def map(self: MyMap[K, V], f: Callable[[K, V], S]) -> MyList[S]:
    return MyList([f(k, v) for k, v in self.items()])
  
  def zip(self, another) -> MyList:
    return MyList(zip(self.items(), another.items() if another | instanceof | Dict else another))
  
  def fold(self: MyMap[K, V], init: S, f: Callable[[S, K, V], S]) -> S:
    acc = init
    for k, v in self.items(): acc = f(acc, k, v)
    return acc
  
  def forEach(self: MyMap[K, V], f: Callable[[K, V], None]) -> None:
    for k, v in self:
      f(k, v)
  
  def toList(self: MyMap[K, V]) -> MyList[K, V]:
    return MyList(self.items())
  
  def toSet(self: MyMap[K, V]) -> MySet[K, V]:
    return MySet(self.items())
  
  def filter(self: MyMap[K, V], f: Callable[[K, V], bool]) -> MyMap[K, V]:
    return MyMap([(k, v) for k, v in self if f(k, v)])
  
  def __str__(self):
    return "{%s}" % ", ".join(self.map(lambda k, v: str(k) + "->" + str(v)))


def mapOf(*args: Tuple[K, V]) -> MyMap[K, V]:
  return MyMap[K, V](args)


def toMap(self: Iterable[Iterable[K, V]]) -> MyMap[K, V]:
  return MyMap(self)


from forbiddenfruit import curse

for t in (list, tuple, set, dict, map, range):
  for f in (toList, toSet, toMap):
    curse(t, f.__name__, f)
