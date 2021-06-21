#pragma once

#include <boost/heap/fibonacci_heap.hpp>

template<class K, class cmp>
class fibonacci_queue : public boost::heap::fibonacci_heap<K, boost::heap::compare<cmp>> {
  typedef boost::heap::fibonacci_heap<K, boost::heap::compare<cmp>> Heap;
  typedef typename boost::heap::fibonacci_heap<K, boost::heap::compare<cmp>>::handle_type handle_type;
  
  unordered_map<K, handle_type> map;
public:
  explicit fibonacci_queue(const cmp &comp) : Heap(comp) {
  }
  
  void push(K const &k) {
    auto h = Heap::push(k);
    map.insert(make_pair(k, h));
  }
  
  void update(K const &k) {
    Heap::update(map[k]);
  }
  
  void increase(K const &k) {
    Heap::increase(map[k]);
  }
  
  void decrease(K const &k) {
    Heap::decrease(map[k]);
  }
  
  void erase(K const &k) {
    Heap::erase(map[k]);
    map.erase(k);
  }
};