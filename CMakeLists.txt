cmake_minimum_required(VERSION 3.5)
project(Smash)

# curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py
# sudo apt-get install google-perftools libgoogle-perftools-dev cmake build-essential gdb libssl-dev pkgconf tmux clang liblua5.3-dev libboost-all-dev
# cd ~/Smash ; mkdir release ; cd release ; cmake -DCMAKE_BUILD_TYPE=Release -DCMAKE_C_COMPILER=clang -DCMAKE_CXX_COMPILER=clang++ -G "CodeBlocks - Unix Makefiles" .. ; make -j8
# cd ~/Smash ; mkdir debug ; cd debug ; cmake -DCMAKE_BUILD_TYPE=Debug -DCMAKE_C_COMPILER=clang -DCMAKE_CXX_COMPILER=clang++ -G "CodeBlocks - Unix Makefiles" .. ; make -j8
link_directories(/usr/local/lib)
set(CMAKE_C_COMPILER "/usr/bin/clang")
set(CMAKE_CXX_COMPILER "/usr/bin/clang++")

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -march=native -mavx -maes")
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -march=native -mavx -maes")

IF ((CMAKE_BUILD_TYPE MATCHES Debug) OR (CMAKE_BUILD_TYPE MATCHES RelWithDebInfo))
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DPROFILE ")
  set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -DPROFILE ")
  
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -ggdb ")
  set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -ggdb ")
ENDIF ((CMAKE_BUILD_TYPE MATCHES Debug) OR (CMAKE_BUILD_TYPE MATCHES RelWithDebInfo))

IF ((CMAKE_BUILD_TYPE MATCHES Debug))
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -D_GLIBCXX_DEBUG")
  #  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DFULL_DEBUG")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wl,-no-as-needed -ggdb -O0")
  #  set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -DFULL_DEBUG")
  set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wl,-no-as-needed -ggdb -O0")
ENDIF ((CMAKE_BUILD_TYPE MATCHES Debug))

IF ((CMAKE_BUILD_TYPE MATCHES Release) OR (CMAKE_BUILD_TYPE MATCHES RelWithDebInfo))
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Ofast -DNDEBUG")
  set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Ofast -DNDEBUG")
ENDIF ((CMAKE_BUILD_TYPE MATCHES Release) OR (CMAKE_BUILD_TYPE MATCHES RelWithDebInfo))

include_directories(
  ${Boost_INCLUDE_DIR}
  ${CMAKE_CURRENT_SOURCE_DIR}
)

set(COMMON_SOURCE_FILES
  common.cpp
  utils/farmhash/farmhash.cc
  utils/ClientSock.cpp
  utils/hashutil.cc
  input/input_types.cpp
  ycsb_workload.h
  utils/ycsbrand.h
  )

set(HEADER_FILES
  common.h
  ceph.h
  utils/hash.h
  utils/farmhash/farmhash.h
  utils/hashutil.h
  utils/lfsr64.h
  utils/disjointset.h
  utils/ClientSock.h
  utils/json.hpp
  utils/debugbreak.h
  utils/fibonacci_queue.h
  input/input_types.h
  BloomFilter/bloom_filter.h
  Othello/othello_cp_dp.h
  CuckooPresized/cuckoo_cp_dp.h
  CuckooPresized/cuckoo_ht.h
  VacuumFilter/vacuum_filter.h
  VacuumFilter/better_table.h
  Ludo/ludo_cp_dp.h
  Smash/storage_node.h
  Smash/lookup_fn.h
  Smash/client.h
  Smash/master_fn.h
  Smash/node.h
  Smash/adapters.h
  Smash/socket_node.h
  utils/CompactArray.h
  VacuumFilter/cuckoo_filter.h
  Smash/name_server.h
  utils/fibonacci_queue.h
  Smash/commander.h)

#add_executable(validity
#  ${HEADER_FILES}
#  ${COMMON_SOURCE_FILES}
#  validity.cpp)

add_executable(function
  ${HEADER_FILES}
  ${COMMON_SOURCE_FILES}
  Smash/node.cpp
  simulations.cpp)

#add_executable(sideExperiments
#  ${HEADER_FILES}
#  ${COMMON_SOURCE_FILES}
#  sideExperiments.cpp)

#add_executable(testSocket
#  common.h
#  utils/hash.h
#  utils/farmhash/farmhash.h
#  utils/hashutil.h
#  utils/lfsr64.h
#  utils/disjointset.h
#  utils/ClientSock.h
#  utils/json.hpp
#  utils/debugbreak.h
#  input/input_types.h
#  common.cpp
#  utils/farmhash/farmhash.cc
#  utils/ClientSock.cpp
#  utils/hashutil.cc
#  input/input_types.cpp
#  testSocket.cpp)

IF (APPLE)
  set(CMAKE_THREAD_LIBS_INIT "-lpthread")
  set(CMAKE_HAVE_THREADS_LIBRARY 1)
  set(CMAKE_USE_WIN32_THREADS_INIT 0)
  set(CMAKE_USE_PTHREADS_INIT 1)
  set(THREADS_PREFER_PTHREAD_FLAG ON)
ELSE ()
  set(THREADS_PREFER_PTHREAD_FLAG ON)
  find_package(Threads REQUIRED)
#  target_link_libraries(validity LINK_PUBLIC Threads::Threads)
  target_link_libraries(function LINK_PUBLIC Threads::Threads)
#  target_link_libraries(ycsb LINK_PUBLIC Threads::Threads)
#  target_link_libraries(sideExperiments LINK_PUBLIC Threads::Threads)
#  target_link_libraries(testSocket LINK_PUBLIC Threads::Threads)
ENDIF ()


set(Gperftools_DIR "${CMAKE_CURRENT_LIST_DIR}/cmake/")
find_package(Gperftools REQUIRED)
#target_link_libraries(validity LINK_PUBLIC ${GPERFTOOLS_PROFILER})
target_link_libraries(function LINK_PUBLIC ${GPERFTOOLS_PROFILER})
#target_link_libraries(ycsb LINK_PUBLIC ${GPERFTOOLS_PROFILER})
#target_link_libraries(sideExperiments LINK_PUBLIC ${GPERFTOOLS_PROFILER})
#target_link_libraries(testSocket LINK_PUBLIC ${GPERFTOOLS_PROFILER})

#target_link_libraries(validity LINK_PUBLIC ${Rados_LIBRARIES})
target_link_libraries(function LINK_PUBLIC stdc++fs)
target_link_libraries(function LINK_PUBLIC rados)
target_link_libraries(function LINK_PUBLIC boost_system)
target_link_libraries(function LINK_PUBLIC dl)
#target_link_libraries(ycsb LINK_PUBLIC rados)
#target_link_libraries(sideExperiments LINK_PUBLIC ${Rados_LIBRARIES})
#target_link_libraries(testSocket LINK_PUBLIC ${Rados_LIBRARIES})

find_package(PkgConfig REQUIRED)
pkg_search_module(OPENSSL REQUIRED openssl)
#target_link_libraries(validity LINK_PUBLIC ${OPENSSL_LIBRARIES})
target_link_libraries(function LINK_PUBLIC ${OPENSSL_LIBRARIES})
#target_link_libraries(ycsb LINK_PUBLIC ${OPENSSL_LIBRARIES})
#target_link_libraries(sideExperiments LINK_PUBLIC ${OPENSSL_LIBRARIES})
#target_link_libraries(testSocket LINK_PUBLIC ${OPENSSL_LIBRARIES})

find_package(Boost COMPONENTS REQUIRED)
#target_link_libraries(validity LINK_PUBLIC ${Boost_LIBRARIES})
target_link_libraries(function LINK_PUBLIC ${Boost_LIBRARIES})
#target_link_libraries(ycsb LINK_PUBLIC ${Boost_LIBRARIES})
#target_link_libraries(sideExperiments LINK_PUBLIC ${Boost_LIBRARIES})
#target_link_libraries(testSocket LINK_PUBLIC ${Boost_LIBRARIES})
