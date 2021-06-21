sudo killall function
rm Smash -rf
git clone http://gitlab.lun-ucsc.space/Shouqian/Smash.git
cd ~/Smash ; mkdir release ; cd release ; cmake -DCMAKE_BUILD_TYPE=RelWithDebInfo -DCMAKE_C_COMPILER=gcc -DCMAKE_CXX_COMPILER=g++ -G "CodeBlocks - Unix Makefiles" .. ; make -j8
cd ~/Smash ; mkdir debug ; cd debug ; cmake -DCMAKE_BUILD_TYPE=Debug -DCMAKE_C_COMPILER=clang -DCMAKE_CXX_COMPILER=clang++ -G "CodeBlocks - Unix Makefiles" .. ; make -j8
