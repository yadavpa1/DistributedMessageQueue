mkdir -p cmake/build
pushd cmake/build
cmake -DCMAKE_INSTALL_PREFIX=$MY_INSTALL_DIR ../..
make -j 4