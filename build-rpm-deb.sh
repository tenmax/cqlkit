#!/bin/bash

VERSION=$1

rm -f ./build/distributions/*.{rpm,deb}
if [ -d cqlkit ]; then
  rm -rf cqlkit
fi
tar --exclude=*.bat -xvf "build/distributions/cqlkit-${VERSION}.tar"
docker run --rm --name fpm -h fpm -v "$(pwd):/data" -e "VERSION=${VERSION}" tenzer/fpm:no-entrypoint bash /data/fpm/cqlkit-fpm.sh
rm -rf "cqlkit"
mv cqlkit*.{rpm,deb} build/distributions