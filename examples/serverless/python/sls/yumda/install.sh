#!/bin/bash

mkdir -p dependencies

docker run --rm -v "$PWD"/dependencies:/lambda/opt lambci/yumda:1 yum install -y git GraphicsMagick jp2a
rm -f dependencies/bin/ssh-agent\;*

cd dependencies
zip -9yr ../dependencies.zip .
cd ..
rm -rf dependencies
