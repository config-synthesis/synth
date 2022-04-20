#!/bin/bash

rm -rf dependencies
mkdir -p dependencies

docker run --rm -v "$PWD"/dependencies:/lambda/opt lambci/yumda:1 yum install -y git GraphicsMagick jp2a
rm -f dependencies/bin/ssh-agent\;*
