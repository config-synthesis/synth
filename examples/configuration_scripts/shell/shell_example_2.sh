#!/usr/bin/env bash


apt-get install -y nginx
systemctl start nginx
systemctl enable nginx
