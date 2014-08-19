#!/bin/sh
export DEBIAN_FRONTEND=noninteractive
sudo apt-get update
sudo apt-get install -yq git redis-server libpython-dev tmux silversearcher-ag build-essential autoconf libtool pkg-config idle-python2.7

# Mongo
sudo apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv 7F0CEB10
echo 'deb http://downloads-distro.mongodb.org/repo/ubuntu-upstart dist 10gen' | sudo tee /etc/apt/sources.list.d/mongodb.list
sudo apt-get update
sudo apt-get install mongodb-org

sudo curl https://bootstrap.pypa.io/get-pip.py > pip_install.py
sudo python pip_install.py
rm pip_install.py

sudo apt-get install -y zsh curl
chsh -s $(which zsh)
rm -rf ~/.oh-my-zsh
curl -L http://install.ohmyz.sh > ~/install_zsh.sh
zsh -c "echo \"vagrant\" | sh ~/install_zsh.sh"