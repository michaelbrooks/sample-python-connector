#!/bin/sh
export DEBIAN_FRONTEND=noninteractive
sudo apt-get update
sudo apt-get install -yq git mysql-server-5.5 mysql-client-5.5 libmysqlclient-dev redis-server libpython-dev tmux
sudo curl https://bootstrap.pypa.io/get-pip.py > pip_install.py
sudo python pip_install.py
rm pip_install.py
sudo apt-get install -y zsh curl
sudo rm -rf /home/vagrant/.oh-my-zsh
curl -L http://install.ohmyz.sh > ~/install_zsh.sh
zsh -c "echo \"vagrant\" | sh ~/install_zsh.sh"
source ~/.zshrc


