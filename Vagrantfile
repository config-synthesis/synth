# -*- mode: ruby -*-
# vi: set ft=ruby :


# https://www.vagrantup.com/docs/disks/usage.
ENV['VAGRANT_EXPERIMENTAL'] = 'disks'


Vagrant.configure('2') do |config|

  # Set box with pinned version.
  config.vm.box = 'ubuntu/focal64'
  config.vm.box_version = '20201016.0.0'
  config.vm.box_check_update = false

  # Set disk size. We need extra disk size for storing some Docker images.
  config.vm.disk :disk, name: 'root', size: '100GB', primary: true
  config.vm.disk :disk, name: 'docker', size: '100GB', primary: false

  # Virtualbox configuration.
  config.vm.provider "virtualbox" do |vb|
    vb.cpus   = 4
    vb.memory = 16384
  end

  # Provision using the shell. This just makes sure that Python 3 and pip are
  # installed so that the Ansible local provisioner can run correctly, and that
  # Ansible requirements are installed. Vagrant can automatically install
  # Ansible, but it's easier to ensure consistency if done manually.
  config.vm.provision :shell, reset: true, inline: <<-SHELL
    set -xeuo pipefail
    apt-get update
    apt-get install -y python3 python3-pip
    /usr/bin/pip3 install ansible==5.6.0 ansible-core==2.12.4
    mkdir -p /usr/share/ansible/collections
    ansible-galaxy collection install -r /vagrant/requirements.yml -p /usr/share/ansible/collections
    chmod -R 777 /usr/share/ansible
    getent group docker || addgroup docker
    usermod -a -G docker vagrant
  SHELL

  # Provision using Ansible.
  config.vm.provision :ansible_local do |ansible|
    ansible.install  = false
    ansible.playbook = 'playbooks/vagrant.yml'
    ansible.extra_vars = {
      ansible_user: 'vagrant',
      mysql_root_password: '',
    }
  end

end
