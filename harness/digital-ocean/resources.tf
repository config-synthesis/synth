resource "tls_private_key" "synth-ssh-key" {
  algorithm = "RSA"
}

resource "local_file" "synth-ssh-private-key" {
  filename = "id_rsa"
  sensitive_content = tls_private_key.synth-ssh-key.private_key_pem
  file_permission = "0600"
}

resource "local_file" "synth-ssh-public-key" {
  filename = "id_rsa.pub"
  sensitive_content = tls_private_key.synth-ssh-key.public_key_openssh
  file_permission = "0600"
}

resource "digitalocean_ssh_key" "synth-ssh-key" {
  name = "Synth SSH Key"
  public_key = tls_private_key.synth-ssh-key.public_key_openssh
}

resource "digitalocean_droplet" "synth-clients" {
  # See resource slugs at https://slugs.do-api.dev/.
  count      = 20

  name       = format("synth-client-%d", count.index)

  image      = "ubuntu-20-04-x64"
  region     = "nyc3"
  size       = "so1_5-4vcpu-32gb"
  ssh_keys   = [digitalocean_ssh_key.synth-ssh-key.id]
  monitoring = true
  tags       = ["synth"]
}

resource "digitalocean_firewall" "synth-firewall" {
  name = "synth-firewall"

  tags = ["synth"]

  inbound_rule {
    protocol         = "tcp"
    port_range       = "22"
    source_addresses = ["0.0.0.0/0", "::/0"]
  }

  inbound_rule {
    protocol         = "icmp"
    source_addresses = ["0.0.0.0/0", "::/0"]
  }

  outbound_rule {
    protocol              = "tcp"
    port_range            = "1-65535"
    destination_addresses = ["0.0.0.0/0", "::/0"]
  }

  outbound_rule {
    protocol              = "udp"
    port_range            = "1-65535"
    destination_addresses = ["0.0.0.0/0", "::/0"]
  }

  outbound_rule {
    protocol              = "icmp"
    destination_addresses = ["0.0.0.0/0", "::/0"]
  }
}

resource "local_file" "synth-ssh-config" {
  filename = "ssh.digitalocean"
  content = <<-EOF
%{ for idx, client in digitalocean_droplet.synth-clients ~}
Host client${idx}
    Hostname ${client.ipv4_address}

%{ endfor ~}
Host client*
    User           root
    IdentityFile   id_rsa
EOF
}

resource "local_file" "synth-ansible-inventory" {
  filename = "inventory.digitalocean"
  content = <<-EOF
[digitalocean]

[digitalocean:children]
nodes

[digitalocean:vars]
ansible_user=root
ansible_ssh_common_args='-F ssh.digitalocean'
ansible_ssh_private_key_file=id_rsa
ansible_python_interpreter=/usr/bin/python3

[nodes]
%{ for idx, client in digitalocean_droplet.synth-clients ~}
client${idx} ansible_host=${client.ipv4_address}
%{ endfor ~}
EOF
}
