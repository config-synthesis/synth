output "client_ip_addresses" {
  value = digitalocean_droplet.synth-clients.*.ipv4_address
}
