output "vm_public_ip" {
  description = "Public IP of the Dagster VM"
  value       = azurerm_public_ip.dagster.ip_address
}

output "storage_account_name" {
  description = "Storage account name"
  value       = azurerm_storage_account.dagster.name
}

output "storage_container_name" {
  description = "Blob container name"
  value       = azurerm_storage_container.dagster.name
}

output "ssh_command" {
  description = "SSH command to connect to the VM"
  value       = "ssh ${var.admin_username}@${azurerm_public_ip.dagster.ip_address}"
}

output "dagster_url" {
  description = "Dagster webserver URL"
  value       = "http://${azurerm_public_ip.dagster.ip_address}:3000"
}

output "storage_account_primary_connection_string" {
  description = "Storage account connection string"
  value       = azurerm_storage_account.dagster.primary_connection_string
  sensitive   = true
}

output "acr_login_server" {
  description = "ACR login server URL"
  value       = azurerm_container_registry.dagster.login_server
}

output "acr_admin_username" {
  description = "ACR admin username"
  value       = azurerm_container_registry.dagster.admin_username
  sensitive   = true
}

output "acr_admin_password" {
  description = "ACR admin password"
  value       = azurerm_container_registry.dagster.admin_password
  sensitive   = true
}