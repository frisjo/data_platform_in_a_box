variable "resource_group_name" {
  description = "Name of the resource group"
  type        = string
  default     = "rg-air-quality"
}

variable "location" {
  description = "Azure region"
  type        = string
  default     = "swedencentral"
}

variable "storage_account_name" {
  description = "Storage account name (must be globally unique, 3-24 lowercase letters/numbers)"
  type        = string
  default     = "airqualitydatagbg"
}

# For Azure Container Registry
variable "acr_name" {
  description = "Azure Container Registry name (must be globally unique, alphanumeric only)"
  type        = string
  default     = "airqualitygbgacr"
}

variable "vm_size" {
  description = "VM size"
  type        = string
  default     = "Standard_B2s"
}

variable "admin_username" {
  description = "Admin username for the VM"
  type        = string
  default     = "azureuser"
}

variable "ssh_public_key_path" {
  description = "Path to SSH public key"
  type        = string
  default     = "~/.ssh/azure_dagster_rsa.pub"
}

variable "allowed_ssh_ip" {
  description = "IP address allowed to SSH"
  type        = string
  default     = "212.85.67.106"
}

variable "allowed_dagster_ip" {
  description = "IP address allowed to access Dagster UI"
  type        = string
  default     = "212.85.67.106"
}

variable "dagster_port" {
  description = "Port for Dagster webserver"
  type        = number
  default     = 3000
}

variable "environment" {
  description = "Environment name (dev, staging, prod)"
  type        = string
  default     = "dev"
}