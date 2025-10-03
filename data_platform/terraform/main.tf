terraform {
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.0"
    }
  }
}

provider "azurerm" {
  features {}
}

# Resource Group
resource "azurerm_resource_group" "dagster" {
  name     = var.resource_group_name
  location = var.location
}

# Storage Account for Dagster
resource "azurerm_storage_account" "dagster" {
  name                     = var.storage_account_name
  resource_group_name      = azurerm_resource_group.dagster.name
  location                 = azurerm_resource_group.dagster.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
}

# Blob Container
resource "azurerm_storage_container" "dagster" {
  name                  = "dagster-storage"
  storage_account_name  = azurerm_storage_account.dagster.name
  container_access_type = "private"
}

# Virtual Network
resource "azurerm_virtual_network" "dagster" {
  name                = "dagster-vnet"
  address_space       = ["10.0.0.0/16"]
  location            = azurerm_resource_group.dagster.location
  resource_group_name = azurerm_resource_group.dagster.name
}

# Subnet
resource "azurerm_subnet" "dagster" {
  name                 = "dagster-subnet"
  resource_group_name  = azurerm_resource_group.dagster.name
  virtual_network_name = azurerm_virtual_network.dagster.name
  address_prefixes     = ["10.0.1.0/24"]
}

# Public IP
resource "azurerm_public_ip" "dagster" {
  name                = "dagster-public-ip"
  location            = azurerm_resource_group.dagster.location
  resource_group_name = azurerm_resource_group.dagster.name
  allocation_method   = "Static"
  sku                 = "Standard"
}

# Network Security Group
resource "azurerm_network_security_group" "dagster" {
  name                = "dagster-nsg"
  location            = azurerm_resource_group.dagster.location
  resource_group_name = azurerm_resource_group.dagster.name

  # SSH access
  security_rule {
    name                       = "SSH"
    priority                   = 1001
    direction                  = "Inbound"
    access                     = "Allow"
    protocol                   = "Tcp"
    source_port_range          = "*"
    destination_port_range     = "22"
    source_address_prefix      = var.allowed_ssh_ip
    destination_address_prefix = "*"
  }

  # Dagster webserver (default port 3000)
  security_rule {
    name                       = "Dagster"
    priority                   = 1002
    direction                  = "Inbound"
    access                     = "Allow"
    protocol                   = "Tcp"
    source_port_range          = "*"
    destination_port_range     = "3000"
    source_address_prefix      = var.allowed_dagster_ip
    destination_address_prefix = "*"
  }
}

# Network Interface
resource "azurerm_network_interface" "dagster" {
  name                = "dagster-nic"
  location            = azurerm_resource_group.dagster.location
  resource_group_name = azurerm_resource_group.dagster.name

  ip_configuration {
    name                          = "internal"
    subnet_id                     = azurerm_subnet.dagster.id
    private_ip_address_allocation = "Dynamic"
    public_ip_address_id          = azurerm_public_ip.dagster.id
  }
}

# Associate NSG with NIC
resource "azurerm_network_interface_security_group_association" "dagster" {
  network_interface_id      = azurerm_network_interface.dagster.id
  network_security_group_id = azurerm_network_security_group.dagster.id
}

# Virtual Machine
resource "azurerm_linux_virtual_machine" "dagster" {
  name                = "dagster-vm"
  resource_group_name = azurerm_resource_group.dagster.name
  location            = azurerm_resource_group.dagster.location
  size                = var.vm_size
  admin_username      = var.admin_username

  network_interface_ids = [
    azurerm_network_interface.dagster.id,
  ]

  admin_ssh_key {
    username   = var.admin_username
    public_key = file(var.ssh_public_key_path)
  }

  os_disk {
    caching              = "ReadWrite"
    storage_account_type = "Premium_LRS"
    disk_size_gb         = 100
  }

  source_image_reference {
    publisher = "Canonical"
    offer     = "0001-com-ubuntu-server-jammy"
    sku       = "22_04-lts-gen2"
    version   = "latest"
  }

# Install Docker and dependencies
  custom_data = base64encode(<<-EOF
    #!/bin/bash
    set -e
    
    # Update system
    apt-get update
    apt-get upgrade -y
    
    # Install Docker
    curl -fsSL https://get.docker.com -o get-docker.sh
    sh get-docker.sh
    rm get-docker.sh
    
    # Add user to docker group
    usermod -aG docker ${var.admin_username}
    
    # Install Docker Compose
    apt-get install -y docker-compose-plugin
    
    # Create directory for Dagster
    mkdir -p /opt/dagster
    chown ${var.admin_username}:${var.admin_username} /opt/dagster
    
    # Install Azure CLI (for blob storage and ACR access)
    curl -sL https://aka.ms/InstallAzureCLIDeb | bash
    
    # Configure Docker to use managed identity for ACR
    az login --identity
    
    # Enable Docker service
    systemctl enable docker
    systemctl start docker
    
    # Log installation completion
    echo "VM setup completed at $(date)" > /opt/dagster/setup.log
    
  EOF
  )

  identity {
    type = "SystemAssigned"
  }
}

# Assign Storage Blob Data Contributor role to VM
resource "azurerm_role_assignment" "dagster_storage" {
  scope                = azurerm_storage_account.dagster.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_linux_virtual_machine.dagster.identity[0].principal_id
}

# Azure Container Registry
resource "azurerm_container_registry" "dagster" {
  name                = var.acr_name
  resource_group_name = azurerm_resource_group.dagster.name
  location            = azurerm_resource_group.dagster.location
  sku                 = "Basic"
  admin_enabled       = true
}

# Give VM permission to pull from ACR
resource "azurerm_role_assignment" "dagster_acr" {
  scope                = azurerm_container_registry.dagster.id
  role_definition_name = "AcrPull"
  principal_id         = azurerm_linux_virtual_machine.dagster.identity[0].principal_id
}