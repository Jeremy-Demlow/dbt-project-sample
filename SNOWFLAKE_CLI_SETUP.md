# Snowflake CLI Connection Setup Guide

## 📁 Configuration File Location

Your Snowflake CLI configuration should be placed at:
```
~/.snowflake/config.toml
```

## 🔧 Setup Steps

### 1. Create the Configuration Directory
```bash
mkdir -p ~/.snowflake
```

### 2. Create the Configuration File
Copy the example configuration from `snowflake_cli_config_example.toml` and customize it:

```bash
# Copy the example and edit it
cp snowflake_cli_config_example.toml ~/.snowflake/config.toml
vim ~/.snowflake/config.toml  # or use your preferred editor
```

### 3. Fill in Your Snowflake Details

You'll need to replace these placeholders:

- **`your_account_identifier`**: Your Snowflake account identifier
  - Format: `<account_name>.<region>.<cloud>` (e.g., `abc12345.us-east-1.aws`)
  - Or just `<account_name>` if using default region
  - Find this in your Snowflake URL: `https://your_account_identifier.snowflakecomputing.com`

- **`your_username`**: Your Snowflake username

- **Authentication**: Choose one method:
  - **Password**: Simple but less secure
  - **Private Key**: Recommended for automation
  - **SSO**: For organizations using single sign-on

## 🔐 Authentication Options

### Option 1: Password Authentication (Simplest)
```toml
[connections.default]
account = "abc12345.us-east-1.aws"
user = "john_doe"
password = "your_secure_password"
database = "DBT_CORTEX_LLMS"
schema = "ANALYTICS"
warehouse = "CORTEX_WH"
role = "DBT_ROLE"
```

### Option 2: Private Key Authentication (Recommended)
First, generate a key pair:
```bash
# Generate private key
openssl genrsa -out ~/.ssh/snowflake_rsa_key 2048

# Generate public key
openssl rsa -in ~/.ssh/snowflake_rsa_key -pubout -out ~/.ssh/snowflake_rsa_key.pub

# Convert to PKCS#8 format (required by Snowflake CLI)
openssl pkcs8 -topk8 -inform PEM -outform PEM -nocrypt \
    -in ~/.ssh/snowflake_rsa_key -out ~/.ssh/snowflake_rsa_key.p8
```

Then configure:
```toml
[connections.default]
account = "abc12345.us-east-1.aws"
user = "john_doe"
private_key_path = "~/.ssh/snowflake_rsa_key.p8"
database = "DBT_CORTEX_LLMS"
schema = "ANALYTICS"
warehouse = "CORTEX_WH"
role = "DBT_ROLE"
```

**Important**: You must add the public key to your Snowflake user:
```sql
-- In Snowflake, run this command (replace with your actual public key content)
ALTER USER john_doe SET RSA_PUBLIC_KEY='FAKEKEY...';
```

### Option 3: SSO Authentication
```toml
[connections.default]
account = "abc12345.us-east-1.aws"
user = "john_doe"
authenticator = "externalbrowser"
database = "DBT_CORTEX_LLMS"
schema = "ANALYTICS"
warehouse = "CORTEX_WH"
role = "DBT_ROLE"
```

## ✅ Test Your Connection

After setting up your configuration:

```bash
# Test the connection
snow connection test --connection default

# List all configured connections
snow connection list

# Test a SQL query
snow sql -q "SELECT CURRENT_USER(), CURRENT_ROLE(), CURRENT_DATABASE();" --connection default
```

## 🎯 Your Project-Specific Configuration

Based on your dbt project, your `default` connection should have:

```toml
[connections.default]
account = "your_account_identifier"
user = "your_username"
password = "your_password"  # or use private_key_path
database = "DBT_CORTEX_LLMS"
schema = "ANALYTICS"
warehouse = "CORTEX_WH"
role = "DBT_ROLE"
```

## 🔍 Finding Your Account Identifier

1. **From Snowflake URL**: Look at your Snowflake web interface URL
   - `https://abc12345.snowflakecomputing.com` → account is `abc12345`
   - `https://abc12345.us-east-1.aws.snowflakecomputing.com` → account is `abc12345.us-east-1.aws`

2. **From Snowflake SQL**: Run this query in Snowflake:
   ```sql
   SELECT CURRENT_ACCOUNT();
   ```

3. **From SHOW PARAMETERS**: 
   ```sql
   SHOW PARAMETERS LIKE 'ACCOUNT_LOCATOR';
   ```


## 🔧 Troubleshooting

### Common Issues:
- **"Connection failed"**: Check account identifier format
- **"Invalid user"**: Verify username and role permissions  
- **"Authentication failed"**: Check password or private key setup
- **"Database not found"**: Ensure `DBT_CORTEX_LLMS` database exists and you have access

### Debug Commands:
```bash
# Verbose connection test
snow connection test --connection default --verbose

# Check CLI version
snow --version

# List available commands
snow --help
``` 