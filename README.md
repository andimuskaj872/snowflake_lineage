# Snowflake Lineage Explorer

Interactive Streamlit application for exploring Snowflake data lineage relationships and column-level access history using the `GET_LINEAGE` function and `ACCESS_HISTORY` views.

## Features

- 🔗 **Data Lineage Explorer**: Trace upstream/downstream dependencies using Snowflake's GET_LINEAGE function
- 📊 **Column-Level Access History**: Analyze when and how columns were last accessed (last 7 days)
- 🔄 **Cascading Dropdowns**: Smart database/schema/table/column selection with lazy loading
- 📥 **Multiple Export Options**: Download results as CSV or save directly to Snowflake tables
- 🏔️ **Optimized Queries**: Efficient ACCESS_HISTORY queries with proper clustering/pruning
- 🌐 **Easy Authentication**: Support for both .env files and native Snowflake config files

> **Note**: The custom query history feature is still a work in progress and may be added in future releases.

## Prerequisites

- Python 3.8+
- [uv](https://docs.astral.sh/uv/) (recommended) or pip
- Snowflake account with appropriate permissions
- Snowflake user credentials

## Quick Start

### Option 1: Using uv (Recommended - Fast & Modern)

1. **Install uv** (if not already installed)
   ```bash
   # macOS/Linux
   curl -LsSf https://astral.sh/uv/install.sh | sh
   
   # Windows
   powershell -c "irm https://astral.sh/uv/install.ps1 | iex"
   
   # Alternative: use pip
   pip install uv
   ```

2. **Clone and setup**
   ```bash
   git clone <your-repo-url>
   cd snowflake_lineage
   
   # Create virtual environment and install dependencies (one command!)
   uv sync
   
   # If you get TLS/SSL certificate errors, use:
   # uv sync --native-tls
   ```

3. **Configure Snowflake credentials** (Choose one method)
   
   **Option A: Snowflake Config File (Easiest)**
   ```bash
   # Edit the sample config file with your actual Snowflake config
   nano snowflake_config.toml
   # Replace the sample with your config from Snowflake profile
   ```
   
   **Option B: Environment Variables**
   ```bash
   cp .env.example .env
   # Edit .env with your credentials
   ```

4. **Run the app**
   ```bash
   uv run streamlit run app.py
   ```

### Option 2: Using uv without pyproject.toml

```bash
git clone <your-repo-url>
cd snowflake_lineage

# Install dependencies directly
uv add streamlit snowflake-connector-python pandas python-dotenv

# Configure and run
cp .env.example .env
# Edit .env with your credentials
uv run streamlit run app.py
```

### Option 3: Using pip (Traditional Method)

```bash
git clone <your-repo-url>
cd snowflake_lineage

# Create virtual environment
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Configure and run
cp .env.example .env
# Edit .env with your credentials
streamlit run app.py
```

## Why uv?

🚀 **uv is blazingly fast** - 10-100x faster than pip
🔒 **Reliable dependency resolution** - Better conflict resolution
🎯 **Modern Python tooling** - Built for today's Python ecosystem
📦 **All-in-one tool** - Replaces pip, pip-tools, virtualenv, and more

## Installation Troubleshooting

### Installing uv

#### macOS/Linux
```bash
# Using the installer script
curl -LsSf https://astral.sh/uv/install.sh | sh

# Using Homebrew
brew install uv

# Using pip (if you have Python already)
pip install uv
```

#### Windows
```bash
# Using PowerShell
powershell -c "irm https://astral.sh/uv/install.ps1 | iex"

# Using pip
pip install uv

# Using Chocolatey
choco install uv
```

### Common uv Issues

#### "uv: command not found"
**Solution**: Restart your terminal or add uv to your PATH:
```bash
# Add to your shell profile (~/.bashrc, ~/.zshrc, etc.)
export PATH="$HOME/.cargo/bin:$PATH"
```

#### Permission errors
**Solution**: uv handles virtual environments automatically, no sudo needed!

## Configuration

### Snowflake Account Identifier

Your account identifier can be found in several formats:
- **Preferred**: `orgname-accountname` (e.g., `myorg-myaccount`)
- **Legacy**: `account.region` (e.g., `xy12345.us-east-1`)

### Environment Variables

| Variable | Description | Example |
|----------|-------------|---------|
| `SNOWFLAKE_USER` | Your Snowflake username | `john.doe` |
| `SNOWFLAKE_PASSWORD` | Your Snowflake password | `mySecurePassword123` |
| `SNOWFLAKE_ACCOUNT` | Account identifier | `myorg-myaccount` |
| `SNOWFLAKE_WAREHOUSE` | Warehouse name | `COMPUTE_WH` |
| `SNOWFLAKE_DATABASE` | Database name | `MY_DATABASE` |
| `SNOWFLAKE_SCHEMA` | Schema name | `PUBLIC` |

## Usage

1. **Start the application**
   ```bash
   # Using uv (recommended)
   uv run streamlit run app.py
   
   # Using pip (if using traditional method)
   source venv/bin/activate
   streamlit run app.py
   ```

2. **Open your browser**
   The app will automatically open at `http://localhost:8501`
   
   If it doesn't open automatically, manually navigate to: `http://localhost:8501`

3. **Connect to Snowflake**
   - Click the "Connect to Snowflake" button
   - Wait for the green "✅ Connected to Snowflake successfully!" message
   - If connection fails, verify your `.env` file credentials

4. **Execute queries**
   - Enter your SQL query in the text area
   - Click "Execute Query" to run it
   - View results in the interactive table below
   - Download results as CSV if needed
   - Use "Clear Query" to start fresh

## Getting Your Snowflake Credentials (Super Easy!)

### ⭐ Method 1: Copy Config File (Recommended - Easiest!)

**Just copy and paste - no manual typing needed!**

1. **Log into Snowflake Web UI**
2. **Click your profile icon** (top right corner)
3. **Click "Account"** from the dropdown
4. **Click "View Account Details"**
5. **Copy the entire config section**

**You'll get something like this:**

**Option 1: SSO/Browser Auth (Recommended):**
```toml
[connections.my_example_connection]
account = "xy12345-ab67890"
user = "john.doe"
authenticator = "externalbrowser"
role = "ACCOUNTADMIN"
warehouse = "COMPUTE_WH"
database = "SALES_DB"
schema = "PUBLIC"
```

**Option 2: Password Auth (Traditional):**
```toml
[connections.my_example_connection]
account = "xy12345-ab67890"
user = "john.doe"
password = "YourPassword123!"
role = "ACCOUNTADMIN"
warehouse = "COMPUTE_WH"
database = "SALES_DB"
schema = "PUBLIC"
```

6. **Save it to the config file:**
   ```bash
   # Copy the example and choose your authentication method
   cp snowflake_config.toml.example snowflake_config.toml
   nano snowflake_config.toml  # or code snowflake_config.toml, vim, etc.
   
   # For SSO: Keep 'authenticator = "externalbrowser"', remove password
   # For Password: Remove authenticator line, add your password
   ```

✅ **Benefits of this method:**
- **Copy-paste setup** - No manual typing or translation needed
- **SSO/MFA Support** - Works with your organization's authentication
- **Easy switching** - Comment/uncomment lines to change auth methods
- **More secure** - Browser auth doesn't store passwords locally
- **Exact format** - Guaranteed to work with Snowflake's format
- **App detects automatically** - Prioritizes config file over .env

### Alternative: Manual Method (If Needed)

<details>
<summary>Click to expand manual credential finding</summary>

#### Finding Account Identifier Manually
1. **From URL**: `https://[account_identifier].snowflakecomputing.com`
2. **From profile**: Bottom left → Account tab → Copy "Account Identifier"
3. **From SQL**: Run `SELECT CURRENT_ACCOUNT();`

#### Finding Other Details
- **Warehouses**: Admin → Warehouses (common: `COMPUTE_WH`)
- **Databases**: Data → Databases  
- **Schemas**: Usually `PUBLIC`
- **Roles**: Run `SELECT CURRENT_ROLE();`

</details>

### Method 2: Environment Variables (.env file)

**If you prefer the traditional approach:**

```bash
# 1. Copy the example file
cp .env.example .env

# 2. Edit .env with your actual values (use any text editor)
nano .env  # or code .env, vim .env, etc.
```

#### Browser Authentication (Recommended)
**Using details from your Snowflake profile:**
```bash
# Use externalbrowser - no password needed!
SNOWFLAKE_USER=john.doe                     # From profile config
SNOWFLAKE_ACCOUNT=xy12345-ab67890           # From profile config  
SNOWFLAKE_AUTHENTICATOR=externalbrowser     # Enables browser login
SNOWFLAKE_ROLE=ACCOUNTADMIN                 # From profile config
SNOWFLAKE_WAREHOUSE=COMPUTE_WH              # Set your preferred warehouse
SNOWFLAKE_DATABASE=SALES_DB                 # Set your preferred database
SNOWFLAKE_SCHEMA=PUBLIC                     # Usually PUBLIC
```

#### Password Authentication (Traditional)
**If you prefer username/password:**
```bash  
SNOWFLAKE_USER=john.doe
SNOWFLAKE_PASSWORD=MySecurePassword123!
SNOWFLAKE_ACCOUNT=xy12345-ab67890
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_DATABASE=SALES_DB  
SNOWFLAKE_SCHEMA=PUBLIC
```

**💡 Configuration Priority:**
1. **snowflake_config.toml** (checked first)
2. **.env file** (fallback)
3. App will show which method it's using

### Test Your Connection

After setting up your `.env` file, test with these queries:

```sql
-- Basic connection test
SELECT 1;

-- Check your current context
SELECT CURRENT_USER(), CURRENT_ROLE(), CURRENT_DATABASE(), CURRENT_WAREHOUSE();

-- List available databases (if you have permissions)
SHOW DATABASES;

-- List tables in your database
SHOW TABLES IN SCHEMA your_database.your_schema;
```

### Common Setup Issues & Solutions

#### "I don't know my warehouse name"
**Solution**: Ask your Snowflake admin or try common names:
- `COMPUTE_WH` (most common default)
- `ANALYTICS_WH`
- `DEV_WH`
- Or run: `SHOW WAREHOUSES;` in Snowflake Web UI

#### "I don't have access to any databases"
**Solution**: Contact your Snowflake admin to grant you access:
- You need at least `USAGE` on database and schema
- And `SELECT` on tables you want to query

#### "My account identifier doesn't work"
**Try these formats**:
- `orgname-accountname` (new format)
- `account.region` (legacy format like `xy12345.us-east-1`)
- Just the account part without region (sometimes works)

## Example Queries

```sql
-- List all tables in current database
SHOW TABLES;

-- Get table information
SELECT * FROM INFORMATION_SCHEMA.TABLES LIMIT 10;

-- Sample data query
SELECT * FROM your_table_name LIMIT 100;

-- Aggregate query
SELECT 
    column_name,
    COUNT(*) as count
FROM your_table_name 
GROUP BY column_name 
ORDER BY count DESC;
```

## Security

- **Never commit your `.env` file** - it contains sensitive credentials
- The `.env` file is included in `.gitignore` to prevent accidental commits
- Use strong passwords and follow your organization's security policies
- Consider using Snowflake's key-pair authentication for enhanced security

## Troubleshooting

### Connection Issues

#### ❌ "Connection failed: 250001"
**Problem**: Invalid credentials or account identifier
**Solutions**:
- Double-check username and password in `.env` file
- Verify account identifier format (see "Getting Your Snowflake Credentials" section)
- Try logging into Snowflake web UI with same credentials

#### ❌ "Connection failed: 250003" 
**Problem**: Network connectivity issues
**Solutions**:
- Check your internet connection
- Verify you can access Snowflake web UI from your browser
- Check if your organization uses VPN or firewall restrictions

#### ❌ "Query execution failed"
**Problem**: SQL syntax error or insufficient permissions
**Solutions**:
- Check SQL syntax in your query
- Verify you have SELECT permissions on the tables/views
- Try simpler queries first (e.g., `SELECT 1;`)

#### ❌ "Warehouse not available"
**Problem**: Warehouse is suspended or doesn't exist
**Solutions**:
- Check if warehouse name is correct in `.env` file
- Ask your admin to resume the warehouse
- Try using `COMPUTE_WH` (default warehouse)

### Installation Issues

#### "uv sync fails" or dependency conflicts
**Solutions**:
```bash
# Clear uv cache and retry
uv cache clean
uv sync

# For TLS/SSL certificate errors (common on corporate networks)
uv sync --native-tls

# Or use specific Python version
uv sync --python 3.11
```

#### "pip install fails" or "externally-managed-environment" 
**Solution**: Switch to uv! It handles this automatically:
```bash
# Instead of fighting pip, use uv
uv sync
```

#### "command not found: python3"
**Solutions**:
- **macOS**: `brew install python3` or use uv: `uv python install 3.11`
- **Windows**: Download from [python.org](https://python.org) or `uv python install 3.11`
- **Linux**: `sudo apt-get install python3` or `uv python install 3.11`

#### "No module found" errors with uv
**Solution**: Use `uv run` instead of activating environments:
```bash
# Don't do this
source .venv/bin/activate
streamlit run app.py

# Do this instead
uv run streamlit run app.py
```

### Quick Diagnosis

Run this checklist if you're having issues:

1. **Test uv and Python**:
   ```bash
   uv --version         # Should show uv version
   uv python list       # Shows available Python versions
   uv tree              # Shows installed packages
   ```

2. **Test .env file**:
   ```bash
   cat .env  # Should show your credentials (be careful not to share!)
   ```

3. **Test basic connection**:
   - Start with `SELECT 1;` query
   - Then try `SELECT CURRENT_USER();`

4. **Test uv environment**:
   ```bash
   uv run python --version  # Should work without activation
   uv run python -c "import streamlit; print('OK')"
   ```

### Getting Help

- 📖 [Snowflake Python Connector Docs](https://docs.snowflake.com/en/user-guide/python-connector-api.html)
- 🔧 [Streamlit Documentation](https://docs.streamlit.io/)
- ⚡ [uv Documentation](https://docs.astral.sh/uv/)
- 💬 Open an issue in this repository with:
  - Your operating system
  - Python version (`uv run python --version`)
  - uv version (`uv --version`)
  - Error message (remove any sensitive info)
  - Steps you've already tried

## Dependencies

- `streamlit`: Web application framework
- `snowflake-connector-python`: Snowflake Python connector
- `pandas`: Data manipulation and analysis
- `python-dotenv`: Environment variable management

## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## License

This project is open source and available under the [MIT License](LICENSE).

## Support

If you encounter any issues or have questions, please:
1. Check the troubleshooting section above
2. Review Snowflake's documentation
3. Open an issue in this repository

---

**Happy querying!** 🚀