import streamlit as st
import snowflake.connector
import pandas as pd
import os
import configparser
from dotenv import load_dotenv

load_dotenv()

st.set_page_config(page_title="Snowflake Lineage Explorer", layout="wide")

def load_snowflake_config():
    """Load Snowflake configuration from config file or environment variables"""
    connection_params = {}
    
    # Try to load from Snowflake config file first
    config_file = 'snowflake_config.toml'
    if os.path.exists(config_file):
        try:
            config = configparser.ConfigParser()
            config.read(config_file)
            
            # Look for the first connection section
            for section_name in config.sections():
                if section_name.startswith('connections.'):
                    section = config[section_name]
                    
                    # Map config file keys to connection parameters
                    if 'user' in section:
                        connection_params['user'] = section['user'].strip('"')
                    if 'account' in section:
                        connection_params['account'] = section['account'].strip('"')
                    if 'authenticator' in section:
                        auth_value = section['authenticator'].strip('"')
                        if auth_value:  # Only add if not empty
                            connection_params['authenticator'] = auth_value
                    if 'role' in section:
                        role = section['role'].strip('"')
                        if role != "<none selected>":
                            connection_params['role'] = role
                    if 'warehouse' in section:
                        warehouse = section['warehouse'].strip('"')
                        if warehouse != "<none selected>":
                            connection_params['warehouse'] = warehouse
                    if 'database' in section:
                        database = section['database'].strip('"')
                        if database != "<none selected>":
                            connection_params['database'] = database
                    if 'schema' in section:
                        schema = section['schema'].strip('"')
                        if schema != "<none selected>":
                            connection_params['schema'] = schema
                    if 'password' in section:
                        connection_params['password'] = section['password'].strip('"')
                    
                    break  # Use the first connection found
                    
            if connection_params:
                st.info("📁 Using configuration from snowflake_config.toml")
                return connection_params
                
        except Exception as e:
            st.warning(f"Could not parse config file: {str(e)}")
    
    # Fallback to environment variables
    connection_params = {
        'user': os.getenv('SNOWFLAKE_USER'),
        'account': os.getenv('SNOWFLAKE_ACCOUNT'),
    }
    
    # Add optional parameters if they exist
    if os.getenv('SNOWFLAKE_PASSWORD'):
        connection_params['password'] = os.getenv('SNOWFLAKE_PASSWORD')
    
    if os.getenv('SNOWFLAKE_AUTHENTICATOR'):
        connection_params['authenticator'] = os.getenv('SNOWFLAKE_AUTHENTICATOR')
    
    if os.getenv('SNOWFLAKE_ROLE'):
        connection_params['role'] = os.getenv('SNOWFLAKE_ROLE')
        
    if os.getenv('SNOWFLAKE_WAREHOUSE'):
        connection_params['warehouse'] = os.getenv('SNOWFLAKE_WAREHOUSE')
        
    if os.getenv('SNOWFLAKE_DATABASE'):
        connection_params['database'] = os.getenv('SNOWFLAKE_DATABASE')
        
    if os.getenv('SNOWFLAKE_SCHEMA'):
        connection_params['schema'] = os.getenv('SNOWFLAKE_SCHEMA')
    
    if connection_params.get('user') and connection_params.get('account'):
        st.info("🔧 Using configuration from .env file")
    
    return connection_params

def create_connection():
    """Create connection to Snowflake"""
    try:
        connection_params = load_snowflake_config()
        
        # Validate required parameters
        if not connection_params.get('user') or not connection_params.get('account'):
            st.error("Missing required connection parameters. Please check your configuration.")
            return None
        
        # Remove any None or empty values
        connection_params = {k: v for k, v in connection_params.items() if v}
        
        
        # Add SSL configuration to handle certificate issues
        ssl_options = {
            'insecure_mode': False,  # Keep secure by default
            'ocsp_fail_open': True,  # Allow connection if OCSP check fails
            'disable_request_pooling': False,
        }
        
        # For SSL certificate issues, add fallback options
        try:
            # Try with standard SSL settings first
            conn = snowflake.connector.connect(**connection_params, **ssl_options)
            return conn
        except Exception as ssl_error:
            if "certificate verify failed" in str(ssl_error) or "SSL" in str(ssl_error):
                st.warning("⚠️ SSL certificate verification failed. Attempting connection with relaxed SSL settings...")
                
                # Fallback: Try with insecure mode for certificate issues
                ssl_options['insecure_mode'] = True
                try:
                    conn = snowflake.connector.connect(**connection_params, **ssl_options)
                    st.info("✅ Connected using relaxed SSL settings. Consider updating your system certificates for better security.")
                    return conn
                except Exception as fallback_error:
                    st.error(f"Connection failed even with relaxed SSL settings: {str(fallback_error)}")
                    raise fallback_error
            else:
                # Re-raise non-SSL errors
                raise ssl_error
    except Exception as e:
        st.error(f"Connection failed: {str(e)}")
        return None

def execute_lineage_query(conn, object_name, object_type, direction, depth):
    """Execute GET_LINEAGE query and return results"""
    try:
        query = f"""
        SELECT
            *
        FROM TABLE (SNOWFLAKE.CORE.GET_LINEAGE('{object_name}', '{object_type}', '{direction}', {depth}))
        """
        
        cursor = conn.cursor()
        cursor.execute(query)
        results = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        df = pd.DataFrame(results, columns=columns)
        return df, query
    except Exception as e:
        st.error(f"Lineage query execution failed: {str(e)}")
        return None, None

def fetch_databases(conn):
    """Fetch list of databases"""
    try:
        cursor = conn.cursor()
        cursor.execute("SHOW DATABASES")
        results = cursor.fetchall()
        # Extract database names from the results (usually in the 'name' column)
        databases = [row[1] for row in results]  # name is typically the second column
        return sorted(databases)
    except Exception as e:
        st.error(f"Failed to fetch databases: {str(e)}")
        return []

def fetch_schemas(conn, database, include_system_schemas=False):
    """Fetch list of schemas for a given database"""
    try:
        cursor = conn.cursor()
        cursor.execute(f"SHOW SCHEMAS IN DATABASE {database}")
        results = cursor.fetchall()
        # Extract schema names from the results
        all_schemas = [row[1] for row in results]  # name is typically the second column
        
        if include_system_schemas:
            return sorted(all_schemas)
        
        # Filter out common system/internal schemas that users typically don't need
        system_schemas = {
            'INFORMATION_SCHEMA', 'ACCOUNT_USAGE', 'READER_ACCOUNT_USAGE',
            'DATA_SHARING_USAGE', 'ORGANIZATION_USAGE', 'SNOWFLAKE',
            'SNOWFLAKE_SAMPLE_DATA'
        }
        
        # Keep user schemas and common schemas like PUBLIC, RAW_*, etc.
        user_schemas = []
        for schema in all_schemas:
            if (schema not in system_schemas and 
                not schema.startswith('SNOWFLAKE_') and
                not schema.startswith('__')):
                user_schemas.append(schema)
        
        return sorted(user_schemas)
    except Exception as e:
        st.error(f"Failed to fetch schemas for database {database}: {str(e)}")
        return []

def fetch_tables(conn, database, schema):
    """Fetch list of tables for a given database and schema"""
    try:
        cursor = conn.cursor()
        cursor.execute(f"SHOW TABLES IN SCHEMA {database}.{schema}")
        results = cursor.fetchall()
        # Extract table names from the results
        tables = [row[1] for row in results]  # name is typically the second column
        return sorted(tables)
    except Exception as e:
        st.error(f"Failed to fetch tables for {database}.{schema}: {str(e)}")
        return []

def fetch_columns(conn, database, schema, table):
    """Fetch list of columns for a given table"""
    try:
        cursor = conn.cursor()
        cursor.execute(f"SHOW COLUMNS IN TABLE {database}.{schema}.{table}")
        results = cursor.fetchall()
        # Extract column names from the results
        columns = [row[2] for row in results]  # column_name is typically the third column
        return sorted(columns)
    except Exception as e:
        st.error(f"Failed to fetch columns for {database}.{schema}.{table}: {str(e)}")
        return []

def save_results_to_snowflake(conn, df, database, schema, table_name):
    """Save DataFrame results to a Snowflake table"""
    try:
        cursor = conn.cursor()
        
        # Create the full table name
        full_table_name = f"{database}.{schema}.{table_name}"
        
        # Get column names and create table structure
        columns_def = []
        for col in df.columns:
            # Use appropriate data types based on content
            columns_def.append(f'"{col}" VARCHAR(16777216)')
        
        # Create table with proper structure
        create_sql = f"""
        CREATE OR REPLACE TABLE {full_table_name} (
            {', '.join(columns_def)},
            CREATED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
            CREATED_BY VARCHAR DEFAULT CURRENT_USER()
        )
        """
        cursor.execute(create_sql)
        
        # Prepare data for bulk insert
        if not df.empty:
            # Create a temporary staging table or use direct insert
            insert_values = []
            for _, row in df.iterrows():
                # Escape single quotes and handle None values
                escaped_values = []
                for val in row:
                    if pd.isna(val) or val is None:
                        escaped_values.append("NULL")
                    else:
                        # Convert to string and escape quotes
                        str_val = str(val).replace("'", "''")
                        escaped_values.append(f"'{str_val}'")
                
                insert_values.append(f"({', '.join(escaped_values)}, CURRENT_TIMESTAMP(), CURRENT_USER())")
            
            # Batch insert (split into chunks if too large)
            chunk_size = 1000
            total_rows = len(insert_values)
            
            for i in range(0, total_rows, chunk_size):
                chunk = insert_values[i:i + chunk_size]
                values_clause = ', '.join(chunk)
                
                insert_sql = f"""
                INSERT INTO {full_table_name} 
                ({', '.join([f'"{col}"' for col in df.columns])}, CREATED_AT, CREATED_BY)
                VALUES {values_clause}
                """
                cursor.execute(insert_sql)
        
        return True, len(df)
    except Exception as e:
        return False, str(e)

def execute_access_history_query(conn, lineage_df):
    """Execute optimized access history query for all objects in lineage results"""
    try:
        if lineage_df is None or lineage_df.empty:
            return None, None
        
        # Extract unique object names from lineage results
        object_names = set()
        
        # Get objects from different columns in lineage results
        for col in ['OBJECT_NAME', 'SOURCE_OBJECT_NAME', 'TARGET_OBJECT_NAME']:
            if col in lineage_df.columns:
                object_names.update(lineage_df[col].dropna().unique())
        
        if not object_names:
            return None, "No objects found in lineage results"
        
        # Create optimized query with proper pruning
        # Use date range filter first for clustering optimization
        object_list = "', '".join(object_names)
        
        query = f"""
        WITH flattened_access AS (
            SELECT 
                query_start_time,
                user_name,
                obj.value:objectName::string AS object_name,
                obj.value:objectDomain::string AS object_domain,
                col.value:columnName::string AS column_name,
                col.value:columnId::number AS column_id
            FROM SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY,
                 LATERAL FLATTEN(input => direct_objects_accessed) obj,
                 LATERAL FLATTEN(input => obj.value:columns, OUTER => TRUE) col
            WHERE query_start_time >= DATEADD(day, -7, CURRENT_TIMESTAMP())
              AND obj.value:objectName::string IN ('{object_list}')
              AND obj.value:objectDomain::string = 'Table'
            
            UNION ALL
            
            SELECT 
                query_start_time,
                user_name,
                obj.value:objectName::string AS object_name,
                obj.value:objectDomain::string AS object_domain,
                col.value:columnName::string AS column_name,
                col.value:columnId::number AS column_id
            FROM SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY,
                 LATERAL FLATTEN(input => base_objects_accessed) obj,
                 LATERAL FLATTEN(input => obj.value:columns, OUTER => TRUE) col
            WHERE query_start_time >= DATEADD(day, -7, CURRENT_TIMESTAMP())
              AND obj.value:objectName::string IN ('{object_list}')
              AND obj.value:objectDomain::string = 'Table'
        ),
        column_access_summary AS (
            SELECT 
                object_name,
                column_name,
                MAX(query_start_time) AS last_accessed,
                COUNT(DISTINCT user_name) AS unique_users,
                COUNT(*) AS access_count
            FROM flattened_access
            WHERE column_name IS NOT NULL
            GROUP BY object_name, column_name
        ),
        table_access_summary AS (
            SELECT 
                object_name,
                'TABLE_LEVEL' AS column_name,
                MAX(query_start_time) AS last_accessed,
                COUNT(DISTINCT user_name) AS unique_users,
                COUNT(*) AS access_count
            FROM flattened_access
            GROUP BY object_name
        )
        SELECT 
            object_name,
            column_name,
            last_accessed::date AS last_accessed_date,
            last_accessed,
            unique_users,
            access_count
        FROM column_access_summary
        
        UNION ALL
        
        SELECT 
            object_name,
            column_name,
            last_accessed::date AS last_accessed_date,
            last_accessed,
            unique_users,
            access_count
        FROM table_access_summary
        
        ORDER BY object_name, column_name
        """
        
        cursor = conn.cursor()
        cursor.execute(query)
        results = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        df = pd.DataFrame(results, columns=columns)
        return df, query
    except Exception as e:
        st.error(f"Access history query execution failed: {str(e)}")
        return None, None

def execute_query(conn, query):
    """Execute SQL query and return results"""
    try:
        cursor = conn.cursor()
        cursor.execute(query)
        results = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        df = pd.DataFrame(results, columns=columns)
        return df
    except Exception as e:
        st.error(f"Query execution failed: {str(e)}")
        return None

def main():
    st.title("🔗 Snowflake Lineage Explorer")
    st.markdown("Explore data lineage relationships in your Snowflake environment using the `GET_LINEAGE` function")
    
    # Connection status
    if 'connection' not in st.session_state:
        st.session_state.connection = None
    
    # Connection section
    st.header("Connection")
    
    # Show configuration status
    config_params = load_snowflake_config()
    if config_params.get('user') and config_params.get('account'):
        st.success(f"✅ Configuration loaded for user: **{config_params.get('user')}**")
        if config_params.get('authenticator') == 'externalbrowser':
            st.info("🌐 Browser authentication configured - no password needed!")
    else:
        st.warning("⚠️ No valid configuration found. Please set up your credentials.")
        st.info("💡 Create a `snowflake_config.toml` file or configure `.env` file (see instructions below)")
    
    if st.button("Connect to Snowflake"):
        with st.spinner("Connecting..."):
            # Check if using external browser authentication
            if config_params.get('authenticator') == 'externalbrowser':
                st.info("🌐 Browser authentication detected. A browser window will open for login...")
            
            st.session_state.connection = create_connection()
            if st.session_state.connection:
                st.success("✅ Connected to Snowflake successfully!")
                
                # Show connection details
                try:
                    cursor = st.session_state.connection.cursor()
                    cursor.execute("SELECT CURRENT_USER(), CURRENT_ROLE(), CURRENT_WAREHOUSE(), CURRENT_DATABASE()")
                    result = cursor.fetchone()
                    if result:
                        st.info(f"**Connected as:** {result[0]} | **Role:** {result[1]} | **Warehouse:** {result[2]} | **Database:** {result[3]}")
                except:
                    pass  # Don't fail if we can't get connection details
            else:
                st.error("❌ Failed to connect to Snowflake")
                if config_params.get('authenticator') == 'externalbrowser':
                    st.info("💡 **Tip:** Make sure you completed the browser login and didn't close the authentication window.")
    
    # Lineage Explorer section
    if st.session_state.connection:
        st.header("🔍 Lineage Explorer")
        
        # Initialize session state for dropdowns
        if 'databases' not in st.session_state:
            st.session_state.databases = []
        
        # Load databases only when first connected (lazy loading)
        if not st.session_state.databases:
            with st.spinner("Loading databases..."):
                st.session_state.databases = fetch_databases(st.session_state.connection)
        
        # Show connection status
        if st.session_state.databases:
            st.success(f"📊 Connected to Snowflake • {len(st.session_state.databases)} databases available")
        
        # Object selection section outside form for dynamic updates
        st.subheader("GET_LINEAGE Parameters")
        st.markdown("**📍 Object Selection**")
        
        # Database dropdown
        database_options = [""] + st.session_state.databases
        
        database = st.selectbox(
            "Database *",
            options=database_options,
            index=0,  # Always start with empty selection
            help="Select a Snowflake database (required)"
        )
        
        # Schema dropdown (only populate if database is selected)
        if database:
            # Check if we need to load schemas for the selected database
            schemas_key = f'schemas_{database}'
            if schemas_key not in st.session_state:
                with st.spinner(f"Loading schemas for {database}..."):
                    st.session_state[schemas_key] = fetch_schemas(st.session_state.connection, database)
            
            available_schemas = st.session_state[schemas_key]
            schema_options = [""] + available_schemas
            
            schema = st.selectbox(
                "Schema *",
                options=schema_options,
                index=0,  # Always start with empty selection
                help="Select a schema within the database (required)"
            )
        else:
            schema = st.selectbox(
                "Schema *",
                options=[""],
                disabled=True,
                help="Select a database first (required)"
            )
        
        # Table dropdown (only populate if database and schema are selected)
        if database and schema:
            # Check if we need to load tables for the selected database.schema
            tables_key = f'tables_{database}_{schema}'
            if tables_key not in st.session_state:
                with st.spinner(f"Loading tables for {database}.{schema}..."):
                    st.session_state[tables_key] = fetch_tables(st.session_state.connection, database, schema)
            
            available_tables = st.session_state[tables_key]
            table_options = [""] + available_tables
            
            table = st.selectbox(
                "Table *",
                options=table_options,
                index=0,  # Always start with empty selection
                help="Select a table within the schema (required)"
            )
        else:
            table = st.selectbox(
                "Table *",
                options=[""],
                disabled=True,
                help="Select database and schema first (required)"
            )
        
        # Column dropdown (always show, but optional)
        if database and schema and table:
            # Check if we need to load columns for the selected table
            columns_key = f'columns_{database}_{schema}_{table}'
            if columns_key not in st.session_state:
                with st.spinner(f"Loading columns for {database}.{schema}.{table}..."):
                    st.session_state[columns_key] = fetch_columns(st.session_state.connection, database, schema, table)
            
            available_columns = st.session_state[columns_key]
            column_options = [""] + available_columns
            
            column = st.selectbox(
                "Column",
                options=column_options,
                index=0,  # Always start with empty selection
                help="Select a column for column-level lineage (optional - leave blank for table-level lineage)"
            )
        else:
            column = st.selectbox(
                "Column",
                options=[""],
                disabled=True,
                help="Select database, schema, and table first (optional)"
            )
        
        # Lineage parameters section
        st.markdown("**🔍 Lineage Parameters**")
        col3, col4 = st.columns(2)
        
        with col3:
            direction = st.selectbox(
                "Direction *",
                options=["DOWNSTREAM", "UPSTREAM", "BOTH"],
                index=0,
                help="Direction of lineage to explore (required)"
            )
        
        with col4:
            depth_option = st.selectbox(
                "Depth *",
                options=["Until End", "Custom"],
                index=0,
                help="Choose depth strategy: 'Until End' to traverse all the way to the end, 'Custom' for specific number of levels"
            )
        
        # Conditional depth input - only show for Custom (now outside form so it works)
        if depth_option == "Custom":
            depth = st.number_input(
                "Number of Levels",
                min_value=1,
                max_value=50,
                value=4,
                help="Number of levels to traverse"
            )
        else:  # Until End
            depth = 999  # Use a very large number to represent "until end"
            st.info("📡 Will traverse lineage until it reaches the end")
        
        # Additional analysis options
        st.markdown("**📊 Additional Analysis**")
        include_access_history = st.checkbox(
            "Include Access History (Last 7 Days)",
            help="Analyze access history to see when and how this object/column was last accessed (requires ACCOUNTADMIN role or access to ACCOUNT_USAGE)"
        )
        
        # Submit button
        submitted = st.button("🔍 Explore Lineage", type="primary")
        
        # Execute lineage query when form is submitted
        if submitted:
            # Check if required fields are selected
            if not (database and schema and table):
                st.error("Please select all required fields: Database *, Schema *, and Table *")
                object_name = None
                object_type = None
            else:
                # Determine object type and construct object name based on column selection
                if column:
                    # Column-level lineage
                    object_name = f"{database}.{schema}.{table}.{column}"
                    object_type = "column"
                else:
                    # Table-level lineage
                    object_name = f"{database}.{schema}.{table}"
                    object_type = "table"
            
            if object_name:
                # Show the constructed object name and depth strategy
                depth_display = "until end" if depth == 999 else f"{depth} levels"
                st.info(f"🎯 **Analyzing object:** `{object_name}` (type: {object_type}) • **Depth:** {depth_display}")
                
                with st.spinner("Exploring lineage..."):
                    df, query = execute_lineage_query(
                        st.session_state.connection, 
                        object_name, 
                        object_type, 
                        direction, 
                        depth
                    )
                    
                    # Also get access history if requested
                    access_df, access_query = None, None
                    if include_access_history and df is not None:
                        with st.spinner("Analyzing access history for all lineage objects..."):
                            access_df, access_query = execute_access_history_query(
                                st.session_state.connection,
                                df
                            )
                    
                    # Store results in session state to prevent loss on rerun
                    if df is not None:
                        st.session_state.lineage_results = {
                            'df': df,
                            'query': query,
                            'object_name': object_name,
                            'object_type': object_type,
                            'direction': direction,
                            'depth_display': depth_display,
                            'access_df': access_df,
                            'access_query': access_query,
                            'include_access_history': include_access_history
                        }
        
        # Display results (either from current query or from session state)
        results_data = st.session_state.get('lineage_results')
        if results_data:
            df = results_data['df']
            query = results_data['query']
            object_name = results_data['object_name']
            object_type = results_data['object_type']
            direction = results_data['direction']
            depth_display = results_data['depth_display']
            access_df = results_data.get('access_df')
            access_query = results_data.get('access_query')
            include_access_history = results_data.get('include_access_history', False)
            
            
            st.header("📊 Analysis Results")
            
            # Show the executed queries
            with st.expander("🔍 View Generated Queries"):
                st.markdown("**Lineage Query:**")
                st.code(query, language="sql")
                if access_query:
                    st.markdown("**Access History Query:**")
                    st.code(access_query, language="sql")
            
            # Create tabs for different result types
            if include_access_history and access_df is not None:
                tab1, tab2 = st.tabs(["🔗 Lineage Results", "📈 Access History (Last 7 Days)"])
            else:
                tab1 = st.container()
                tab2 = None
            
            with tab1:
                st.subheader("🔗 Lineage Relationships")
                st.write(f"**Lineage relationships found:** {len(df)}")
                
                if not df.empty:
                    # Display results in a nice format
                    st.dataframe(df, use_container_width=True)
                
                # Key insights
                if len(df) > 0:
                    st.subheader("📈 Key Insights")
                    
                    col1, col2, col3 = st.columns(3)
                    
                    with col1:
                        st.metric("Total Objects", len(df))
                    
                    with col2:
                        if 'OBJECT_TYPE' in df.columns:
                            unique_types = df['OBJECT_TYPE'].nunique()
                            st.metric("Object Types", unique_types)
                    
                    with col3:
                        if 'OBJECT_DOMAIN' in df.columns:
                            unique_domains = df['OBJECT_DOMAIN'].nunique()
                            st.metric("Domains", unique_domains)
                    
                    # Show object type breakdown
                    if 'OBJECT_TYPE' in df.columns:
                        st.subheader("Object Type Distribution")
                        type_counts = df['OBJECT_TYPE'].value_counts()
                        st.bar_chart(type_counts)
                
                # Export options
                st.subheader("📤 Export Results")
                col_export1, col_export2 = st.columns(2)
                
                with col_export1:
                    # Download CSV button
                    csv = df.to_csv(index=False)
                    st.download_button(
                        label="📥 Download as CSV",
                        data=csv,
                        file_name=f"lineage_{object_name.replace('.', '_')}_{direction.lower()}.csv",
                        mime="text/csv"
                    )
                
                with col_export2:
                    # Save to Snowflake button
                    if st.button("🏔️ Save to Snowflake Table", help="Create or replace a table in Snowflake with these results"):
                        st.session_state.show_snowflake_save = True
                        st.rerun()
                
                # Snowflake save options (show if button clicked)
                if st.session_state.get('show_snowflake_save', False):
                    st.markdown("---")
                    st.subheader("🏔️ Save to Snowflake Table")
                    
                    # Warning
                    st.warning("⚠️ **Warning**: This will CREATE OR REPLACE the specified table. Any existing table with the same name will be completely overwritten!")
                    
                    
                    # Database and Schema selection outside form for dynamic updates
                    col_save1, col_save2, col_save3 = st.columns(3)
                    
                    with col_save1:
                        save_database = st.selectbox(
                            "Target Database",
                            options=st.session_state.databases,
                            help="Database where the table will be created"
                        )
                    
                    with col_save2:
                        # Load schemas for selected database (dynamic filtering)
                        if save_database:
                            # Check if we need to reload schemas for the selected database
                            current_save_db_key = f'save_schemas_{save_database}'
                            
                            # Clear cached schemas if database selection changed
                            if 'current_save_database' not in st.session_state:
                                st.session_state.current_save_database = None
                            
                            if st.session_state.current_save_database != save_database:
                                # Database changed, clear old schema cache and update
                                st.session_state.current_save_database = save_database
                                # Clear any old cached schemas
                                keys_to_remove = [key for key in st.session_state.keys() if key.startswith('save_schemas_')]
                                for key in keys_to_remove:
                                    del st.session_state[key]
                            
                            # Create cache key for the selected database
                            current_save_db_key = f'save_schemas_{save_database}'
                            
                            if current_save_db_key not in st.session_state:
                                # Fetch schemas for this database and cache them
                                with st.spinner(f"Loading schemas for {save_database}..."):
                                    st.session_state[current_save_db_key] = fetch_schemas(st.session_state.connection, save_database, include_system_schemas=False)
                            
                            available_schemas = st.session_state.get(current_save_db_key, [])
                            schema_options = [""] + available_schemas
                            
                            save_schema = st.selectbox(
                                "Target Schema",
                                options=schema_options,
                                help="Schema where the table will be created"
                            )
                        else:
                            save_schema = st.selectbox(
                                "Target Schema",
                                options=[""],
                                disabled=True,
                                help="Select a database first"
                            )
                    
                    with col_save3:
                        default_table_name = f"LINEAGE_RESULTS_{object_name.replace('.', '_').upper()}_{direction}"
                        save_table_name = st.text_input(
                            "Table Name",
                            value=default_table_name,
                            help="Name of the table to create (will be created or replaced)"
                        )
                    
                    # Form for the action buttons only
                    with st.form("save_to_snowflake_form"):
                        # Action buttons
                        col_action1, col_action2 = st.columns(2)
                        with col_action1:
                            save_submitted = st.form_submit_button("💾 Create Table", type="primary")
                        with col_action2:
                            if st.form_submit_button("❌ Cancel"):
                                st.session_state.show_snowflake_save = False
                                st.rerun()
                    
                    # Execute save operation
                    if save_submitted:
                        if save_database and save_schema and save_table_name:
                            with st.spinner(f"Creating table {save_database}.{save_schema}.{save_table_name}..."):
                                success, result = save_results_to_snowflake(
                                    st.session_state.connection,
                                    df,
                                    save_database,
                                    save_schema,
                                    save_table_name
                                )
                            
                            if success:
                                full_table_name = f"{save_database}.{save_schema}.{save_table_name}"
                                st.success(f"✅ Successfully created table `{full_table_name}` with {result} rows!")
                                
                                # Show the SELECT query for easy copy-paste
                                select_query = f"SELECT * FROM {full_table_name};"
                                st.code(select_query, language="sql")
                                st.info("📋 **Copy the query above to use in Snowflake or any SQL client**")
                                
                                # Hide the save form but don't rerun immediately
                                st.session_state.show_snowflake_save = False
                            else:
                                st.error(f"❌ Failed to create table: {result}")
                        else:
                            st.error("Please fill in all required fields: Database, Schema, and Table Name")
                else:
                    st.info("No lineage relationships found for the specified object.")
            
            # Access History Tab
            if tab2 is not None:
                with tab2:
                    st.subheader("📈 Column Access History (Last 7 Days)")
                    
                    if access_df is not None and not access_df.empty:
                        st.write(f"**Objects/Columns with access data:** {len(access_df)}")
                        
                        # Display access history summary
                        st.dataframe(access_df, use_container_width=True)
                        
                        # Access insights
                        if len(access_df) > 0:
                            st.subheader("📊 Access Summary")
                            
                            col1, col2, col3 = st.columns(3)
                            
                            with col1:
                                unique_objects = access_df['OBJECT_NAME'].nunique()
                                st.metric("Objects Accessed", unique_objects)
                            
                            with col2:
                                column_access_count = len(access_df[access_df['COLUMN_NAME'] != 'TABLE_LEVEL'])
                                st.metric("Columns Accessed", column_access_count)
                            
                            with col3:
                                if 'LAST_ACCESSED' in access_df.columns:
                                    latest_access = access_df['LAST_ACCESSED'].max()
                                    st.metric("Most Recent Access", latest_access.strftime('%Y-%m-%d %H:%M') if latest_access else 'N/A')
                            
                            # Show objects by access recency
                            st.subheader("Objects by Access Recency")
                            object_latest = access_df.groupby('OBJECT_NAME')['LAST_ACCESSED'].max().sort_values(ascending=False)
                            st.bar_chart(object_latest.head(10))
                            
                            # Show column access summary
                            column_data = access_df[access_df['COLUMN_NAME'] != 'TABLE_LEVEL']
                            if not column_data.empty:
                                st.subheader("Column Access Summary")
                                
                                # Most accessed columns
                                top_columns = column_data.nlargest(10, 'ACCESS_COUNT')[['OBJECT_NAME', 'COLUMN_NAME', 'ACCESS_COUNT', 'LAST_ACCESSED_DATE']]
                                st.write("**Most Accessed Columns:**")
                                st.dataframe(top_columns, use_container_width=True)
                                
                                # Recently accessed columns
                                recent_columns = column_data.nlargest(10, 'LAST_ACCESSED')[['OBJECT_NAME', 'COLUMN_NAME', 'LAST_ACCESSED_DATE', 'ACCESS_COUNT']]
                                st.write("**Recently Accessed Columns:**")
                                st.dataframe(recent_columns, use_container_width=True)
                        
                        # Export access history
                        st.subheader("📤 Export Access History")
                        access_csv = access_df.to_csv(index=False)
                        st.download_button(
                            label="📥 Download Column Access History as CSV",
                            data=access_csv,
                            file_name=f"column_access_history_{object_name.replace('.', '_')}_7days.csv",
                            mime="text/csv"
                        )
                    
                    elif include_access_history:
                        st.info("No access history found for objects in the lineage in the last 7 days.")
                        st.markdown("""
                        **Note:** Column access history analysis requires:
                        - ACCOUNTADMIN role or access to SNOWFLAKE.ACCOUNT_USAGE views
                        - Objects must have been accessed within the last 7 days
                        - Optimized query using proper pruning on ACCESS_HISTORY clustered columns
                        """)
                        
                        # Show what objects were analyzed
                        if df is not None and not df.empty:
                            analyzed_objects = set()
                            for col in ['OBJECT_NAME', 'SOURCE_OBJECT_NAME', 'TARGET_OBJECT_NAME']:
                                if col in df.columns:
                                    analyzed_objects.update(df[col].dropna().unique())
                            
                            if analyzed_objects:
                                st.write(f"**Objects analyzed:** {len(analyzed_objects)}")
                                st.code("\n".join(sorted(analyzed_objects)))
            else:
                st.info("No lineage relationships found for the specified object.")
        
        # Add a button to clear results
        if st.session_state.get('lineage_results'):
            if st.button("🗑️ Clear Results", help="Clear current results and start a new analysis"):
                del st.session_state.lineage_results
                if 'show_snowflake_save' in st.session_state:
                    del st.session_state.show_snowflake_save
                st.rerun()
        
        # Custom query section (collapsed by default)
        with st.expander("🛠️ Advanced: Custom Query"):
            st.markdown("For advanced users who want to run custom queries")
            
            custom_query = st.text_area(
                "Enter your SQL query:",
                height=150,
                placeholder="SELECT * FROM your_table_name LIMIT 10;"
            )
            
            if st.button("Execute Custom Query"):
                if custom_query.strip():
                    with st.spinner("Executing query..."):
                        df = execute_query(st.session_state.connection, custom_query)
                        
                        if df is not None and not df.empty:
                            st.dataframe(df, use_container_width=True)
                            
                            csv = df.to_csv(index=False)
                            st.download_button(
                                label="Download as CSV",
                                data=csv,
                                file_name="custom_query_results.csv",
                                mime="text/csv"
                            )
                else:
                    st.warning("Please enter a query before executing.")
    
    else:
        st.info("Please connect to Snowflake first to explore lineage.")
        
        # Show example of what the tool does
        st.subheader("📖 About This Tool")
        st.markdown("""
        This tool helps you explore data lineage in Snowflake using the `GET_LINEAGE` function. You can:
        
        - **Trace downstream dependencies** - See what objects depend on your data
        - **Find upstream sources** - Discover where your data comes from  
        - **Analyze column-level lineage** - Understand how specific columns flow through your pipeline
        - **Visualize relationships** - Get insights into your data architecture
        
        **Example use cases:**
        - Impact analysis before making schema changes
        - Data governance and documentation
        - Troubleshooting data quality issues
        - Understanding complex data transformations
        """)
        
        st.subheader("🚀 Getting Started")
        st.markdown("""
        1. **Connect to Snowflake** using the button above
        2. **Fill in required object details:**
           - Database * (e.g., `TASTY_BYTES_SAMPLE_DATA`)
           - Schema * (e.g., `RAW_POS`)
           - Table * (e.g., `MENU`)
           - Column (optional - for column-level lineage)
        3. **Set lineage parameters:**
           - Direction * (upstream, downstream, or both)
           - Depth * (custom number of levels or "Until End" to traverse completely)
        4. **Explore your lineage!**
        
        **Depth Options:**
        - **Custom**: Specify exact number of levels (1-50)
        - **Until End**: Traverse lineage completely until no more relationships are found
        """)

if __name__ == "__main__":
    main()