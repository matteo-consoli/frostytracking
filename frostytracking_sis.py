import streamlit as st
import base64
import os
import json
import pandas as pd
import plotly.express as px
from snowflake.snowpark.context import get_active_session

# FrostyTracking, User Tracking Dashboard 
# Author: Matteo Consoli 
# Artifact: frostytracking_sis.py 
# Version: v.1.0 
# Date: 03-03-2024

### --------------------------- ###
### Header & Config             ###  
### --------------------------- ###

# Set page title, icon, description
st.set_page_config(
    page_title="FrostyTracking - #Track #User #Activities",
    layout="wide",
    initial_sidebar_state="expanded",
)

### ---------------------------- ###
### Sidebar - Configurations     ### 
### ---------------------------- ###

#Logo
image_name = 'logo.png'
mime_type = image_name.split('.')[-1:][0].lower() 
if os.path.isfile(image_name):
    with open(image_name, "rb") as f:
        content_bytes = f.read()
    content_b64encoded = base64.b64encode(content_bytes).decode()
    image_string = f'data:image/{mime_type};base64,{content_b64encoded}'
    st.sidebar.image(image_string)
else:
    st.sidebar.write("Logo not uploaded in Streamlit App Stage")

# Streamlit app
#st.sidebar.title('Analysis')

selected_insights = st.sidebar.selectbox('Select Dashboard',
                                           ['Login Tracking', 'Query Tracking',
                                            'DDL Tracking'])
st.sidebar.title('Configurations')
x_days = st.sidebar.slider('Analysis Timeframe (Last X Days):', 1, 90, 7, key="x_days")
user_filter = st.sidebar.text_input('Filter out following users (quoted, coma separated):', key="user_filter", placeholder="'admin','appuser'")
top_n = st.sidebar.number_input('Return Top-N results in tabular analysis:', min_value=1, max_value=100, step=1, value=10, key="top_n")
#Credits
st.sidebar.text("Author: Matteo Consoli")

### --------------------------- ###
### Snowflake Connection        ###  
### --------------------------- ###

def get_snowflake_connection():
        return get_active_session()
st.session_state.snowflake_connection = get_active_session()


# Function to fetch data from Snowflake
def fetch_data(query):
    conn = get_snowflake_connection()
    return conn.sql(query).collect();

# Function to get current date and X days ago
def get_date_range(x_days):
    end_date = pd.Timestamp.now().strftime("%Y-%m-%d")
    start_date = (pd.Timestamp.now() - pd.Timedelta(days=x_days)).strftime("%Y-%m-%d")
    return start_date, end_date


### ---------------------------- ###
### Main UI - Configurations     ### 
### ---------------------------- ###

# Display selected insights
if 'Login Tracking' in selected_insights:
    
    # Function to get top users accessing the UI
    def top_users_ui_access(x_days):
        start_date, end_date = get_date_range(x_days)
        query = f"""
        SELECT
            USER_NAME as User,
            COUNT(*) as Login_Count
        FROM
            SNOWFLAKE.ACCOUNT_USAGE.LOGIN_HISTORY
        WHERE
            EVENT_TIMESTAMP >= '{start_date}'
            AND EVENT_TIMESTAMP < '{end_date}'
            AND USER_NAME not in ({user_filter or "''"})
        GROUP BY
            USER_NAME
        ORDER BY
            COUNT(*) DESC
        LIMIT 10
        """
        return fetch_data(query)
    
    # Function to get top users based on the average number of logins in the last X days
    def top_users_avg_login(x_days):
        start_date, end_date = get_date_range(x_days)
        query = f"""
        SELECT
            USER_NAME as User,
            COUNT(*) / {x_days} as Avg_Login_Count
        FROM
            SNOWFLAKE.ACCOUNT_USAGE.LOGIN_HISTORY
        WHERE
            EVENT_TIMESTAMP >= '{start_date}'
            AND EVENT_TIMESTAMP < '{end_date}'
            AND USER_NAME not in ({user_filter or "''"})
        GROUP BY
            USER_NAME
        ORDER BY
            Avg_Login_Count DESC
        LIMIT 10
        """
        return fetch_data(query)
    
    # Function to get last 30 logins
    def last_30_logins():
        query = f"""
        SELECT
            USER_NAME as User,
            EVENT_TIMESTAMP as Login_Time,
            REPORTED_CLIENT_TYPE as Client_Type
        FROM
            SNOWFLAKE.ACCOUNT_USAGE.LOGIN_HISTORY
        WHERE 
            USER_NAME not in ({user_filter or "''"})
        ORDER BY
            EVENT_TIMESTAMP DESC
        LIMIT {top_n}
        """
        return fetch_data(query)
    st.header(f'Frosty Tracking - Login Tracking')
    st.markdown('---')

    # Load data
    top_users_ui_df = pd.DataFrame(top_users_ui_access(x_days), columns=['User', 'Login_Count'])
    top_users_avg_login_df = pd.DataFrame(top_users_avg_login(x_days), columns=['User', 'Avg_Login_Count'])

   # Create pie chart
    fig1 = px.pie(top_users_ui_df, names='User', values='Login_Count', color='User')
    fig1.update_layout(showlegend=False)
    
    # Create bar chart
    fig2 = px.bar(top_users_avg_login_df, x='User', y='Avg_Login_Count', color='User')

    # Display charts side by side
    col1, col2 = st.columns(2)
    with col1:
        st.text(f'Top {top_n} Users Accessing the UI in the last {x_days} days')
        st.plotly_chart(fig1, config={'displayModeBar': False}, use_container_width=True)
    with col2:
        st.text(f'Average Login Count per User in the last {x_days} days')
        st.plotly_chart(fig2, config={'displayModeBar': False}, use_container_width=True)
    
    # Display last 30 logins as a table
    st.text(f'Last {top_n} logins')
    last_30_logins_df = pd.DataFrame(last_30_logins())
    st.dataframe(last_30_logins_df.set_index(last_30_logins_df.columns[0]),use_container_width=True)
if 'Query Tracking' in selected_insights:
    
    # Function to get most accessed table by user
    def top_queries_executed(x_days):
        start_date, end_date = get_date_range(x_days)
        query = f"""
        SELECT
            USER_NAME,
            DATABASE_NAME,
            SCHEMA_NAME,
            WAREHOUSE_NAME,
            ROLE_NAME,
            COUNT(*) as Total_Count
        FROM
            SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
        WHERE
            START_TIME >= '{start_date}'
            AND START_TIME < '{end_date}'
            AND USER_NAME not in ({user_filter or "''"})
        GROUP BY
            USER_NAME,
            DATABASE_NAME,
            SCHEMA_NAME,
            WAREHOUSE_NAME,
            ROLE_NAME
        ORDER BY
            Total_Count DESC
        LIMIT {top_n}
        """
        return fetch_data(query)

    # Function to get most active users in query execution
    def most_active_users_query(x_days):
        start_date, end_date = get_date_range(x_days)
        query = f"""
        SELECT
            USER_NAME as User,
            COUNT(*) as Query_Count
        FROM
            SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
        WHERE
            START_TIME >= '{start_date}'
            AND START_TIME < '{end_date}'
            AND USER_NAME not in ({user_filter or "''"})
        GROUP BY
            USER_NAME
        ORDER BY
            Query_Count DESC
        LIMIT {top_n}
        """
        return fetch_data(query)

    # Function to get query execution trends
    def query_execution_trends(x_days):
        start_date, end_date = get_date_range(x_days)
        query = f"""
        SELECT
            DATE_TRUNC('day', START_TIME) AS Day,
            COUNT(*) as Query_Count
        FROM
            SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
        WHERE
            START_TIME >= '{start_date}'
            AND START_TIME < '{end_date}'
        GROUP BY Day
        ORDER BY Day
        """
        return fetch_data(query)
        
    st.header(f'Frosty Tracking - Query Tracking')
    st.markdown('---')
    # Load data
    top_queries_df = pd.DataFrame(top_queries_executed(x_days))
    most_active_users_df = pd.DataFrame(most_active_users_query(x_days))
    query_trends_df = pd.DataFrame(query_execution_trends(x_days))
    col1, col2 = st.columns(2)
    with col1:
        # Display query execution trends
        st.text('Query Execution Trends')
        fig = px.line(query_trends_df, x='DAY', y='QUERY_COUNT')
        st.plotly_chart(fig,config={'displayModeBar': False}, use_container_width=True)
    with col2:
        # Display most active users
        st.text(f'Top {top_n} Active Users by Query Execution')
        st.dataframe(most_active_users_df.set_index(most_active_users_df.columns[0]),use_container_width=True)

    # Display top queries executed
    st.text(f'Top {top_n} Common Query Patterns')
    st.dataframe(top_queries_df.set_index(top_queries_df.columns[0]),use_container_width=True)

if 'DDL Tracking' in selected_insights:
    st.header(f'Frosty Tracking - DDL Tracking')
    st.markdown('---')

    # Function to get DDL operation trends
    def ddl_operation_trends(x_days):
        start_date, end_date = get_date_range(x_days)
        query = f"""
        SELECT
            DATE_TRUNC('day', START_TIME) AS Day,
            QUERY_TYPE,
            COUNT(*) as Operation_Count
        FROM
            SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
        WHERE
            START_TIME >= '{start_date}'
            AND START_TIME < '{end_date}'
            AND QUERY_TYPE    ILIKE ANY ('CREATE%', 'ALTER%', 'DROP%', 'DESCRIBE%')
            AND USER_NAME not in ({user_filter or "''"})
        GROUP BY
            Day,
            QUERY_TYPE
        ORDER BY
            Day
        """
        return fetch_data(query)

    # Function to get most common DDL operations
    def most_common_ddl_operations(x_days):
        start_date, end_date = get_date_range(x_days)
        query = f"""
        SELECT
            DATABASE_NAME,
            SCHEMA_NAME,
            QUERY_TYPE,
            COUNT(*) as Operation_Count
        FROM
            SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
        WHERE
            START_TIME >= '{start_date}'
            AND START_TIME < '{end_date}'
            AND QUERY_TYPE    ILIKE ANY ('CREATE%', 'ALTER%', 'DROP%', 'DESCRIBE%')
            AND USER_NAME not in ({user_filter or "''"})
        GROUP BY
            DATABASE_NAME,
            SCHEMA_NAME,
            QUERY_TYPE
        ORDER BY
            Operation_Count DESC
        LIMIT {top_n}
        """
        return fetch_data(query)

    # Function to get DML operations by specific user
    def dml_operations_on_tables(x_days):
        start_date, end_date = get_date_range(x_days)
        query = f"""
        SELECT
            USER_NAME,
            QUERY_TYPE,
            COUNT(*) as Operation_Count
        FROM
            SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
        WHERE
            START_TIME >= '{start_date}'
            AND START_TIME < '{end_date}'
            AND QUERY_TYPE    ILIKE ANY ('CREATE%', 'ALTER%', 'DROP%')
            AND USER_NAME not in ({user_filter or "''"})
        GROUP BY
            USER_NAME,
            QUERY_TYPE
        ORDER BY
            Operation_Count DESC
        LIMIT {top_n}
        """
        return fetch_data(query)

    # Load data
    common_ddl_df = pd.DataFrame(most_common_ddl_operations(x_days))
    ddl_trends_df = pd.DataFrame(ddl_operation_trends(x_days))
    dml_on_tables_df = pd.DataFrame(dml_operations_on_tables(x_days))
    # Display DDL operation trends
    st.text('DDL Operation Trends')
    fig1 = px.line(ddl_trends_df, x='DAY', y='OPERATION_COUNT', color='QUERY_TYPE', title='DDL Operation Trends')
    st.plotly_chart(fig1, config={'displayModeBar': False}, use_container_width=True)

    col1, col2 = st.columns(2)
    with col1:
        # Display Most Common DDL Operations
        st.text(f'Top {top_n} DDL Operations')
        st.dataframe(common_ddl_df.set_index(common_ddl_df.columns[0]), use_container_width=True)

    with col2:
        # Display DML Operations on Specific Tables
        st.text(f'Top {top_n} DDL Operations by User')
        st.dataframe(dml_on_tables_df.set_index(dml_on_tables_df.columns[0]), use_container_width=True)

