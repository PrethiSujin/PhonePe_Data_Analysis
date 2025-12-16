

import streamlit as st
from streamlit_option_menu import option_menu
import plotly.express as px 
import pyodbc
import pandas as pd
import json
import requests
import re

# dataframe creation
# dataframe for agg_transaction
def fetch_table(server, database, table_name, username=None, password=None):
    """
    Connects to SQL Server and retrieves the entire table as a Pandas DataFrame.
    
    Parameters:
    - server: SQL Server instance name
    - database: Database name
    - table_name: Table to query
    - username: SQL Server authentication username (if using SQL Server Authentication)
    - password: SQL Server authentication password (if using SQL Server Authentication)
    """

    try:
        # Build connection string based on authentication method
        if username and password:
            # SQL Server Authentication
            conn_str = (
                f"DRIVER={{ODBC Driver 17 for SQL Server}};"
                f"SERVER={server};"
                f"DATABASE={database};"
                f"UID={username};"
                f"PWD={password};"
            )
        else:
            # Windows Authentication (trusted connection)
            conn_str = (
                f"DRIVER={{ODBC Driver 17 for SQL Server}};"
                f"SERVER={server};"
                f"DATABASE={database};"
                f"Trusted_Connection=yes;"
            )

        # Establish connection
        conn = pyodbc.connect(conn_str)
        
        # Read the table into a DataFrame
        query = f"SELECT * FROM [{table_name}];"  # Added square brackets for table name
        df = pd.read_sql(query, conn)

        # Close connection
        conn.close()

        return df

    except pyodbc.Error as e:
        print("Database connection failed:", e)
        return None
    except Exception as ex:
        print("Error:", ex)
        return None
    
Aggregated_transaction = fetch_table("LAPTOP-06C3FHVP\\SQLEXPRESS", "Phonepe_Project", "Aggregated_transaction")
Aggregated_user=fetch_table("LAPTOP-06C3FHVP\\SQLEXPRESS", "Phonepe_Project", "Aggregated_user")
Aggregated_insurance=fetch_table("LAPTOP-06C3FHVP\\SQLEXPRESS", "Phonepe_Project", "Aggregated_insurance")
Map_transaction = fetch_table("LAPTOP-06C3FHVP\\SQLEXPRESS", "Phonepe_Project", "Map_transaction")
Map_user= fetch_table("LAPTOP-06C3FHVP\\SQLEXPRESS", "Phonepe_Project", "Map_user")
Map_insurance = fetch_table("LAPTOP-06C3FHVP\\SQLEXPRESS", "Phonepe_Project", "Map_insurance")
Top_transaction= fetch_table("LAPTOP-06C3FHVP\\SQLEXPRESS", "Phonepe_Project", "Top_transaction")
Top_user = fetch_table("LAPTOP-06C3FHVP\\SQLEXPRESS", "Phonepe_Project", "Top_user")
Top_insurance= fetch_table("LAPTOP-06C3FHVP\\SQLEXPRESS", "Phonepe_Project", "Top_insurance")








# CREATE UNIFIED DATAFRAME


# -------------------------
# CREATE UNIFIED DATAFRAME FOR STATE WISE
# -------------------------

# 1) Aggregated Transaction (Count & Amount)
agg_trans = Aggregated_transaction.groupby(["State", "Year", "Quarter"]).agg({
    "Transaction_amount": "sum",
    "Transaction_count": "sum"
}).reset_index()

agg_trans_long = pd.melt(
    agg_trans,
    id_vars=["State", "Year", "Quarter"],
    value_vars=["Transaction_amount", "Transaction_count"],
    var_name="Metric_Name",
    value_name="Metric_Value"
)

agg_trans_long["Metric_Name"] = agg_trans_long["Metric_Name"].replace({
    "Transaction_amount": "Agg Transaction Amount",
    "Transaction_count": "Agg Transaction Count"
})

# 2) Map Transaction (Count & Amount)
map_trans = Map_transaction.groupby(["State", "Year", "Quarter"]).agg({
    "Transaction_amount": "sum",
    "Transaction_count": "sum"
}).reset_index()

map_trans_long = pd.melt(
    map_trans,
    id_vars=["State", "Year", "Quarter"],
    value_vars=["Transaction_amount", "Transaction_count"],
    var_name="Metric_Name",
    value_name="Metric_Value"
)

map_trans_long["Metric_Name"] = map_trans_long["Metric_Name"].replace({
    "Transaction_amount": "Map Transaction Amount",
    "Transaction_count": "Map Transaction Count"
})

# 3) Top Transaction
top_trans = Top_transaction.groupby(["State", "Year", "Quarter"]).agg({
    "Transaction_amount": "sum",
    "Transaction_count": "sum"
}).reset_index()

top_trans_long = pd.melt(
    top_trans,
    id_vars=["State", "Year", "Quarter"],
    value_vars=["Transaction_amount", "Transaction_count"],
    var_name="Metric_Name",
    value_name="Metric_Value"
)

top_trans_long["Metric_Name"] = top_trans_long["Metric_Name"].replace({
    "Transaction_amount": "Top Transaction Amount",
    "Transaction_count": "Top Transaction Count"
})

# 4) Aggregated User
agg_user = Aggregated_user.groupby(["State", "Year", "Quarter"]).agg({
    "User_count": "sum"
}).reset_index()

agg_user.rename(columns={"User_count": "Metric_Value"}, inplace=True)
agg_user["Metric_Name"] = "Aggregated User Count"

# 5) Map User (Registered Users, App Opens)
map_user = Map_user.groupby(["State", "Year", "Quarter"]).agg({
    "Map_registeredUsers": "sum",
    "Map_UserappOpens": "sum"
}).reset_index()

map_user_long = pd.melt(
    map_user,
    id_vars=["State", "Year", "Quarter"],
    value_vars=["Map_registeredUsers", "Map_UserappOpens"],
    var_name="Metric_Name",
    value_name="Metric_Value"
)

map_user_long["Metric_Name"] = map_user_long["Metric_Name"].replace({
    "Map_registeredUsers": "Map Registered Users",
    "Map_UserappOpens": "Map App Opens"
})

# 6) Top User
top_user = Top_user.groupby(["State", "Year", "Quarter"]).agg({
    "Top_RegisteredUsers": "sum"
}).reset_index()

top_user.rename(columns={"Top_RegisteredUsers": "Metric_Value"}, inplace=True)
top_user["Metric_Name"] = "Top Registered Users"


# 7) Aggregated Insurance
agg_insur = Aggregated_insurance.groupby(["State", "Year", "Quarter"]).agg({
    "Transaction_amount": "sum",
    "Transaction_count": "sum"
}).reset_index()

agg_insur_long = pd.melt(
    agg_insur,
    id_vars=["State", "Year", "Quarter"],
    value_vars=["Transaction_amount", "Transaction_count"],
    var_name="Metric_Name",
    value_name="Metric_Value"
)

agg_insur_long["Metric_Name"] = agg_insur_long["Metric_Name"].replace({
    "Transaction_amount": "Aggregated Insurance Amount",
    "Transaction_count": "Aggregated Insurance Count"
})


# 8) Map Insurance
map_insur = Map_insurance.groupby(["State", "Year", "Quarter"]).agg({
    "Transaction_amount": "sum",
    "Transaction_count": "sum"
}).reset_index()

map_insur_long = pd.melt(
    map_insur,
    id_vars=["State", "Year", "Quarter"],
    value_vars=["Transaction_amount", "Transaction_count"],
    var_name="Metric_Name",
    value_name="Metric_Value"
)

map_insur_long["Metric_Name"] = map_insur_long["Metric_Name"].replace({
    "Transaction_amount": "Map Insurance Amount",
    "Transaction_count": "Map Insurance Count"
})


# 9) Top Insurance
top_insur = Top_insurance.groupby(["State", "Year", "Quarter"]).agg({
    "Transaction_amount": "sum",
    "Transaction_count": "sum"
}).reset_index()

top_insur_long = pd.melt(
    top_insur,
    id_vars=["State", "Year", "Quarter"],
    value_vars=["Transaction_amount", "Transaction_count"],
    var_name="Metric_Name",
    value_name="Metric_Value"
)

top_insur_long["Metric_Name"] = top_insur_long["Metric_Name"].replace({
    "Transaction_amount": "Top Insurance Amount",
    "Transaction_count": "Top Insurance Count"
})


# -------------------------
# COMBINE ALL TABLES
# -------------------------
unified_df = pd.concat([
    agg_trans_long,
    map_trans_long,
    top_trans_long,
    agg_user,
    map_user_long,
    top_user,
    agg_insur_long,
    map_insur_long,
    top_insur_long
], ignore_index=True)

# Clean State names and rows
unified_df.dropna(subset=["State"], inplace=True)
unified_df.reset_index(drop=True, inplace=True)



# CREATE UNIFIED DATAFRAME FOR DISTRICT WISE

# MAP TRANSACTION
Map_transaction1 = Map_transaction.copy()

Map_transaction1["District"] = (
    Map_transaction1["Transaction_type"]
    .str.replace(" district", "", case=False, regex=False)
    .str.strip()
    .str.title()
)

map_trans_dist = Map_transaction1.groupby(
    ["State", "Year", "Quarter", "District"]
).agg({
    "Transaction_amount": "sum",
    "Transaction_count": "sum"
}).reset_index()

map_trans_dist_long = pd.melt(
    map_trans_dist,
    id_vars=["State", "Year", "Quarter", "District"],
    value_vars=["Transaction_amount", "Transaction_count"],
    var_name="Metric_Name",
    value_name="Metric_Value"
)

map_trans_dist_long["Metric_Name"] = map_trans_dist_long["Metric_Name"].replace({
    "Transaction_amount": "Map Transaction Amount",
    "Transaction_count": "Map Transaction Count"
    
})

map_trans_dist_long.rename(
    columns={"Map_Transactiondistrict": "District"},
    inplace=True
)

# MAP USER

map_user_dist = Map_user.groupby(
    ["State", "Year", "Quarter", "Map_Userdistrict"]
).agg({
    "Map_registeredUsers": "sum",
    "Map_UserappOpens": "sum"
}).reset_index()

map_user_dist_long = pd.melt(
    map_user_dist,
    id_vars=["State", "Year", "Quarter", "Map_Userdistrict"],
    value_vars=["Map_registeredUsers", "Map_UserappOpens"],
    var_name="Metric_Name",
    value_name="Metric_Value"
)

map_user_dist_long["Metric_Name"] = map_user_dist_long["Metric_Name"].replace({
    "Map_registeredUsers": "Map Registered Users",
    "Map_UserappOpens": "Map App Opens"
})



map_user_dist_long.rename(
    columns={"Map_Userdistrict": "District"},
    inplace=True
)

map_user_dist_long["District"] = (
    map_user_dist_long["District"]
    .str.replace(" district", "", case=False, regex=True)
    .str.strip()
    .str.title()
)

# MAP INSURANCE
Map_insurance1 = Map_insurance.copy()

Map_insurance1["District"] = (
    Map_insurance1["Transaction_type"]
    .str.replace(" district", "", case=False,regex=False)
    .str.strip()
    .str.title()
)

map_insur_dist = Map_insurance1.groupby(
    ["State", "Year", "Quarter", "District"]
).agg({
    "Transaction_amount": "sum",
    "Transaction_count": "sum"
}).reset_index()

map_insur_dist_long = pd.melt(
    map_insur_dist,
    id_vars=["State", "Year", "Quarter", "District"],
    value_vars=["Transaction_amount", "Transaction_count"],
    var_name="Metric_Name",
    value_name="Metric_Value"
)

map_insur_dist_long["Metric_Name"] = map_insur_dist_long["Metric_Name"].replace({
    "Transaction_amount": "Map Insurance Amount",
    "Transaction_count": "Map Insurance Count"
    
})

map_insur_dist_long.rename(
    columns={"Map Insurancedistrict": "District"},
    inplace=True
)

# COMBINED DF

unified_district_df = pd.concat([
    map_trans_dist_long,
    map_user_dist_long,
    map_insur_dist_long
], ignore_index=True)

unified_district_df.dropna(subset=["State", "District"], inplace=True)
unified_district_df.reset_index(drop=True, inplace=True)


unified_district_df["District"] = (
    unified_district_df["District"]
    .str.strip()
    .str.title()
)




def transaction_count_amount_Y(df,year):

    new=df[df["Year"]==year]
    new.reset_index(drop=True, inplace=True)
    
    newg=new.groupby("State")[["Transaction_count","Transaction_amount"]].sum()
   
    newg.reset_index( inplace=True)
    
    col1,col2=st.columns(2)
    with col1:
        fig_amount=px.bar(newg,x="State",y="Transaction_amount",title=f"{year} TRANSACTION AMOUNT", height=650,width=600)
        st.plotly_chart(fig_amount)
    with col2:
        fig_count=px.bar(newg,x="State",y="Transaction_count",title=f"{year} TRANSACTION COUNT", height=650,width=600)
        st.plotly_chart(fig_count)
    
    
    col1,col2=st.columns(2)

    with col1:
    
    
        df1="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
        response=requests.get(df1)
        data1=json.loads(response.content)
        state_name=[]
        for feature in data1["features"]:
            state_name.append(feature["properties"]["ST_NM"])
        state_name.sort()
    
        fig_india_1 = px.choropleth(
            newg,
            geojson=data1,
            featureidkey='properties.ST_NM',
            locations='State',
            range_color=(newg["Transaction_amount"].min(),newg["Transaction_amount"].max()),
            hover_name='State',
            title=f"{year} TRANSACTION AMOUNT",
            height= 600,width= 600,
            color='Transaction_amount',
            color_continuous_scale='Rainbow',fitbounds="locations"
        )
    
        fig_india_1.update_geos( visible=False)
        st.plotly_chart(fig_india_1)
    with col2:
        fig_india_2 = px.choropleth(
            newg,
            geojson=data1,
            featureidkey='properties.ST_NM',
            locations=newg["State"],
            range_color=(newg["Transaction_count"].min(),newg["Transaction_count"].max()),
            hover_name=newg["State"],
            title=f"{year} TRANSACTION COUNT",
            height= 600,width= 600,
            color='Transaction_count',
            color_continuous_scale='Rainbow',fitbounds="locations"
        )
    
        fig_india_2.update_geos( visible=False)
        st.plotly_chart(fig_india_2) 

        return new
   
def transaction_count_amount_Y_Q(df,year, quarter):
    df = df[df["Quarter"] == int(quarter)]
    newg = df.groupby("State")[["Transaction_amount", "Transaction_count"]].sum()
    newg.reset_index(inplace= True)

    col1,col2=st.columns(2)
    with col1:
        
    # Amount chart
        fig_amount = px.bar(
            newg,
            x="State",
            y="Transaction_amount",
            title=f"{df['Year'].min()} YEAR {quarter} QUARTER TRANSACTION AMOUNT",
            height=650,
            width=600
        )
        st.plotly_chart(fig_amount)
    with col2:
        
        # Count chart
        fig_count = px.bar(
            newg,
            x="State",
            y="Transaction_count",
            title=f"{df['Year'].min()} YEAR {quarter} QUARTER TRANSACTION COUNT",
            height=650,
            width=600
        )
        st.plotly_chart(fig_count)
    

    col1,col2=st.columns(2)

    with col1:
        df1 ="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
        
        
        response=requests.get(df1)
        data1=json.loads(response.content)
        state_name=[]
        for feature in data1["features"]:
            state_name.append(feature["properties"]["ST_NM"])
        state_name.sort()
        
        fig_india_1 = px.choropleth(
            newg,
            geojson=data1,
            featureidkey='properties.ST_NM',
            locations=newg["State"],
            range_color=(newg["Transaction_amount"].min(),newg["Transaction_amount"].max()),
            hover_name=newg["State"],
            title=f"{df['Year'].min()} YEAR {quarter} QUARTER TRANSACTION AMOUNT",
            height= 600,width= 600,
            color='Transaction_amount',
            color_continuous_scale='Rainbow'
        )
        
        fig_india_1.update_geos(fitbounds="locations", visible=False)
        st.plotly_chart(fig_india_1)
    with col2:    
        fig_india_2 = px.choropleth(
            newg,
            geojson=data1,
            featureidkey='properties.ST_NM',
            locations=newg["State"],
            range_color=(newg["Transaction_count"].min(),newg["Transaction_count"].max()),
            hover_name=newg["State"],
            title=f"{df['Year'].min()} YEAR {quarter} QUARTER TRANSACTION COOUNT",
            height= 600,width= 600,
            color='Transaction_count',
            color_continuous_scale='Rainbow'
        )
        
        fig_india_2.update_geos(fitbounds="locations", visible=False)
        st.plotly_chart(fig_india_2)

    return df
# agg trans type
def aggre_tran_t_type(df,state):

    new = df[df["State"] == state]
    new.reset_index(drop= True,inplace= True)
    
    newg = new.groupby("Transaction_type")[["Transaction_amount", "Transaction_count"]].sum()
    newg.reset_index(inplace= True)
    col1,col2=st.columns(2)
    with col1:
        fig_pie_1=px.pie(data_frame= newg, names= "Transaction_type", values="Transaction_amount",
                        width= 600, title= f"{state.upper()} TRANSACTION AMOUNT", hole=0.5,color_discrete_sequence=px.colors.qualitative.Plotly)
        st.plotly_chart(fig_pie_1)

    with col2:
        fig_pie_2=px.pie(data_frame= newg, names= "Transaction_type", values="Transaction_count",
                        width= 600, title= f"{state.upper()} TRANSACTION COUNT", hole=0.5,color_discrete_sequence=px.colors.qualitative.Plotly)
        st.plotly_chart(fig_pie_2)


# aggregated user
def Agg_user1(df,year):
    df=df[df["Year"]==year]
    df.reset_index()
    agg_user_y_g=pd.DataFrame(df.groupby('User_brand')['User_count'].sum())
    agg_user_y_g.reset_index(inplace= True)
    
    fig_user_count_bar1=px.bar(agg_user_y_g, x="User_brand",y="User_count",title=f"{year} USER BRAND TRANSACTION COUNT",height=600,width=800,color_continuous_scale=px.colors.sequential.Viridis,hover_name="User_brand")
    st.plotly_chart(fig_user_count_bar1)

    return df

# aggregated user quarter
def Agg_user2(df,quarter):
    df=df[df["Quarter"]==quarter]
    df.reset_index(drop=True, inplace=True)
    
    agg_user_yqg=pd.DataFrame(df.groupby("User_brand")["User_count"].sum())
    agg_user_yqg.reset_index(inplace= True)
    
    fig_userq_count_bar1 =px.bar(agg_user_yqg, x="User_brand",y="User_count",title=f"{quarter} QUARTER USER BRAND TRANSACTION COUNT",height=600,width=800,color_continuous_scale=px.colors.qualitative.Plotly)
    st.plotly_chart(fig_userq_count_bar1)
    return df


#aggregated user 3
def Agg_user3(df, state):
    Agg_us_ye_qs=df[df["State"] == "Tamil Nadu"]
    Agg_us_ye_qs.reset_index(drop= True,inplace= True)
    
    fig_line1=px.line(Agg_us_ye_qs,x="User_brand", y="User_count", hover_data="User_percentage",title=f"{state.upper()} USER BRAND TRANSACTION COUNT AND PERCENTAGE",width=1000,markers= True)
    st.plotly_chart(fig_line1)

# map_insurance_district
def map_insur_dist_type(df,state):

    new = df[df["State"] == state]
    new.reset_index(drop= True,inplace= True)
    
    newg = new.groupby("Transaction_type")[["Transaction_amount", "Transaction_count"]].sum()
    newg.reset_index(inplace= True)
    col1,col2=st.columns(2)
    with col1:
        fig_bar_1=px.bar(newg, y= "Transaction_type", x="Transaction_amount",orientation="h",height=600,
                        title= f"{state.upper()} DISTRICT TRANSACTION AMOUNT",color_discrete_sequence=px.colors.qualitative.Plotly)
        st.plotly_chart(fig_bar_1)
    with col2:    
        fig_bar_2=px.bar(newg, y= "Transaction_type", x="Transaction_count",orientation="h",height=600,
                        title= f"{state.upper()} DISTRICT TRANSACTION COUNT",color_discrete_sequence=px.colors.qualitative.Plotly)
        st.plotly_chart(fig_bar_2)



# map user p1
def map_user_p1(df,year):
    map_user_y=df[df["Year"]== year]
    map_user_y.reset_index(drop=True, inplace=True)
    
    map_user_y_g=map_user_y.groupby('State')[['Map_registeredUsers','Map_UserappOpens']].sum()
    map_user_y_g.reset_index(inplace= True)
    
    
    fig_line2=px.line(map_user_y_g,x="State", y=["Map_registeredUsers","Map_UserappOpens"] ,title=f"{year} REGISTERED USER APP OPENS",width=1000,height=800,markers= True,color_discrete_sequence=px.colors.qualitative.Set1 )
    st.plotly_chart(fig_line2)

    return map_user_y

# map user p2
def map_user_p2(df,quarter):
    map_user_yq=df[df["Quarter"]== quarter]
    map_user_yq.reset_index(drop=True, inplace=True)
    
    map_user_yq_g=map_user_yq.groupby('State')[['Map_registeredUsers','Map_UserappOpens']].sum()
    map_user_yq_g.reset_index(inplace= True)
    
    
    fig_line3=px.line(map_user_yq_g,x="State", y=["Map_registeredUsers","Map_UserappOpens"] ,title=f"{df['Year'].min()} YEAR {quarter}  QUARTER REGISTERED USER APP OPENS",width=1000,height=800,markers=True,color_discrete_sequence=px.colors.qualitative.Set1 )
    st.plotly_chart(fig_line3)

    return map_user_yq




# top_user for year
def top_user_p1(df,year):
    top_user_y=df[df["Year"]==year]
    top_user_y.reset_index()
    
    top_user_y_g=pd.DataFrame(top_user_y.groupby(['State','Quarter'])['Top_RegisteredUsers'].sum())
    top_user_y_g.reset_index(inplace= True)
    top_user_y_g
    
    fig_top_user1=px.bar(top_user_y_g, x='State', y='Top_RegisteredUsers',color='Quarter',hover_name='State',width=1000,height=800,color_discrete_sequence=px.colors.qualitative.Alphabet[11],title=f"{year} REGISTERED USERS")
    st.plotly_chart(fig_top_user1)

    return top_user_y

# map_user_p3 for district
def map_user_p3(df, state):
    map_user_yq_state=df[df["State"]== state]
    map_user_yq_state.reset_index(drop=True, inplace=True)
    
    col1,col2=st.columns(2)
    with col1:
        fig_map_user_state_bar1=px.bar(map_user_yq_state,x="Map_registeredUsers",y="Map_Userdistrict",orientation="h",title=f"{state.upper()} REGISTERED USER",height=800,color_discrete_sequence=px.colors.sequential.Rainbow_r)
        st.plotly_chart(fig_map_user_state_bar1)
    with col2:
        fig_map_user_state_bar2=px.bar(map_user_yq_state,x="Map_UserappOpens",y="Map_Userdistrict",orientation="h",title=f"{state.upper()}REGISTERED USER",height=800,color_discrete_sequence=px.colors.sequential.Rainbow)
        st.plotly_chart(fig_map_user_state_bar2)

# top insurance plot 1
def top_insur_p1(df,state):
    top_insur_y=df[df["State"]== state]
    top_insur_y.reset_index(drop=True, inplace=True)
    col1,col2=st.columns(2)
    with col1:
        fig_top_insur_bar1=px.bar(top_insur_y,x="Quarter",y="Transaction_amount",hover_data="Transaction_Pincode",title="TRANSACTION AMOUNT",height=600,width=600,color_discrete_sequence=px.colors.sequential.Rainbow_r)
        st.plotly_chart(fig_top_insur_bar1)
    with col2:
    
        fig_top_insur_bar2=px.bar(top_insur_y,x="Quarter",y="Transaction_count",hover_data="Transaction_Pincode",title="TRANSACTION COUNT",height=600,width=600,color_discrete_sequence=px.colors.sequential.Rainbow_r)
        st.plotly_chart(fig_top_insur_bar2)

    return top_insur_y


# top user for state p1
def top_user_y_state(df,state):
    top_user_y_state=df[df["State"] == state]
    top_user_y_state.reset_index(drop=True, inplace=True)
    
    fig_top_user_p1=px.bar(top_user_y_state,x="Quarter",y="Top_RegisteredUsers",title="REGISTERED USERS PINCODES QUARTER",width=1000,height=800,color="Top_RegisteredUsers",hover_data="Top_UserPincodes",color_discrete_sequence=px.colors.qualitative.Set1)
    st.plotly_chart(fig_top_user_p1)
 
    return top_user_y_state



# Connect using Windows Authentication for top chart
def top_chart_transaction_amount(table_name):
    try:
        conn = pyodbc.connect(
            "Driver={SQL Server};"
            "Server=LAPTOP-06C3FHVP\SQLEXPRESS;" 
            "Database=Phonepe_Project;"
            "Trusted_Connection=yes;"
            )
    
        cursor = conn.cursor()
       
        FD=f'''SELECT TOP 10 State,
        SUM (Transaction_amount) AS Transaction_amount 
        FROM {table_name} 
        GROUP BY State 
        ORDER BY Transaction_amount DESC;''' 
    
    
        cursor.execute(FD)
        MD=cursor.fetchall()
        conn.commit()
    
        # top 10 states
        df1 = pd.DataFrame([(row[0], row[1]) for row in MD], columns=['state', 'transaction_amount'])
        
        column1,column2=st.columns(2)
        with column1:
            fig_amount1 = px.bar(
                df1,
                x="state",
                y="transaction_amount",
                title=" TOP 10 STATES REVIEW FOR TRANSACTION AMOUNT",
                height=650,
                width=600,hover_name="state"
            )
            st.plotly_chart(fig_amount1)
        
        FF=f'''SELECT TOP 10 State,
        SUM (Transaction_amount) AS Transaction_amount 
        FROM {table_name} 
        GROUP BY State 
        ORDER BY Transaction_amount ASC ;''' 
    
    
        cursor.execute(FF)
        MM=cursor.fetchall()
        conn.commit()
    
        # low 10 states
        df2 = pd.DataFrame([(row[0], row[1]) for row in MM], columns=['state', 'transaction_amount'])
        with column2:
            fig_amount2 = px.bar(
                df2,
                x="state",
                y="transaction_amount",
                title=" LOW 10 STATES REVIEW FOR TRANSACTION AMOUNT",
                height=650,
                width=600,hover_name="state"
            )
            st.plotly_chart(fig_amount2)
        
        # avg values for states
        LL=f''' SELECT State, AVG(Transaction_amount) AS Transaction_amount 
        FROM {table_name} 
        GROUP BY State 
        ORDER BY Transaction_amount;
        ''' 
    
    
        cursor.execute(LL)
        SS=cursor.fetchall()
        conn.commit()
    
        
        df3 = pd.DataFrame([(row[0], row[1]) for row in SS], columns=['state', 'transaction_amount'])
        fig_amount3 = px.bar(
            df3,
            x="transaction_amount",
            y="state",
            
            title=" STATES REVIEW FOR AVERAGE TRANSACTION AMOUNT",
            height=800,
            width=1000,hover_name="state",orientation='h'
        )
        st.plotly_chart(fig_amount3)
    
    finally:
        cursor.close()
        conn.close()


# Connect using Windows Authentication for top chart
def top_chart_transaction_count(table_name):
    try:
        conn = pyodbc.connect(
            "Driver={SQL Server};"
            "Server=LAPTOP-06C3FHVP\SQLEXPRESS;" 
            "Database=Phonepe_Project;"
            "Trusted_Connection=yes;"
            )
    
        cursor = conn.cursor()
       
        FD=f'''SELECT TOP 10 State,
        SUM (Transaction_count) AS Transaction_count 
        FROM {table_name} 
        GROUP BY State 
        ORDER BY Transaction_count DESC;''' 
    
    
        cursor.execute(FD)
        MD=cursor.fetchall()
        conn.commit()
    
        # top 10 states
        df1 = pd.DataFrame([(row[0], row[1]) for row in MD], columns=['state', 'transaction_count'])
        
        col1,col2=st.columns(2)
        with col1:
            fig_amount_t1 = px.bar(
                df1,
                x="state",
                y="transaction_count",
                title=" TOP 10 STATES REVIEW FOR TRANSACTION COUNT",
                height=650,
                width=600,hover_name="state"
            )
            st.plotly_chart(fig_amount_t1)
        
        FF=f'''SELECT TOP 10 State,
        SUM (Transaction_count) AS Transaction_count 
        FROM {table_name} 
        GROUP BY State 
        ORDER BY Transaction_count ASC ;''' 
    
    
        cursor.execute(FF)
        MM=cursor.fetchall()
        conn.commit()
    
        # low 10 states
        df2 = pd.DataFrame([(row[0], row[1]) for row in MM], columns=['state', 'transaction_count'])
        with col2:
            fig_amount_t2 = px.bar(
                df2,
                x="state",
                y="transaction_count",
                title=" LOW 10 STATES REVIEW FOR TRANSACTION COUNT",
                height=650,
                width=600,hover_name="state"
            )
            st.plotly_chart(fig_amount_t2)
        
        # avg values for states
        LL=f''' SELECT State, AVG(Transaction_count) AS Transaction_count 
        FROM {table_name} 
        GROUP BY State 
        ORDER BY Transaction_count;
        ''' 
    
    
        cursor.execute(LL)
        SS=cursor.fetchall()
        conn.commit()
    
        
        df3 = pd.DataFrame([(row[0], row[1]) for row in SS], columns=['state', 'transaction_count'])
        fig_amount_t3 = px.bar(
            df3,
            x="transaction_count",
            y="state",
            
            title=" STATES REVIEW FOR AVERAGE TRANSACTION COUNT",
            height=800,
            width=1000,hover_name="state",orientation='h'
        )
        st.plotly_chart(fig_amount_t3)
    
    finally:
        cursor.close()
        conn.close()



# Connect using Windows Authentication for top chart user
def top_chart_transaction_count_user(table_name):
    try:
        conn = pyodbc.connect(
            "Driver={SQL Server};"
            "Server=LAPTOP-06C3FHVP\SQLEXPRESS;" 
            "Database=Phonepe_Project;"
            "Trusted_Connection=yes;"
            )
    
        cursor = conn.cursor()
       
        u1=f'''SELECT TOP 10 State,
        SUM (User_count) AS User_count 
        FROM {table_name} 
        GROUP BY State 
        ORDER BY User_count DESC;''' 
    
    
        cursor.execute(u1)
        s1=cursor.fetchall()
        conn.commit()
    
        # top 10 states
        d1 = pd.DataFrame([(row[0], row[1]) for row in s1], columns=['state', 'user_count'])
        col1,col2=st.columns(2)
        with col1:
            fig_amount_u1 = px.bar(
                d1,
                x="state",
                y="user_count",
                title=" TOP 10 STATES REVIEW FOR TRANSACTION COUNT",
                height=650,
                width=600,hover_name="state"
            )
            st.plotly_chart(fig_amount_u1)
    
        u2=f'''SELECT TOP 10 State,
        SUM (User_count) AS User_count 
        FROM {table_name} 
        GROUP BY State 
        ORDER BY User_count ASC ;''' 
    
    
        cursor.execute(u2)
        s2=cursor.fetchall()
        conn.commit()
    
        # low 10 states
        d2 = pd.DataFrame([(row[0], row[1]) for row in s2], columns=['state', 'user_count'])
        with col2:
            fig_amount_u2 = px.bar(
                d2,
                x="state",
                y="user_count",
                title=" LOW 10 STATES REVIEW FOR TRANSACTION COUNT",
                height=650,
                width=600,hover_name="state"
            )
            st.plotly_chart(fig_amount_u2)
        
        # avg values for states
        u3=f''' SELECT State, AVG(User_count) AS User_count 
        FROM {table_name} 
        GROUP BY State 
        ORDER BY User_count;
        ''' 
    
    
        cursor.execute(u3)
        s3=cursor.fetchall()
        conn.commit()
    
        
        d3 = pd.DataFrame([(row[0], row[1]) for row in s3], columns=['state', 'user_count'])
        fig_amount_u3 = px.bar(
            d3,
            x="user_count",
            y="state",
            
            title=" STATES REVIEW FOR AVERAGE TRANSACTION COUNT",
            height=800,
            width=1000,hover_name="state",orientation='h'
        )
        st.plotly_chart(fig_amount_u3)
    
    finally:
        cursor.close()
        conn.close()



# Connect using Windows Authentication for top chart registered user for map
def top_chart_registered_user(table_name, state):
    try:
        conn = pyodbc.connect(
            "Driver={SQL Server};"
            "Server=LAPTOP-06C3FHVP\SQLEXPRESS;" 
            "Database=Phonepe_Project;"
            "Trusted_Connection=yes;"
            )
    
        cursor = conn.cursor()
       
        rr=f'''SELECT  TOP 10 Map_Userdistrict,SUM(Map_registeredUsers) AS Registered_user
                FROM {table_name}
                WHERE State='{state}'
                GROUP BY Map_Userdistrict
                ORDER BY Registered_user DESC;''' 
    
    
        cursor.execute(rr)
        rr1=cursor.fetchall()
        conn.commit()
    
        # top 10 states
        df1 = pd.DataFrame([(row[0], row[1]) for row in rr1], columns=['district', 'registered_user'])
        
        col1,col2=st.columns(2)
        with col1:
            fig_amount_t1 = px.bar(
                df1,
                x="district",
                y="registered_user",
                title=" TOP 10 STATES REVIEW FOR REGISTERED USER",
                height=650,
                width=600,hover_name="district"
            )
            st.plotly_chart(fig_amount_t1)
        
        qq=f'''SELECT  TOP 10 Map_Userdistrict,SUM(Map_registeredUsers) AS Registered_user
                FROM {table_name}
                WHERE State='{state}'
                GROUP BY Map_Userdistrict
                ORDER BY Registered_user ;''' 
    
    
        cursor.execute(qq)
        qq1=cursor.fetchall()
        conn.commit()
    
        # low 10 states
        df2 = pd.DataFrame([(row[0], row[1]) for row in qq1], columns=['district', 'registered_user'])
        with col2:
            fig_amount_t2 = px.bar(
                df2,
                x="district",
                y="registered_user",
                title=" LOW 10 STATES REVIEW FOR REGISTERED USER",
                height=650,
                width=600,hover_name="district"
            )
            st.plotly_chart(fig_amount_t2)
    
        # avg values for states
        ee=f''' SELECT Map_Userdistrict,AVG(Map_registeredUsers) AS Registered_user
                FROM {table_name}
                WHERE State='{state}'
                GROUP BY Map_Userdistrict
                ORDER BY Registered_user;


        ''' 
    
    
        cursor.execute(ee)
        ee1=cursor.fetchall()
        conn.commit()
    
        
        df3 = pd.DataFrame([(row[0], row[1]) for row in ee1], columns=['district', 'registered_user'])
        fig_amount_t3 = px.bar(
            df3,
            x="registered_user",
            y="district",
            
            title=" AVERAGE VALUE OF REGISTERED USER",
            height=800,
            width=1000,hover_name="district",orientation='h'
        )
        st.plotly_chart(fig_amount_t3)
    
    finally:
        cursor.close()
        conn.close()


# Connect using Windows Authentication for top registered user
def top_chart_REGISTERED_USER(table_name):
    try:
        conn = pyodbc.connect(
            "Driver={SQL Server};"
            "Server=LAPTOP-06C3FHVP\SQLEXPRESS;" 
            "Database=Phonepe_Project;"
            "Trusted_Connection=yes;"
            )
    
        cursor = conn.cursor()
       
        rr=f'''SELECT  TOP 10 State, SUM(Top_RegisteredUsers) AS Registered_users 
                FROM Top_user
                GROUP BY State 
                ORDER BY Registered_users DESC;
                ''' 
    
    
        cursor.execute(rr)
        rr1=cursor.fetchall()
        conn.commit()
    
        # top 10 states
        df1 = pd.DataFrame([(row[0], row[1]) for row in rr1], columns=['State', 'Registered_users'])
        
        col1,col2=st.columns(2)
        with col1:
            fig_amount_N1 = px.bar(
                df1,
                x="State",
                y="Registered_users",
                title=" TOP 10 STATES REVIEW FOR REGISTERED USERS",
                height=650,
                width=600,hover_name="State"
            )
            st.plotly_chart(fig_amount_N1)
    
        qq=f'''SELECT  TOP 10 State, SUM(Top_RegisteredUsers) AS Registered_users 
                FROM Top_user
                GROUP BY State 
                ORDER BY Registered_users;

                ''' 
    
    
        cursor.execute(qq)
        qq1=cursor.fetchall()
        conn.commit()
    
        # low 10 states
        df2 = pd.DataFrame([(row[0], row[1]) for row in qq1], columns=['State', 'Registered_users'])
        with col2:
            fig_amount_N2 = px.bar(
                df2,
                x="State",
                y="Registered_users",
                title=" LOWER 10 STATES REVIEW FOR APPOPENS",
                height=650,
                width=600,hover_name="State"
            )
            st.plotly_chart(fig_amount_N2)
    
        # avg values for states
        ee=f''' SELECT  TOP 10 State, AVG(Top_RegisteredUsers) AS Registered_users 
                FROM Top_user
                GROUP BY State 
                ORDER BY Registered_users;
                '''

    
        cursor.execute(ee)
        ee1=cursor.fetchall()
        conn.commit()
    
        
        df3 = pd.DataFrame([(row[0], row[1]) for row in ee1], columns=['State', 'Registered_users'])
        fig_amount_N3 = px.bar(
            df3,
            x="Registered_users",
            y="State",
            
            title=" AVERAGE VALUE OF APPOPENS",
            height=800,
            width=1000,hover_name="State",orientation='h'
        )
        st.plotly_chart(fig_amount_N3)
    
    finally:
        cursor.close()
        conn.close()

        

# Connect using Windows Authentication for top chart appopens
def top_chart_appopens(table_name, state):
    try:
        conn = pyodbc.connect(
            "Driver={SQL Server};"
            "Server=LAPTOP-06C3FHVP\SQLEXPRESS;" 
            "Database=Phonepe_Project;"
            "Trusted_Connection=yes;"
            )
    
        cursor = conn.cursor()
       
        rr=f'''SELECT  TOP 10 Map_Userdistrict,SUM(Map_UserappOpens) AS Appopens
                FROM Map_user
                WHERE State='West Bengal'
                GROUP BY Map_Userdistrict
                ORDER BY Appopens DESC;
                ''' 
    
    
        cursor.execute(rr)
        rr1=cursor.fetchall()
        conn.commit()
    
        # top 10 states
        df1 = pd.DataFrame([(row[0], row[1]) for row in rr1], columns=['district', 'Appopens'])
        
        col1,col2=st.columns(2)
        with col1:
            fig_amount_a1 = px.bar(
                df1,
                x="district",
                y="Appopens",
                title=" TOP 10 DISTRICT REVIEW FOR APPOPENS",
                height=650,
                width=600,hover_name="district"
            )
            st.plotly_chart(fig_amount_a1)
        
        qq=f'''SELECT  TOP 10 Map_Userdistrict,SUM(Map_UserappOpens) AS Appopens
                FROM Map_user
                WHERE State='West Bengal'
                GROUP BY Map_Userdistrict
                ORDER BY Appopens ;
                ''' 
    
    
        cursor.execute(qq)
        qq1=cursor.fetchall()
        conn.commit()
    
        # low 10 states
        df2 = pd.DataFrame([(row[0], row[1]) for row in qq1], columns=['district', 'Appopens'])
        with col2:
            fig_amount_a2 = px.bar(
                df2,
                x="district",
                y="Appopens",
                title=" LOWER 10 DISTRICT REVIEW FOR APPOPENS",
                height=650,
                width=600,hover_name="district"
            )
            st.plotly_chart(fig_amount_a2)
        
        # avg values for states
        ee=f''' SELECT Map_Userdistrict,AVG(Map_UserappOpens) AS Appopens
                FROM Map_user
                WHERE State='West Bengal'
                GROUP BY Map_Userdistrict
                ORDER BY Appopens;'''

    
        cursor.execute(ee)
        ee1=cursor.fetchall()
        conn.commit()
    
        
        df3 = pd.DataFrame([(row[0], row[1]) for row in ee1], columns=['district', 'Appopens'])
        fig_amount_a3 = px.bar(
            df3,
            x="Appopens",
            y="district",
            
            title=" AVERAGE VALUE OF APPOPENS",
            height=800,
            width=1000,hover_name="district",orientation='h'
        )
        st.plotly_chart(fig_amount_a3)
    
    finally:
        cursor.close()
        conn.close()


def normalize_district(name):
    if not isinstance(name, str):
        return ""
    name = name.lower()
    name = name.replace("&", "and")
    name = re.sub(r"\bdistrict\b", "", name)
    name = name.replace("thir", "tir")
    name = name.replace("thoothukudi", "thoothukkudi")
    name = name.replace("kancheepuram", "kanchipuram")
    name = re.sub(r"\s+", " ", name)
    return name.strip()

def normalize_state_geo(name):
    return (
        name.lower()
        .replace("&", "and")
        .strip()
    )



# streamlit part           
      
st.set_page_config(layout="wide")
st.title("PhonePe data visualization and exploration")
with st.sidebar:
    select=option_menu("MENU",["HOME","DATA EXPLORATION","TOP CHARTS"])
if select == "HOME":
    with st.expander("About Phonepe Data"):
        st.write("PhonePe Pulse is a data analytics platform that provides insights into how Indians are using digital payments. It features over 2000 crore transactions and is the largest digital payments platform in India, holding a 46% UPI market share. Users can access and visualize data through an interactive map of India, gaining deep insights into digital payment trends and patterns. The platform is designed to help users explore and understand the data effectively, making complex information more accessible and engaging.")
    with st.expander("MAP VIEW OF PHONEPE"):


       
        st.subheader("📊 Phonepe Transactions India Choropleth Map")
        
        
        def load_data():
            return pd.DataFrame(unified_df)  # change if needed
        
        df = load_data()
        
        # -------------------------------
        # Sidebar filters
        # -------------------------------
       
        col1,col2,col3=st.columns(3)
        with col1:
        
            metric = st.selectbox(
                "Select Metric",
                sorted(df["Metric_Name"].unique())
            )

        with col2:
            year = st.selectbox(
                "Select Year",
                sorted(df["Year"].unique())
            )

        with col3:
            quarter = st.selectbox(
                "Select Quarter",
                sorted(df["Quarter"].unique())
            )
        
            # -------------------------------
            # Filter data
            # -------------------------------
        df_filtered = df[
            (df["Metric_Name"] == metric) &
            (df["Year"] == year) &
            (df["Quarter"] == quarter)
        ]
        
        # -------------------------------
        # Choropleth Map
        # -------------------------------
        fig = px.choropleth(
            df_filtered,
            geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
            featureidkey="properties.ST_NM",
            locations="State",
            color="Metric_Value",
            color_continuous_scale="Reds",
            hover_name="State",
            hover_data={"Metric_Value":":,.0f"},
            title=f"{metric.replace('_',' ').title()} – {year} Q{quarter}"
        )
        
        fig.update_geos(fitbounds="locations", visible=False)
        fig.update_layout(height=650)
        fig.update_traces(marker_line_width=0.5)
        fig.update_coloraxes(cmin=0)
        
        st.plotly_chart(fig, use_container_width=True)
    

        # map for district level

       
            
        st.subheader("🗺️ District Wise Choropleth Map")
        
        col1, col2, col3, col4 = st.columns(4)
       
        with col1:
            state_sel = st.selectbox(
                "State",
                sorted(unified_district_df["State"].unique())
            )
        
        with col2:
            metric_sel = st.selectbox(
                "Metric",
                sorted(unified_district_df["Metric_Name"].unique())
            )
        
        with col3:
            year_sel = st.selectbox(
                "Year",
                sorted(unified_district_df["Year"].unique())
            )
        
        with col4:
            quarter_sel = st.selectbox(
                "Quarter",
                sorted(unified_district_df["Quarter"].unique())
            )
    
        
        
        with open(r"C:\Users\MARY PRETHI\india_district.geojson.txt","r",encoding="utf-8") as f:geo = json.load(f)


        
        geo["features"] = [f for f in geo["features"]if normalize_state_geo(f["properties"]["NAME_1"])== normalize_state_geo(state_sel)]



        
        
        for f in geo["features"]:
            f["properties"]["district_norm"] = normalize_district(f["properties"]["NAME_2"])
    

        df_dist = unified_district_df[(unified_district_df["State"] == state_sel) &(unified_district_df["Metric_Name"] == metric_sel) &(unified_district_df["Year"] == year_sel) &(unified_district_df["Quarter"] == quarter_sel)].copy()


    
        df_dist["district_norm"] = df_dist["District"].apply(normalize_district)


       
        

        df_dist_agg = (df_dist.groupby("district_norm", as_index=False)["Metric_Value"].sum())


        geo_base = pd.DataFrame({"district_norm": [ f["properties"]["district_norm"] for f in geo["features"]],"District": [ f["properties"]["NAME_2"] for f in geo["features"]]})

        geo_base["district_norm"] = geo_base["district_norm"].astype(str)
        
       

        # MERGE GEOJSON BASE WITH DATA 
        df_plot = geo_base.merge(df_dist_agg,on="district_norm",how="left")

        df_plot["Metric_Value"] = df_plot["Metric_Value"].fillna(0)


        fig_2 = px.choropleth(
            df_plot,
            geojson=geo,
            featureidkey="properties.district_norm",
            locations="district_norm",
            color="Metric_Value",
            color_continuous_scale="Blues",
            hover_name="District",
            title=f"{state_sel} – District Wise {metric_sel}"
        )
        
        fig_2.update_geos(
            fitbounds="locations",
            visible=False,
            showcountries=False
        )
        fig_2.update_traces(marker_line_width=0.5)
        fig_2.update_coloraxes(cmin=0)

        
        fig_2.update_layout(
            height=700,
            margin={"r":0,"t":50,"l":0,"b":0}
        )
        
        st.plotly_chart(fig_2, use_container_width=True)

    
        







elif select == "DATA EXPLORATION":
    tab1,tab2,tab3=st.tabs(["Aggregated Analysis","Map Analysis","Top Analysis"])

    with tab1:
        method=st.radio("View",["Transaction Analysis","User Analysis","Insurance Analysis"])
        if method == "Transaction Analysis":
            col1,col2=st.columns(2)
            with col1:
                year = st.slider("Select The Year",
                             int(Aggregated_transaction["Year"].min()),int(Aggregated_transaction["Year"].max()))

            aggre_trans_call1=transaction_count_amount_Y(Aggregated_transaction,year)

            col1,col2=st.columns(2)
            with col1:
                state_y=st.selectbox("Select The State",aggre_trans_call1['State'].unique(),key="state_y_select")
            aggre_tran_t_type(aggre_trans_call1,state_y)

            col1,col2=st.columns(2)
            with col1:
                quarter = st.slider("Select The Quarter",
                             int(aggre_trans_call1["Quarter"].min()),int(aggre_trans_call1["Quarter"].max())) 
            aggre_trans_call1_Q=transaction_count_amount_Y_Q(aggre_trans_call1,year, quarter)

            col1,col2=st.columns(2)
            with col1:
                state_q=st.selectbox("Select The State",aggre_trans_call1_Q['State'].unique(),key="state_q_select")
            aggre_tran_t_type(aggre_trans_call1_Q,state_q)

        elif method =="User Analysis":
            col1,col2=st.columns(2)
            with col1:
                year1 = st.slider("Select The Year",
                             int(Aggregated_user["Year"].min()),int(Aggregated_user["Year"].max()),key="year1_select")

            Agg_user_year=Agg_user1(Aggregated_user,year1)
            col1,col2=st.columns(2)
            with col1:
                quarter = st.slider("Select The Quarter",
                             int(Agg_user_year["Quarter"].min()),int(Agg_user_year["Quarter"].max())) 
            Agg_user_year_Q=Agg_user2(Agg_user_year, quarter)

            col1,col2=st.columns(2)
            with col1:
                state_y=st.selectbox("Select The State",Agg_user_year_Q['State'].unique(),key="state_y_select")
            Agg_user3(Agg_user_year_Q,state_y)


            
        elif method =="Insurance Analysis":
            col1,col2=st.columns(2)
            with col1:
                year2 = st.slider("Select The Year",
                             int(Aggregated_insurance["Year"].min()),int(Aggregated_insurance["Year"].max()),key="year2_select")

            call1=transaction_count_amount_Y(Aggregated_insurance,year2)

            col1,col2=st.columns(2)
            with col1:
                quarter = st.slider("Select The Quarter",
                             int(call1["Quarter"].min()),int(call1["Quarter"].max())) 
            transaction_count_amount_Y_Q(call1,year2, quarter)

            
    with tab2:
        method2=st.radio("View",["Map Tansaction","Map User","Map Insurance"])
        if method2 == "Map Tansaction":

            col1,col2=st.columns(2)
            with col1:
                year3 = st.slider("Select The Year",
                             int(Map_transaction["Year"].min()),int(Map_transaction["Year"].max()),key="year3_select")

            map_trans_call1=transaction_count_amount_Y(Map_transaction,year3)

            col1,col2=st.columns(2)
            with col1:
                state_y_d=st.selectbox("Select The State", map_trans_call1['State'].unique(),key="state_y_d_select")
            map_insur_dist_type( map_trans_call1,state_y_d)

            col1,col2=st.columns(2)
            with col1:
                quarter_map2 = st.slider("Select The Quarter",
                         int( map_trans_call1["Quarter"].min()),int( map_trans_call1["Quarter"].max()),key="quarter_map2_select") 
            map_insu_call1_Q=transaction_count_amount_Y_Q( map_trans_call1,year3,  quarter_map2)

            col1,col2=st.columns(2)
            with col1:
                state_q_m=st.selectbox("Select The State",  map_insu_call1_Q['State'].unique(),key="state_q_m_select")
            map_insur_dist_type( map_insu_call1_Q,state_q_m)

        elif method2 == "Map User":
            col1,col2=st.columns(2)
            with col1:
                year4 = st.slider("Select The Year",
                             int(Map_user["Year"].min()),int(Map_user["Year"].max()),key="year4_select")

            map_user_call1=map_user_p1(Map_user,year4)

            col1,col2=st.columns(2)
            with col1:
                quarter_map3 = st.slider("Select The Quarter",
                         int( map_user_call1["Quarter"].min()),int( map_user_call1["Quarter"].max()),key="quarter_map2_select") 
            map_user_call1_Q=map_user_p2( map_user_call1, quarter_map3)

            col1,col2=st.columns(2)
            with col1:
                state_q_md=st.selectbox("Select The State",  map_user_call1_Q['State'].unique(),key="state_q_md_select")
            map_user_p3( map_user_call1_Q,state_q_md)


        elif method2 == "Map Insurance":
             col1,col2=st.columns(2)
             with col1:
                 year5 = st.slider("Select The Year",
                             int(Map_insurance["Year"].min()),int(Map_insurance["Year"].max()),key="year5_select")

             map_insu_call1=transaction_count_amount_Y(Map_insurance,year5)

             col1,col2=st.columns(2)
             with col1:
                 state_y_d=st.selectbox("Select The State", map_insu_call1['State'].unique(),key="state_y_d_select")
             map_insur_dist_type(map_insu_call1,state_y_d)

             col1,col2=st.columns(2)
             with col1:
                 quarter_map1 = st.slider("Select The Quarter",
                             int(map_insu_call1["Quarter"].min()),int(map_insu_call1["Quarter"].max())) 
             map_insu_call1_Q=transaction_count_amount_Y_Q(map_insu_call1, year5, quarter_map1)

             col1,col2=st.columns(2)
             with col1:
                 state_q_m=st.selectbox("Select The State",map_insu_call1_Q['State'].unique(),key="state_q_select")
             map_insur_dist_type(map_insu_call1_Q,state_q_m)


    with tab3:
        method3=st.radio("View",["Top Tansaction","Top User","Top Insurance"])
        if method3 == "Top Tansaction":
            col1,col2=st.columns(2)
            with col1:
                year7 = st.slider("Select The Year",
                             int(Top_transaction["Year"].min()),int(Top_transaction["Year"].max()),key="year7_select")

            top_trans_call1=transaction_count_amount_Y(Top_transaction,year7)

            col1,col2=st.columns(2)
            with col1:
                state_t_y=st.selectbox("Select The State", top_trans_call1['State'].unique(),key="state_t_y_select")
            top_insur_p1( top_trans_call1,state_t_y)

            col1,col2=st.columns(2)
            with col1:
                quarter_trans1 = st.slider("Select The Quarter",
                         int( top_trans_call1["Quarter"].min()),int( top_trans_call1["Quarter"].max()),key="quarter_trans1_select") 
            top_trans_call1_Q=transaction_count_amount_Y_Q(top_trans_call1,year7, quarter_trans1)

        elif method3 == "Top User":
            col1,col2=st.columns(2)
            with col1:
                year8 = st.slider("Select The Year",
                             int(Top_user["Year"].min()),int(Top_user["Year"].max()),key="year8_select")

            top_user_call1=top_user_p1(Top_user,year8)

            col1,col2=st.columns(2)
            with col1:
                state_top_y=st.selectbox("Select The State", top_user_call1['State'].unique(),key="state_top_y_select")
            top_user_y_state( top_user_call1,state_top_y)

        elif method3 == "Top Insurance":
            col1,col2=st.columns(2)
            with col1:
                year6 = st.slider("Select The Year",
                             int(Top_insurance["Year"].min()),int(Top_insurance["Year"].max()),key="year6_select")

            top_insur_call1=transaction_count_amount_Y(Top_insurance,year6)

            col1,col2=st.columns(2)
            with col1:
                state_ty=st.selectbox("Select The State", top_insur_call1['State'].unique(),key="state_ty_select")
            top_insur_p1( top_insur_call1,state_ty)

            col1,col2=st.columns(2)
            with col1:
                quarter_insur1 = st.slider("Select The Quarter",
                         int( top_insur_call1["Quarter"].min()),int( top_insur_call1["Quarter"].max()),key="quarter_insur1_select") 
            top_insur_call1_Q=transaction_count_amount_Y_Q(top_insur_call1,year6, quarter_insur1)


            


elif select == "TOP CHARTS":
    question=st.selectbox("Select the Question",["1.Transaction Amount and Count of Aggregated Transaction",
                                                "2.Transaction Amount and Count of Map Transaction",
                                                "3.Transaction Amount and Count of Top Transaction",
                                                "4.Transaction Amount and Count of Aggregated Insurance",
                                                "5.Transaction Amount and Count of Map Insurance",
                                                "6.Transaction Amount and Count of Top Insurance",
                                                "7.Transaction Count of Aggregated User",
                                                "8.Registered Users of Map User",
                                                "9.Registered Users of Top User",
                                                "10.App opens of Map User"])
    if question == "1.Transaction Amount and Count of Aggregated Transaction":
        
        st.subheader("TRANSACTION AMOUNT")
        top_chart_transaction_amount("Aggregated_transaction")
        st.subheader("TRANSACTION COUNT")
        top_chart_transaction_count("Aggregated_transaction")
    elif question == "2.Transaction Amount and Count of Map Transaction":
        
        st.subheader("TRANSACTION AMOUNT")
        top_chart_transaction_amount("Map_transaction")
        st.subheader("TRANSACTION COUNT")
        top_chart_transaction_count("Map_transaction")
    elif question == "3.Transaction Amount and Count of Top Transaction":
        
        st.subheader("TRANSACTION AMOUNT")
        top_chart_transaction_amount("Top_transaction")
        st.subheader("TRANSACTION COUNT")
        top_chart_transaction_count("Top_transaction")
    elif question == "4.Transaction Amount and Count of Aggregated Insurance":
        
        st.subheader("TRANSACTION AMOUNT")
        top_chart_transaction_amount("Aggregated_insurance")
        st.subheader("TRANSACTION COUNT")
        top_chart_transaction_count("Aggregated_insurance")
    elif question == "5.Transaction Amount and Count of Map Insurance":
        
        st.subheader("TRANSACTION AMOUNT")
        top_chart_transaction_amount("Map_insurance")
        st.subheader("TRANSACTION COUNT")
        top_chart_transaction_count("Map_insurance")
    elif question == "6.Transaction Amount and Count of Top Insurance":
        
        st.subheader("TRANSACTION AMOUNT")
        top_chart_transaction_amount("Top_insurance")
        st.subheader("TRANSACTION COUNT")
        top_chart_transaction_count("Top_insurance")
    elif question == "7.Transaction Count of Aggregated User":
        
        st.subheader("TRANSACTION COUNT")
        top_chart_transaction_count_user("Aggregated_user")

    elif question == "8.Registered Users of Map User":

        STATES=st.selectbox("Select the State",Map_user['State'].unique())
        st.subheader(" MAP REGISTERED USERS ")
        top_chart_registered_user("Map_user",STATES)
    
    elif question == "9.Registered Users of Top User":

        st.subheader("TOP REGISTERED USERS ")
        top_chart_REGISTERED_USER("Top_user")
        
    elif question == "10.App opens of Map User":

        STATES1=st.selectbox("Select the State",Map_user['State'].unique())
        st.subheader("APPOPENS ")
        top_chart_appopens('Map_user', STATES1)




        
