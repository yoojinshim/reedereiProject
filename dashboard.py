import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime

# 1. Page Configuration
st.set_page_config(page_title="Chartering Manager Operations Dashboard", layout="wide")

# 2. Database Connection Function
def get_data(query):
    # Ensure the path matches your project structure
    conn = duckdb.connect('output/reederei_mart.duckdb', config={"access_mode": "READ_ONLY"})
    df = conn.execute(query).df()
    conn.close()
    return df

# --- Pre-load Dimensions for Sidebar Filters ---
vessel_df = get_data("SELECT DISTINCT vessel_name FROM Vessel")
cargo_df = get_data("SELECT DISTINCT cargo_grade FROM Cargo")
charterer_df = get_data("SELECT DISTINCT charterer_name FROM Charterer")
port_df = get_data("SELECT DISTINCT port_name FROM Port")

# --- Sidebar Filters ---
st.sidebar.header("Filter")
with st.sidebar.form("filter_form"):
    # A. Date Range (CP Date)
    date_range = st.sidebar.date_input(
        "1. CP Date Range",
        value=[datetime(2022, 1, 1), datetime(2024, 12, 31)]
    )

    # B. Collapsible Multiselects (Clean Dropdown Format)
    with st.expander("Filter by Vessel", expanded=False):
        vessel_list = get_data("SELECT DISTINCT vessel_name FROM Vessel ORDER BY vessel_name")['vessel_name'].tolist()
        container = st.container()
        all_vessels = st.checkbox("Select All Vessels")
        if all_vessels:
            sel_vessels = container.multiselect(
                "Vessels", 
                vessel_list, 
                default=vessel_list 
            )
        else:
            sel_vessels = container.multiselect(
                "Vessels", 
                vessel_list
            )

    with st.expander("Filter by Cargo", expanded=False):
        cargo_list = get_data("SELECT DISTINCT cargo_grade FROM Cargo ORDER BY cargo_grade")['cargo_grade'].tolist()
        container = st.container()
        all_cargo = st.checkbox("Select All Cargos")
        if all_vessels:
            sel_cargos = container.multiselect(
                "Cargos", 
                cargo_list, 
                default=cargo_list 
            )
        else:
            sel_cargos = container.multiselect(
                "Cargos", 
                cargo_list
            )

    with st.expander("Filter by Charterers", expanded=False):
        charterer_list = get_data("SELECT DISTINCT charterer_name FROM Charterer ORDER BY charterer_name")['charterer_name'].tolist()
        container = st.container()
        all_charterer = st.checkbox("Select All Charterer")
        if all_vessels:
            sel_charterers = container.multiselect(
                "Charterers", 
                charterer_list, 
                default=charterer_list 
            )
        else:
            sel_charterers = container.multiselect(
                "Charterers", 
                charterer_list
            )

    with st.expander("Filter by Ports", expanded=False):    
        port_list = get_data("SELECT DISTINCT port_name FROM Port ORDER BY port_name")['port_name'].tolist()
        container = st.container()
        all_port_l = st.checkbox("Select All Load Ports")
        if all_port_l:
            sel_load_ports = container.multiselect(
                "Load Ports", 
                port_list, 
                default=port_list 
            )
        else:
            sel_load_ports = container.multiselect(
                "Load Ports", 
                port_list
            )
        
        container = st.container()
        all_port_d = st.checkbox("Select All Discharge Ports")
        if all_port_d:
            sel_disc_ports = container.multiselect(
                "Discharge Ports", 
                port_list, 
                default=port_list 
            )
        else:
            sel_disc_ports = container.multiselect(
                "Discharge Ports", 
                port_list
            )

    # C. TCE Range with Manual Input and Slidebar
    col_min, col_max = st.sidebar.columns(2)
    with col_min:
        min_input = st.number_input("Min TCE (USD/Day)", value=-100000, step=1000)
    with col_max:
        max_input = st.number_input("Max TCE (USD/Day)", value=300000, step=1000)

    submitted = st.form_submit_button("Apply")

# Helper to format SQL lists
def to_sql_list(lst):
    return "('" + "','".join(lst) + "')" if lst else "('')"

# Fallback: if user clears everything, treat as "Select All" to avoid crash
if not sel_vessels: sel_vessels = vessel_list
if not sel_cargos: sel_cargos = cargo_list
if not sel_charterers: sel_charterers = charterer_list
if not sel_load_ports: sel_load_ports = port_list
if not sel_disc_ports: sel_disc_ports = port_list

# --- Data Fetching Logic ---
if len(date_range) == 2:
    start_date, end_date = date_range
    
    main_query = f"""
        SELECT 
            v.*, 
            vs.vessel_name,
            ch.charterer_name,
            p_load.port_name as load_port,
            p_disc.port_name as disc_port,
            c.cargo_grade
        FROM Voyage_P_L v
        JOIN Vessel vs ON v.IMO_NUMBER = vs.imo_number
        JOIN Voyage_Leg vl_l ON v.VOYAGE_ID_T || '-L' = vl_l.voyage_id
        JOIN Cargo c ON vl_l.cargo_id = c.cargo_id
        JOIN Charterer ch ON vl_l.charterer_id = ch.charterer_id
        JOIN Port p_load ON vl_l.origin_port_id = p_load.port_id
        JOIN Port p_disc ON vl_l.destination_port_id = p_disc.port_id
        WHERE v.CP_DATE BETWEEN '{start_date}' AND '{end_date}'
          AND vs.vessel_name IN {to_sql_list(sel_vessels)}
          AND c.cargo_grade IN {to_sql_list(sel_cargos)}
          AND ch.charterer_name IN {to_sql_list(sel_charterers)}
          AND p_load.port_name IN {to_sql_list(sel_load_ports)}
          AND p_disc.port_name IN {to_sql_list(sel_disc_ports)}
          AND v.TCE_USD BETWEEN {min_input} AND {max_input}
    """
    df = get_data(main_query)
    df['CP_DATE'] = pd.to_datetime(df['CP_DATE'])
else:
    st.info("Please select a complete start and end date in the sidebar.")
    st.stop()

if df.empty:
    st.warning("⚠️ No data matches the current filters.")
    st.stop()

# --- Dashboard Layout ---
st.title("🚢 Fleet Performance Analytics")

# 1. Fleet TCE Trend Over Time
st.subheader("1. Fleet TCE Trend Over Time")
granularity = st.radio("Time Granularity:", ["Daily", "Monthly", "Yearly"], horizontal=True)

if granularity == "Daily":
    if (end_date - start_date).days > 100:
        st.error("Daily view is restricted to a 100-day range. Please shorten the range.")
        trend_df = pd.DataFrame()
    else:
        trend_df = df.groupby(df['CP_DATE'])['TCE_USD'].mean().reset_index()
elif granularity == "Monthly":
    trend_df = df.groupby(df['CP_DATE'].dt.to_period('M').astype(str))['TCE_USD'].mean().reset_index()
else:
    trend_df = df.groupby(df['CP_DATE'].dt.to_period('Y').astype(str))['TCE_USD'].mean().reset_index()

if not trend_df.empty:
    fig1 = px.line(trend_df, x=trend_df.columns[0], y='TCE_USD', markers=True, 
                   labels={'TCE_USD': 'TCE (USD/Day)', 'index': 'Date'})
    st.plotly_chart(fig1, use_container_width=True)

st.markdown("---")
col1, col2 = st.columns(2)

# 2. Per Vessel P&L

st.subheader("2. Per Vessel P&L Analysis")
# Fetch voyage-level demurrage totals
v_ids = to_sql_list(df['VOYAGE_ID_T'].tolist())
dem_sql = f"SELECT LEFT(voyage_id,7) as vid, SUM(demurrage_cost_usd) as sum_dem FROM Voyage_Leg WHERE vid IN {v_ids} GROUP BY 1"
dem_lookup = get_data(dem_sql)
pnl_df = df.merge(dem_lookup, left_on='VOYAGE_ID_T', right_on='vid', how='left')
    
# Financial Logic: Freight (+), Demurrage Outcome (+/-), Other Costs (-)
pnl_df['Gross Freight'] = pnl_df['GROSS_FREIGHT_REVENUE_USD']
pnl_df['Demurrage Outcome'] = pnl_df['sum_dem'] * -1  
pnl_df['Other Costs'] = (pnl_df['TOTAL_VOYAGE_COST_USD'] - pnl_df['sum_dem']) * -1
    
res_df = pnl_df.groupby('vessel_name')[['Gross Freight', 'Demurrage Outcome', 'Other Costs']].sum().reset_index()
fig2 = go.Figure(data=[
    go.Bar(name='Gross Freight', x=res_df['vessel_name'], y=res_df['Gross Freight'], marker_color='#2ca02c'),
    go.Bar(name='Demurrage Outcome', x=res_df['vessel_name'], y=res_df['Demurrage Outcome'], marker_color='#ff7f0e'),
    go.Bar(name='Other Costs', x=res_df['vessel_name'], y=res_df['Other Costs'], marker_color='#d62728')
])
fig2.update_layout(barmode='group', yaxis_title="USD", xaxis_title="Vessel Name")
st.plotly_chart(fig2, use_container_width=True)

# 3. TCE by Cargo Grade

st.subheader("3. TCE by Cargo Grade")
cargo_tce = df.groupby('cargo_grade')['TCE_USD'].sum().reset_index()
fig3 = px.pie(cargo_tce, values='TCE_USD', names='cargo_grade', hole=0.4)
st.plotly_chart(fig3, use_container_width=True)


# 4. Top 10 Routes by Net Voyage Result
st.subheader("4. Top 10 Routes by Net Result")
df['Route'] = df['load_port'] + " to " + df['disc_port']
route_df = df.groupby('Route')['NET_VOYAGE_RESULT_USD'].sum().sort_values(ascending=False).head(10).reset_index()
fig4 = px.bar(route_df, x='NET_VOYAGE_RESULT_USD', y='Route', orientation='h', 
                color='NET_VOYAGE_RESULT_USD', color_continuous_scale='Blues')
fig4.update_layout(yaxis={'categoryorder':'total ascending'}, xaxis_title="Net Result (USD)")
st.plotly_chart(fig4, use_container_width=True)

# 5. Demurrage Exposure by Charterer
st.subheader("5. Demurrage Exposure by Charterer")
char_dem = pnl_df.groupby('charterer_name')['sum_dem'].sum().reset_index()
char_dem['Exposure'] = char_dem['sum_dem'] * -1
fig5 = px.bar(char_dem, x='charterer_name', y='Exposure', 
                labels={'Exposure': 'Net Demurrage (USD)', 'charterer_name': 'Charterer'})
st.plotly_chart(fig5, use_container_width=True)


# 6. Bunker Cost Breakdown by Grade & Scrubber
st.subheader("6. Bunker Cost Breakdown by Scrubber Status")
df['Scrubber_Status'] = df['SCRUBBER_FLAG'].map({True: 'Scrubber (HSFO/MDO)', False: 'Standard (VLSFO/MDO)'})
bunker_sum = df.groupby(['Scrubber_Status', 'BUNKER_GRADE'])['TOTAL_VOYAGE_COST_USD'].sum().reset_index()
fig6 = px.bar(bunker_sum, x='Scrubber_Status', y='TOTAL_VOYAGE_COST_USD', color='BUNKER_GRADE', 
              barmode='group', labels={'TOTAL_VOYAGE_COST_USD': 'Total Fuel Cost (USD)'})
st.plotly_chart(fig6, use_container_width=True)

# 7. Market Context Panel: Fleet Avg WS vs Flat Rate
st.subheader("7. Market Context: Fleet Avg WS vs Monthly Flat Rate")
mkt_df = df.groupby(df['CP_DATE'].dt.to_period('M').astype(str)).agg({
    'WS_POINTS': 'mean',
    'FLAT_RATE_USD_PER_MT': 'mean'
}).reset_index()

fig7 = go.Figure()
fig7.add_trace(go.Scatter(x=mkt_df['CP_DATE'], y=mkt_df['WS_POINTS'], name='Fleet Avg WS', yaxis='y1', line=dict(width=3, color='royalblue')))
fig7.add_trace(go.Scatter(x=mkt_df['CP_DATE'], y=mkt_df['FLAT_RATE_USD_PER_MT'], name='Market Flat Rate', yaxis='y2', line=dict(dash='dash', color='firebrick')))

fig7.update_layout(
    xaxis=dict(title="Month (CP Date)"),
    yaxis=dict(title="Worldscale Points", titlefont=dict(color="royalblue"), tickfont=dict(color="royalblue")),
    yaxis2=dict(title="Flat Rate (USD/MT)", titlefont=dict(color="firebrick"), tickfont=dict(color="firebrick"), overlaying='y', side='right'),
    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
)
st.plotly_chart(fig7, use_container_width=True)

# --- Data dictionary ( Chartering Manager ) ---
with st.expander("📖 Data Dictionary & Methodology"):
    st.markdown("""
    - **TCE (Time Charter Equivalent)**: `(Gross Revenue - Voyage Costs) / Total Days`. Standardized industry daily earnings.
    - **Demurrage Net Position**: Net financial impact of port stays. Positive indicates income.
    - **Scrubber Flag**: Highlighted to show HSFO burn efficiency vs VLSFO costs.
    """)