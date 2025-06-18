import streamlit as st
import pandas as pd
import numpy as np
from google.cloud import bigquery
import datetime
import altair as alt

#region config

st.set_page_config(
    page_title="London Bicycle Hires Analysis",
    page_icon="🚲",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# CSS for formatting
st.markdown("""
<style>
    .header {
        font-size: 2.5rem;
        font-weight: bold;
        margin-bottom: 1rem;
        color: #f23f3f;
    }
    .subheader {
        font-size: 1.8rem;
        font-weight: bold;
        margin-top: 2rem;
        margin-bottom: 1rem;
        color: #000c7c;
    }
    .section-header {
        font-size: 1.4rem;
        font-weight: bold;
        margin-top: 1.5rem;
        margin-bottom: 0.8rem;
        color: #e26363;
    }
    .insight-box {
        background-color: #E3F2FD;
        padding: 1rem;
        border-radius: 0.5rem;
        margin-bottom: 1rem;
        border-left: 5px solid #1E88E5;
    }
    .recommendation-box {
        background-color: #E8F5E9;
        padding: 1rem;
        border-radius: 0.5rem;
        margin-bottom: 1rem;
        border-left: 5px solid #43A047;
    }
    .problem-box {
        background-color: #FFEBEE;
        padding: 1rem;
        border-radius: 0.5rem;
        margin-bottom: 1rem;
        border-left: 5px solid #E53935;
    }
    .capacity-normal {
        color: #43A047;
    }
    .capacity-warning {
        color: #FB8C00;
    }
    .capacity-critical {
        color: #E53935;
    }
</style>
""", unsafe_allow_html=True)

# Title and summary
st.markdown('<div class="header">London Bicycle Hires Analysis - Capacity Optimisation</div>', unsafe_allow_html=True)

# BigQuery client 
@st.cache_resource
def get_bigquery_client():
    return bigquery.Client()

@st.cache_data(ttl=3600)
def run_query(query):
    """Run a BigQuery query and return results as a DataFrame"""
    try:
        client = get_bigquery_client()
        query_job = client.query(query)
        return query_job.to_dataframe()
    except Exception as e:
        st.error(f"Error executing query: {e}")
        return pd.DataFrame()

# Min/max definitions based on actual dataset span
MIN_DATE = datetime.date(2015, 1, 4)
MAX_DATE = datetime.date(2023, 1, 15)

# Sidebar for date selection
with st.sidebar:
    st.header("Analysis Parameters")
    st.write(f"Dataset spans from {MIN_DATE} to {MAX_DATE}")
    
    # Default to showing one year of data (or less if dataset is smaller)
    default_end_date = datetime.date(2016, 12, 31)  # Using a known good date with data
    default_start_date = datetime.date(2016, 1, 1)  # One year of data
    
    start_date = st.date_input(
        "Start date:",
        value=default_start_date,
        min_value=MIN_DATE,
        max_value=MAX_DATE
    )
    
    end_date = st.date_input(
        "End date:",
        value=default_end_date,
        min_value=start_date,
        max_value=MAX_DATE
    )
    
    # Dropdown for time interval for insights
    interval_options = ['Daily', 'Weekly', 'Monthly', 'Quarterly', 'Annual']
    selected_interval = st.selectbox("Analysis interval for insights:", interval_options, index=1)
    
    interval_days = {
        'Daily': 1,
        'Weekly': 7,
        'Monthly': 30,
        'Quarterly': 90,
        'Annual': 365
    }
    
    interval_days_count = interval_days[selected_interval]
    
    total_days = (end_date - start_date).days
    num_intervals = max(1, total_days // interval_days_count)
    
    st.write(f"Selected period contains approximately {num_intervals} {selected_interval.lower()} intervals")

# Date to string conversions for SQL queries
start_date_str = start_date.strftime("%Y-%m-%d")
end_date_str = end_date.strftime("%Y-%m-%d")

# Container for main content
main_container = st.container()

with main_container:
    #region 1: Problem Statement
    st.markdown('<div class="subheader">1. Declining Rides, Rising Complaints...</div>', unsafe_allow_html=True)
    
    col1, col2 = st.columns([3, 2])
    
    with col1:
        st.markdown('<div class="problem-box">Since 2022, the Santander Cycle Hire Scheme has faced a decline in usage, and voices of frustration are rife on social media. <b>What problems need to be addressed by the scheme to rebound its popularity?</b></div>', unsafe_allow_html=True)
        st.markdown("""
        1. **Station Capacity Issues**: Imbalanced stock and capacity issues often leave some stations full or empty, especially during peak hours, making it hard for users to find or return bikes when and where they need them.
        
        2. **Insufficient station coverage**: Lack of stations and locational spread in high-demand areas and outer zones limits accessibility, particularly in growing residential and commercial districts beyond Central London. 
            This also places a greater burden on certain stations in highly connected areas that are frequented by commuters and visitors from further out.
                    
        3. **Growing competition**: Most notably in recent years, dockless bicycle services like Lime offer users more flexibility and convenience, drawing riders away from Santander cycles due to fewer geographic and parking restrictions.
        """)
    
    with col2:
        st.markdown("""
        **Impact of Capacity Issues:**
        
        - Reduced user satisfaction and retention as inconsistent availability and rigid docking requirements lead to frustration and abandonment of the service.
        - Loss of market share to more flexible competitors like Lime, which better meet user expectations for convenience and geographic reach.
        - Underutilised infrastructure and inefficiencies, where some stations are overburdened while others are underused, leading to operational challenges and missed revenue opportunities.
        """)

    st.markdown('<br>', unsafe_allow_html=True)
    st.markdown('<div class="recommendation-box"><h6>This analysis aims to examine bicycle hire data from the Santander Cycle Hire Scheme (Greater London Authority), and use its results ' \
    'to develop strategies for optimising bicycle capacity and rebalancing stock where needed to resolve frequent issues.</h6>By analysing historical usage patterns (this dataset spans from 2015 to 2023), ' \
    'we can recommend targeted solutions to improve system efficiency, user experience, and subsequently, the success of the scheme.</div>', unsafe_allow_html=True)

    #endregion

    #region 2: Data Analysis - Capacity Issues
    st.markdown('<div class="subheader">2. Identifying Capacity Hotspots</div>', unsafe_allow_html=True)
    
    # Query to find stations with capacity issues and peak times
    full_stations_query = f"""
    -- Updated query to include day of week for peak times
    WITH hourly_station_status AS (
        SELECT 
            h.end_station_id,
            s.name AS station_name,
            s.docks_count AS total_docks,
            EXTRACT(DATE FROM h.end_date) AS date,
            EXTRACT(HOUR FROM h.end_date) AS hour,
            EXTRACT(DAYOFWEEK FROM h.end_date) AS day_of_week,
            COUNT(*) AS arrivals_in_hour,
            COUNT(*) / s.docks_count * 100 AS utilisation_pct
        FROM 
            `bigquery-public-data.london_bicycles.cycle_hire` AS h
        JOIN 
            `bigquery-public-data.london_bicycles.cycle_stations` AS s
        ON 
            h.end_station_id = s.id
        WHERE 
            h.end_date BETWEEN TIMESTAMP('{start_date_str}') AND TIMESTAMP('{end_date_str}')
            AND s.docks_count > 0  -- Ensure no division by zero
        GROUP BY 
            h.end_station_id, s.name, s.docks_count, date, hour, day_of_week
    ),
    peak_hours AS (
        SELECT
            station_name,
            hour,
            day_of_week,
            COUNT(*) AS num_occurrences,
            AVG(utilisation_pct) AS avg_util_at_hour
        FROM
            hourly_station_status
        WHERE
            utilisation_pct >= 95
        GROUP BY
            station_name, hour, day_of_week
        QUALIFY ROW_NUMBER() OVER (PARTITION BY station_name ORDER BY num_occurrences DESC, avg_util_at_hour DESC) = 1
    )
    SELECT 
        s.station_name,
        s.total_docks,
        s.instances_near_capacity,
        s.instances_at_capacity,
        s.avg_hourly_arrivals,
        s.avg_utilisation_pct,
        s.max_hourly_arrivals,
        s.max_utilisation_pct,
        p.hour AS peak_hour,
        p.day_of_week AS peak_day_of_week,
        p.num_occurrences AS peak_hour_occurrences,
        p.avg_util_at_hour AS peak_hour_avg_util
    FROM 
        (
            SELECT 
                station_name,
                total_docks,
                COUNT(*) AS instances_near_capacity,
                COUNTIF(utilisation_pct >= 100) AS instances_at_capacity,
                ROUND(AVG(arrivals_in_hour), 1) AS avg_hourly_arrivals,
                ROUND(AVG(utilisation_pct), 1) AS avg_utilisation_pct,
                MAX(arrivals_in_hour) AS max_hourly_arrivals,
                ROUND(MAX(utilisation_pct), 1) AS max_utilisation_pct
            FROM 
                hourly_station_status
            WHERE
                utilisation_pct >= 80  -- At least 80% of capacity
                AND total_docks > 0  -- Extra safeguard against division by zero
            GROUP BY 
                station_name, total_docks
        ) s
    LEFT JOIN
        peak_hours p
    ON
        s.station_name = p.station_name
    ORDER BY 
        s.instances_at_capacity DESC, s.instances_near_capacity DESC
    LIMIT 10
    """
    
    with st.spinner(f"Loading capacity data for period {start_date_str} to {end_date_str}..."):
        full_stations_df = run_query(full_stations_query)

    if not full_stations_df.empty:
        st.markdown('<div class="section-header">Top 10 Most Problematic Stations</div>', unsafe_allow_html=True)
        
        # DataFrame for display with all the key information in one view
        display_data = full_stations_df.copy()
        
        # Convert day of week number to name (Sunday as day 1)
        def get_day_name(day_num):
            days = {
                1: "Sunday",
                2: "Monday",
                3: "Tuesday",
                4: "Wednesday",
                5: "Thursday",
                6: "Friday",
                7: "Saturday"
            }
            return days.get(day_num, "Unknown")
        
        # Format the peak time to include day of week
        display_data['peak_day_time'] = display_data.apply(
            lambda row: f"{get_day_name(row['peak_day_of_week'])} {int(row['peak_hour']):02d}:00-{int(row['peak_hour'])+1:02d}:00" 
                        if pd.notnull(row['peak_hour']) and pd.notnull(row['peak_day_of_week']) 
                        else "Unknown",
            axis=1
        )
        
        # Bar chart with station names and at_capacity count
        chart_data = pd.DataFrame({
            'Station': display_data['station_name'],
            'Times at Capacity': display_data['instances_at_capacity']
        })
        
        st.bar_chart(chart_data.set_index('Station'))
        st.caption("Y-Axis: Number of times at capacity (i.e., completely full at 100%+ utilisation)")
        st.caption("X-Axis: Station name")
        
        # Full summary information table
        st.subheader("Problematic Stations Summary")
        
        summary_df = display_data[['station_name', 'total_docks', 'instances_at_capacity',
                                'peak_day_time', 'avg_utilisation_pct']]
        summary_df.columns = ['Station', 'Dock Count', 'Times at Capacity', 'Peak Day/Time', 'Average Utilisation %']
        
        # Sort by Times at Capacity
        summary_df = summary_df.sort_values('Times at Capacity', ascending=False)
        
        # Display as a clean table
        st.dataframe(summary_df, use_container_width=True)
        
        # Insights with specified intervals
        st.markdown('<div class="insight-box"><b>Key Station Capacity Insights</b>', unsafe_allow_html=True)
        
        # Most problematic station
        most_at_capacity = full_stations_df.iloc[0]['station_name']
        most_at_capacity_count = full_stations_df.iloc[0]['instances_at_capacity']
        
        # peak_time from peak_hour
        most_at_capacity_peak_hour = full_stations_df.iloc[0]['peak_hour']
        if pd.notnull(most_at_capacity_peak_hour):
            most_at_capacity_peak = f"{int(most_at_capacity_peak_hour):02d}:00-{int(most_at_capacity_peak_hour)+1:02d}:00"
        else:
            most_at_capacity_peak = "Unknown"
        
        # Per interval metrics
        days_in_period = (end_date - start_date).days
        intervals_in_period = max(1, days_in_period // interval_days_count)
        
        capacity_instances_per_interval = most_at_capacity_count / intervals_in_period
        
        # Estimated impact (revenue loss)
        avg_bike_rental_fee = 1.65  # £1.65 for first 30 minutes
        lost_rentals_estimate = most_at_capacity_count * 5  # Assuming 5 lost rentals per full station instance
        revenue_impact = lost_rentals_estimate * avg_bike_rental_fee
        revenue_impact_per_interval = revenue_impact / intervals_in_period
        
        st.markdown(f"""
        - **{most_at_capacity}** is the most problematic station, reaching full capacity **{most_at_capacity_count}** times during the period from **{start_date_str}** to **{end_date_str}**.
        - This station typically reaches capacity during **{most_at_capacity_peak}**, making this the critical time for rebalancing.
        - On average, during this period it reaches capacity **{capacity_instances_per_interval:.1f}** times {selected_interval.lower()}.
        - Potential revenue impact: **At least £{revenue_impact:,.2f}** (for rides <30 minutes) in lost revenue over the entire period ({days_in_period} days).
        - The majority of high-capacity stations are in central London and transport hubs, which are frequently at capacity during commuter rush hours.
        """)
        st.markdown('</div>', unsafe_allow_html=True)
        
        # Station names for the dropdown
        station_names = full_stations_df['station_name'].tolist()
        
        #endregion

        #region 2.1: Station-specific hourly capacity 
        st.markdown('<div class="subheader">2.1 Station-Specific Hourly Capacity</div>', unsafe_allow_html=True)
        st.markdown("""
        This section displays hourly capacity patterns for individual stations. The dropdown below includes only the **top 10 most problematic stations** based on frequency of capacity issues.

        (A **capacity issue** is defined as when a station reaches at least 80% utilisation (near capacity) or exceeds 100% utilisation (at capacity)).
        """)
        
        # Station selector
        selected_station = st.selectbox("Select a station to analyze hourly capacity:", 
                                        station_names, 
                                        index=0)
        
        # Dock count for the selected station
        selected_station_docks = full_stations_df[full_stations_df['station_name'] == selected_station]['total_docks'].values[0]
        
        # Query for hourly station capacity data
        hourly_capacity_query = f"""
        WITH hourly_data AS (
            SELECT 
                h.end_station_id,
                s.name AS station_name,
                s.docks_count AS total_docks,
                EXTRACT(DATE FROM h.end_date) AS date,
                EXTRACT(HOUR FROM h.end_date) AS hour,
                EXTRACT(DAYOFWEEK FROM h.end_date) AS day_of_week,
                COUNT(*) AS arrivals_in_hour,
                COUNT(*) / NULLIF(s.docks_count, 0) * 100 AS utilisation_pct
            FROM 
                `bigquery-public-data.london_bicycles.cycle_hire` AS h
            JOIN 
                `bigquery-public-data.london_bicycles.cycle_stations` AS s
            ON 
                h.end_station_id = s.id
            WHERE 
                h.end_date BETWEEN TIMESTAMP('{start_date_str}') AND TIMESTAMP('{end_date_str}')
                AND s.name = '{selected_station}'
                AND s.docks_count > 0
            GROUP BY 
                h.end_station_id, s.name, s.docks_count, date, hour, day_of_week
        )
        SELECT 
            hour,
            CASE 
                WHEN day_of_week = 1 THEN 'Sunday'
                WHEN day_of_week = 2 THEN 'Monday'
                WHEN day_of_week = 3 THEN 'Tuesday'
                WHEN day_of_week = 4 THEN 'Wednesday'
                WHEN day_of_week = 5 THEN 'Thursday'
                WHEN day_of_week = 6 THEN 'Friday'
                WHEN day_of_week = 7 THEN 'Saturday'
            END AS day_name,
            AVG(arrivals_in_hour) AS avg_arrivals,
            AVG(utilisation_pct) AS avg_utilisation_pct,
            MAX(utilisation_pct) AS max_utilisation_pct,
            COUNTIF(utilisation_pct >= 80 AND utilisation_pct < 100) AS near_capacity_count,
            COUNTIF(utilisation_pct >= 100) AS at_capacity_count
        FROM 
            hourly_data
        GROUP BY 
            hour, day_name, day_of_week
        ORDER BY 
            day_of_week, hour
        """
        
        with st.spinner(f"Loading hourly capacity data for {selected_station}..."):
            hourly_capacity_df = run_query(hourly_capacity_query)
        
        if not hourly_capacity_df.empty:
            # Weekday/weekend split
            hourly_capacity_df['is_weekend'] = hourly_capacity_df['day_name'].apply(
                lambda x: 'Weekend' if x in ['Saturday', 'Sunday'] else 'Weekday'
            )
            
            # Capacity visualization using Altair
            weekday_data = hourly_capacity_df[hourly_capacity_df['is_weekend'] == 'Weekday'].copy()
            weekend_data = hourly_capacity_df[hourly_capacity_df['is_weekend'] == 'Weekend'].copy()
            
            # Station metrics
            avg_near_capacity = hourly_capacity_df['near_capacity_count'].sum()
            avg_at_capacity = hourly_capacity_df['at_capacity_count'].sum()
            total_capacity_issues = avg_near_capacity + avg_at_capacity
            issues_per_interval = total_capacity_issues / intervals_in_period
            
            # Station summary
            st.markdown(f"### Capacity Analysis for {selected_station} ({selected_station_docks} docks)")
            
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Times Near Capacity (80-99%)", avg_near_capacity)
            with col2:
                st.metric("Times At Capacity (100%+)", avg_at_capacity)
            with col3:
                st.metric(f"Capacity Issues per {selected_interval}", f"{issues_per_interval:.1f}")
            
            # Max utilisation percentage for proper scaling
            max_weekday_util = weekday_data['max_utilisation_pct'].max() if not weekday_data.empty else 150
            max_weekend_util = weekend_data['max_utilisation_pct'].max() if not weekend_data.empty else 150
            max_util = max(max_weekday_util, max_weekend_util, 150)  # At least 150% for visibility
            
            # Weekday visualization
            st.markdown("#### Weekday Hourly Capacity Trend")
            
            # Altair chart
            weekday_chart = alt.Chart(weekday_data).mark_line().encode(
                x=alt.X('hour:O', title='Hour of Day', axis=alt.Axis(labelAngle=0)),
                y=alt.Y('avg_utilisation_pct:Q', 
                       title='Average Utilisation %',
                       scale=alt.Scale(domain=[0, max_util * 1.1])),  # Extended scale
                tooltip=['hour:O', 'avg_utilisation_pct:Q', 'max_utilisation_pct:Q', 'near_capacity_count:Q', 'at_capacity_count:Q']
            ).properties(
                width=600,
                height=300
            )
            
            # Reference lines for capacity thresholds
            capacity_rule = alt.Chart(pd.DataFrame({'y': [100]})).mark_rule(
                color='red', strokeDash=[5, 5]
            ).encode(y='y:Q')
            
            near_capacity_rule = alt.Chart(pd.DataFrame({'y': [80]})).mark_rule(
                color='orange', strokeDash=[5, 5]
            ).encode(y='y:Q')
            
            # Colour bands for capacity zones with extended upper limit
            capacity_bands = alt.Chart(pd.DataFrame({
                'x': [0, 23],
                'y1': [0, 0],
                'y2': [80, 80],
                'y3': [100, 100],
                'y4': [max_util * 1.1, max_util * 1.1]  # Extend beyond maximum value
            })).mark_area(opacity=0.1, color='green').encode(
                x='x:O',
                y='y1:Q',
                y2='y2:Q'
            ) + alt.Chart(pd.DataFrame({
                'x': [0, 23],
                'y1': [80, 80],
                'y2': [100, 100]
            })).mark_area(opacity=0.1, color='orange').encode(
                x='x:O',
                y='y1:Q',
                y2='y2:Q'
            ) + alt.Chart(pd.DataFrame({
                'x': [0, 23],
                'y1': [100, 100],
                'y2': [max_util * 1.1, max_util * 1.1]  # Extend beyond maximum value
            })).mark_area(opacity=0.1, color='red').encode(
                x='x:O',
                y='y1:Q',
                y2='y2:Q'
            )
            
            # Weekday chart
            weekday_final_chart = (capacity_bands + weekday_chart + capacity_rule + near_capacity_rule)
            st.altair_chart(weekday_final_chart, use_container_width=True)
            
            # Weekday chart legend
            col1, col2, col3 = st.columns(3)
            with col1:
                st.markdown('<span class="capacity-normal">■</span> Normal (<80%)', unsafe_allow_html=True)
            with col2:
                st.markdown('<span class="capacity-warning">■</span> Near Capacity (80-99%)', unsafe_allow_html=True)
            with col3:
                st.markdown('<span class="capacity-critical">■</span> At Capacity (100%+)', unsafe_allow_html=True)
            
            # Weekend visualization
            st.markdown("#### Weekend Hourly Capacity Trend")
            
            # Altair chart
            weekend_chart = alt.Chart(weekend_data).mark_line().encode(
                x=alt.X('hour:O', title='Hour of Day', axis=alt.Axis(labelAngle=0)),
                y=alt.Y('avg_utilisation_pct:Q', 
                       title='Average Utilisation %',
                       scale=alt.Scale(domain=[0, max_util * 1.1])),  # Extended scale
                tooltip=['hour:O', 'avg_utilisation_pct:Q', 'max_utilisation_pct:Q', 'near_capacity_count:Q', 'at_capacity_count:Q']
            ).properties(
                width=600,
                height=300
            )
            
            # Weekend chart
            weekend_final_chart = (capacity_bands + weekend_chart + capacity_rule + near_capacity_rule)
            st.altair_chart(weekend_final_chart, use_container_width=True)
            
            # Specific station insights
            peak_hour_weekday = weekday_data.loc[weekday_data['avg_utilisation_pct'].idxmax()]
            peak_hour_weekend = weekend_data.loc[weekend_data['avg_utilisation_pct'].idxmax()]
            
            st.markdown(f"""
            **Station-Specific Insights for {selected_station}:**
            
            - **Weekday Peak Hour**: {int(peak_hour_weekday['hour']):02d}:00 with {peak_hour_weekday['avg_utilisation_pct']:.1f}% average utilisation
            - **Weekend Peak Hour**: {int(peak_hour_weekend['hour']):02d}:00 with {peak_hour_weekend['avg_utilisation_pct']:.1f}% average utilisation
            - This station has been at capacity ({peak_hour_weekday['at_capacity_count']:.0f} times) during weekday peak hours
            - The station experiences full capacity on average {avg_at_capacity/intervals_in_period:.1f} times per {selected_interval.lower()}
            - {"Recommended for capacity expansion" if avg_at_capacity > 20 else "Moderate capacity issues, monitoring recommended"}
            """)
            st.markdown('</div>', unsafe_allow_html=True)
        else:
            st.warning(f"No hourly capacity data available for {selected_station} in the selected time period.")
    else:
        st.warning("No capacity data found for the selected time period. Try adjusting the date range.")

    #endregion

    #region 3: Usage Patterns and System Imbalance Analysis section
    st.markdown('<div class="subheader">3. Usage Patterns & System Imbalance Analysis</div>', unsafe_allow_html=True)
    
    # Query for combined usage patterns and rebalancing data
    combined_analysis_query = f"""
    WITH 
    -- Critical times analysis
    peak_times AS (
        SELECT 
            EXTRACT(HOUR FROM h.end_date) AS hour_of_day,
            CASE 
                WHEN EXTRACT(DAYOFWEEK FROM h.end_date) IN (1, 7) THEN 'Weekend'
                ELSE 'Weekday'
            END AS day_type,
            COUNT(*) AS arrivals
        FROM 
            `bigquery-public-data.london_bicycles.cycle_hire` AS h
        WHERE 
            h.end_date BETWEEN TIMESTAMP('{start_date_str}') AND TIMESTAMP('{end_date_str}')
        GROUP BY 
            hour_of_day, day_type
    ),
    -- Station imbalance analysis
    station_flows AS (
        SELECT 
            s.id AS station_id,
            s.name AS station_name,
            s.docks_count AS total_docks,
            COUNT(CASE WHEN h.start_station_id = s.id THEN 1 END) AS outflows,
            COUNT(CASE WHEN h.end_station_id = s.id THEN 1 END) AS inflows,
            COUNT(CASE WHEN h.end_station_id = s.id THEN 1 END) - 
            COUNT(CASE WHEN h.start_station_id = s.id THEN 1 END) AS net_flow
        FROM 
            `bigquery-public-data.london_bicycles.cycle_stations` s
        LEFT JOIN 
            `bigquery-public-data.london_bicycles.cycle_hire` h
        ON 
            (s.id = h.start_station_id OR s.id = h.end_station_id)
            AND h.start_date BETWEEN TIMESTAMP('{start_date_str}') AND TIMESTAMP('{end_date_str}')
        WHERE
            s.docks_count > 0
        GROUP BY 
            s.id, s.name, s.docks_count
    )
    -- Return both data sets
    SELECT 
        'peak_times' AS data_type,
        CAST(hour_of_day AS STRING) AS id,
        day_type AS name,
        CAST(NULL AS INT64) AS total_docks,
        CAST(NULL AS INT64) AS outflows,
        CAST(arrivals AS INT64) AS inflows,
        CAST(NULL AS INT64) AS net_flow,
        CAST(NULL AS FLOAT64) AS imbalance_pct,
        CAST(NULL AS STRING) AS station_type
    FROM 
        peak_times
    
    UNION ALL
    
    SELECT 
        'station_flows' AS data_type,
        CAST(station_id AS STRING) AS id,
        station_name AS name,
        total_docks,
        outflows,
        inflows,
        net_flow,
        (net_flow / NULLIF(total_docks, 0)) * 100 AS imbalance_pct,
        CASE 
            WHEN net_flow > 0 THEN 'Accumulator (Fills Up)'
            WHEN net_flow < 0 THEN 'Generator (Empties Out)'
            ELSE 'Balanced'
        END AS station_type
    FROM 
        station_flows
    WHERE 
        (outflows > 0 OR inflows > 0)
        AND ABS(net_flow) > 20
    """
    
    with st.spinner("Loading combined analysis data..."):
        combined_data = run_query(combined_analysis_query)
    
    if not combined_data.empty:
        # Split into peak times and station flows
        peak_times_df = combined_data[combined_data['data_type'] == 'peak_times'].copy()
        station_flows_df = combined_data[combined_data['data_type'] == 'station_flows'].copy()
        
        # Layout with two columns
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown('<div class="section-header">Critical Times for Rebalancing</div>', unsafe_allow_html=True)
            
            if not peak_times_df.empty:
                # Convert hour_of_day to integer for sorting
                peak_times_df['hour'] = peak_times_df['id'].astype(int)
                
                # Weekday and weekend data
                weekday_data = peak_times_df[peak_times_df['name'] == 'Weekday'].sort_values('hour')
                weekend_data = peak_times_df[peak_times_df['name'] == 'Weekend'].sort_values('hour')
                
                # Top 3 peak hours for weekdays and weekends
                top_weekday_hours = weekday_data.nlargest(3, 'inflows')
                top_weekend_hours = weekend_data.nlargest(3, 'inflows')
                
                weekday_data['day_type'] = 'Weekday'
                weekend_data['day_type'] = 'Weekend'
                combined_hours = pd.concat([weekday_data, weekend_data])
                
                hour_chart = alt.Chart(combined_hours).mark_line().encode(
                    x=alt.X('hour:O', title='Hour of Day'),
                    y=alt.Y('inflows:Q', title='Arrivals'),
                    color=alt.Color('day_type:N', title='Day Type')
                ).properties(
                    width=400,
                    height=300
                )
                
                # Points for peak hours
                top_hours = pd.concat([top_weekday_hours, top_weekend_hours])
                top_hours['day_type'] = top_hours['name']
                
                peak_points = alt.Chart(top_hours).mark_circle(
                    size=100,
                    color='red'
                ).encode(
                    x='hour:O',
                    y='inflows:Q',
                    tooltip=['day_type', 'hour:O', 'inflows:Q']
                )
                
                # Line and points
                final_chart = hour_chart + peak_points
                
                # Chart display
                st.altair_chart(final_chart, use_container_width=True)
                
                # Key insights about critical times
                st.markdown('<div class="insight-box"><b>Critical Rebalancing Times</b>', unsafe_allow_html=True)
                st.markdown(f"""
                **Weekday Peaks:**
                {', '.join([f"{int(hour):02d}:00" for hour in top_weekday_hours['hour']])}
                
                **Weekend Peaks:**
                {', '.join([f"{int(hour):02d}:00" for hour in top_weekend_hours['hour']])}
                """)
                st.markdown('</div>', unsafe_allow_html=True)
            else:
                st.info("No peak time data available for the selected period.")
        
        with col2:
            st.markdown('<div class="section-header">System Imbalance</div>', unsafe_allow_html=True)
            
            if not station_flows_df.empty:
                # Split into generators and accumulators
                generators_df = station_flows_df[station_flows_df['net_flow'] < 0].sort_values('net_flow')
                accumulators_df = station_flows_df[station_flows_df['net_flow'] > 0].sort_values('net_flow', ascending=False)
                
                # Top 5 generators and accumulators
                top_generators = generators_df.head(5).copy()
                top_accumulators = accumulators_df.head(5).copy()
                
                # Net flow to absolute value
                top_generators['abs_flow'] = top_generators['net_flow'].abs()
                top_generators['station_type'] = 'Generator (Needs Bikes)'
                
                top_accumulators['abs_flow'] = top_accumulators['net_flow']
                top_accumulators['station_type'] = 'Accumulator (Excess Bikes)'
                
                # Combine datasets
                combined_stations = pd.concat([top_generators, top_accumulators])
                
                # Horizontal bar chart
                station_chart = alt.Chart(combined_stations).mark_bar().encode(
                    y=alt.Y('name:N', title=None, sort=alt.EncodingSortField(field='abs_flow', order='descending')),
                    x=alt.X('abs_flow:Q', title='Bike Imbalance (absolute)'),
                    color=alt.Color('station_type:N', title='Station Type',
                                  scale=alt.Scale(domain=['Generator (Needs Bikes)', 'Accumulator (Excess Bikes)'],
                                                range=['#E53935', '#43A047'])),
                    tooltip=['name:N', 'abs_flow:Q', 'station_type:N']
                ).properties(
                    width=400,
                    height=300
                )
                
                # Chart display
                st.altair_chart(station_chart, use_container_width=True)
                
                # Overall system balance metrics
                total_imbalance = station_flows_df['net_flow'].abs().sum() / 2  # Divide by 2 because each imbalance is counted twice
                total_trips = station_flows_df['outflows'].sum()
                imbalance_pct = (total_imbalance / total_trips) * 100 if total_trips > 0 else 0
                
                # Per interval metrics
                bikes_rebalanced_per_interval = total_imbalance / intervals_in_period
                
                # Key metrics 
                col1, col2 = st.columns(2)
                with col1:
                    st.metric("Total Bikes Needing Rebalancing", int(total_imbalance))
                with col2:
                    st.metric(f"Bikes to Rebalance per {selected_interval}", f"{bikes_rebalanced_per_interval:.0f}")
            else:
                st.info("No rebalancing data available for the selected period.")
        
        if not station_flows_df.empty and not peak_times_df.empty:
            # Safe access to dataframes
            generator_station = generators_df.iloc[0]['name'] if not generators_df.empty else "generator stations"
            accumulator_station = accumulators_df.iloc[0]['name'] if not accumulators_df.empty else "accumulator stations"
            
            # Peak times formatting
            weekday_peak_times = ", ".join([f"{int(h):02d}:00" for h in top_weekday_hours['hour'].head(2)])
            weekend_peak_times = ", ".join([f"{int(h):02d}:00" for h in top_weekend_hours['hour'].head(2)])
            
            st.markdown('<div class="insight-box"><b>Combined System Analysis Insights</b>', unsafe_allow_html=True)
            st.markdown(f"""
            1. From **{start_date_str}** to **{end_date_str}**, the system requires rebalancing of approximately **{int(total_imbalance)} bikes** ({imbalance_pct:.1f}% of total trips).
            
            2. Focusing rebalancing at **{generator_station}** (needs bikes) and **{accumulator_station}** (excess bikes) will help optimise availability and reduce underutilisation during key times (**weekdays: {weekday_peak_times}**, **weekends: {weekend_peak_times}**). 
            
            3. Generators (stations that lose bikes) are typically in residential areas or at the top of hills, while accumulators (stations that gain bikes) are in business districts, tourist areas, and at the bottom of hills.
            """)
            st.markdown('</div>', unsafe_allow_html=True)
        else:
            st.markdown('<div class="insight-box"><b>System Analysis Insights</b>', unsafe_allow_html=True)
            st.markdown("""
            Insufficient data available for the selected period to generate comprehensive system insights. 
            Please adjust the date range to include more data.
            """)
            st.markdown('</div>', unsafe_allow_html=True)
    else:
        st.warning("No system analysis data available for the selected time period.")

    #region 4: Proposed Solution Strategy
    st.markdown('<div class="subheader">4. Proposed Solution Strategy</div>', unsafe_allow_html=True)

    st.markdown('<div class="recommendation-box"><b>Based on this analysis and its key results, there are some potential strategies that can be implemented to help address both capacity issues and improper bike parking</b></div>', unsafe_allow_html=True)
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown('<div class="section-header">Infrastructure Solutions</div>', unsafe_allow_html=True)
        st.markdown("""
        **1. Targeted Capacity Expansion:**
        - Add docks at high-demand stations
        - Prioritize stations with the highest instances of reaching capacity
        - Plan for adding new nearby stations in consistently busy areas with little station spread
        
        **2. Overflow Parking Zones:**
        - Create designated overflow parking areas near high-demand stations
        - Install clear signage and physical markers (painted areas, small barriers)
        - Equip bikes with GPS tracking to locate those parked in overflow zones
        
        **3. Virtual Docking Stations:**
        - Implement geo-fenced areas where bikes can be left without physical docks
        - Use the mobile app to guide users to these designated areas
        - Apply incentives for proper use of virtual docks
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col2:
        st.markdown('<div class="section-header">Operational Solutions</div>', unsafe_allow_html=True)
        st.markdown(f"""
        **1. Dynamic Rebalancing Strategy:**
        - Implement time-based rebalancing focused on peak hours
        - Create routes that prioritise stations with the highest imbalances
        - Potential cost savings through optimised scheduling
        
        **2. User-Driven Rebalancing:**
        - Consider offering incentives (ride credits, discounts) for returning bikes to specific stations
        - Implement dynamic pricing based on station demand (higher fees for popular destinations)
        - Create a points system rewarding users who help balance the system
        
        **3. Predictive Analytics:**
        - Use historical data to predict capacity issues before they occur
        - Integrate weather forecasts to anticipate demand fluctuations
        - Develop an early warning system for stations approaching capacity
        """)
        st.markdown('</div>', unsafe_allow_html=True)

    #endregion