import streamlit as st
import json
import pandas as pd
import plotly.express as px
import os
from datetime import datetime, date

from pathlib import Path

# Your actual data structure
DEFAULT_DATA = {
  "Level1Groups": [
    {
      "name": "GroupA",
      "subsystems": [
        {
          "name": "SubsystemX",
          "pipelines": {
            "streaming": {
              "finalized": 0,
              "uat": 0,
              "planned": 0,
              "production": 0
            },
            "batch": {
              "finalized": 0,
              "uat": 0,
              "planned": 0,
              "production": 0
            }
          },
          "contacts": {
            "producerTech": [
              "Alice",
              "Bob"
            ],
            "producerBusiness": [
              "Eve"
            ],
            "ourTech": [
              "Charlie"
            ],
            "ourBusiness": [
              "Diana"
            ]
          },
          "issues": [
            {
              "id": "ISS-101",
              "description": "Data delay from source",
              "status": "Open",
              "start_date": "2025-08-27",
              "close_date": None
            },
            {
              "id": "ISS-102",
              "description": "Schema mismatch on v2",
              "status": "In Progress",
              "start_date": "2025-08-25",
              "close_date": None
            }
          ],
          "pipelineDetails": {
            "streaming": [],
            "batch": []
          }
        }
      ]
    }
  ]
}

STAGE_DISPLAY = {
    "finalized": "Finalized",
    "uat": "UAT",
    "planned": "Planned",
    "production": "Production",
}

# Pipeline field definitions with tooltips
BATCH_FIELDS = {
    "pipeline_name": "Actual name of pipeline",
    "data_name": "What business/producer team recognize this data flow",
    "frequency": "Frequency of file publish (daily, weekly, monthly)",
    "run_day": "Which day of week/month this file will be pushed",
    "run_timestamp": "Exact time of run",
    "file_size": "File size in MB",
    "uat_date": "Planned UAT deploy date",
    "prod_date": "Planned PROD deploy date",
    "status": "Current pipeline status",
    "comment": "Additional comments or notes"
}

STREAMING_FIELDS = {
    "pipeline_name": "Actual name of pipeline",
    "data_name": "What business/producer team recognize this data flow",
    "start_time": "Streaming start time",
    "end_time": "Streaming end time",
    "run_day": "Which day of week/month this file will be pushed",
    "rough_volume": "Total size in MB between start and end time",
    "uat_date": "Planned UAT deploy date",
    "prod_date": "Planned PROD deploy date",
    "status": "Current pipeline status",
    "comment": "Additional comments or notes"
}

def load_data() -> dict:
    """Load data from JSON file, create default if file doesn't exist"""
    data_file = "data.json"
    
    if os.path.exists(data_file):
        try:
            with open(data_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                # Ensure all subsystems have pipelineDetails
                for group in data.get("Level1Groups", []):
                    for subsystem in group.get("subsystems", []):
                        if "pipelineDetails" not in subsystem:
                            subsystem["pipelineDetails"] = {"streaming": [], "batch": []}
                return data
        except (json.JSONDecodeError, FileNotFoundError) as e:
            st.warning(f"Error loading data file: {e}. Using default data.")
            return DEFAULT_DATA
    else:
        # Create default data file if it doesn't exist
        save_data(DEFAULT_DATA)
        return DEFAULT_DATA

def save_data(data: dict) -> bool:
    """Save data to JSON file"""
    try:
        with open("data.json", 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        # Reset modification flag after successful save
        if 'data_modified' in st.session_state:
            st.session_state.data_modified = False
        return True
    except Exception as e:
        st.error(f"Error saving data: {e}")
        return False

def mark_data_modified():
    """Mark data as modified to show save warning"""
    st.session_state.data_modified = True

def get_today_string():
    """Get today's date as string in YYYY-MM-DD format"""
    return date.today().strftime("%Y-%m-%d")

def calculate_blocked_days(start_date_str, close_date_str=None):
    """Calculate blocked days between start and close date (or today if not closed)"""
    try:
        start_date = datetime.strptime(start_date_str, "%Y-%m-%d").date()
        if close_date_str:
            end_date = datetime.strptime(close_date_str, "%Y-%m-%d").date()
        else:
            end_date = date.today()
        
        delta = end_date - start_date
        return max(0, delta.days)
    except (ValueError, TypeError):
        return 0

def format_date_display(date_str):
    """Format date string for display"""
    if not date_str:
        return "Not set"
    try:
        date_obj = datetime.strptime(date_str, "%Y-%m-%d")
        return date_obj.strftime("%B %d, %Y")
    except ValueError:
        return date_str

def validate_date_range(start_date, close_date):
    """Validate that close date is not before start date"""
    if start_date and close_date:
        return close_date >= start_date
    return True

def get_issue_age_color(blocked_days):
    """Get color indicator based on issue age"""
    if blocked_days <= 7:
        return "🟢"  # Green for recent
    elif blocked_days <= 30:
        return "🟡"  # Yellow for moderate
    else:
        return "🔴"  # Red for old

def extract_pipelines(data: dict) -> pd.DataFrame:
    records = []
    for group in data.get("Level1Groups", []):
        for subsystem in group.get("subsystems", []):
            for ptype, stages in subsystem.get("pipelines", {}).items():
                for stage_key, count in stages.items():
                    records.append({
                        "Group": group.get("name"),
                        "Subsystem": subsystem.get("name"),
                        "PipelineType": ptype,
                        "StageKey": stage_key,
                        "Stage": STAGE_DISPLAY.get(stage_key, stage_key.title()),
                        "Count": int(count or 0),
                    })
    df = pd.DataFrame.from_records(records)
    if df.empty:
        return pd.DataFrame(columns=["Group", "Subsystem", "PipelineType", "StageKey", "Stage", "Count"])
    return df

def get_subsystem_node(data: dict, group_name: str, subsystem_name: str) -> dict | None:
    for group in data.get("Level1Groups", []):
        if group.get("name") == group_name:
            for subsystem in group.get("subsystems", []):
                if subsystem.get("name") == subsystem_name:
                    return subsystem
    return None

def update_pipeline_counts(data: dict):
    """Update pipeline counts based on actual pipeline details"""
    for group in data.get("Level1Groups", []):
        for subsystem in group.get("subsystems", []):
            if "pipelineDetails" in subsystem:
                for ptype in ["streaming", "batch"]:
                    if ptype in subsystem["pipelineDetails"]:
                        pipelines = subsystem["pipelineDetails"][ptype]
                        counts = {"finalized": 0, "uat": 0, "planned": 0, "production": 0}
                        
                        for pipeline in pipelines:
                            status = pipeline.get("status", "").lower()
                            if status == "uat":
                                counts["uat"] += 1
                            elif status == "prod":
                                counts["production"] += 1
                            elif status == "planned":
                                counts["planned"] += 1
                            elif status == "blocked":
                                counts["finalized"] += 1  # Blocked pipelines count as finalized for now
                            else:
                                counts["finalized"] += 1
                        
                        # Update the counts
                        if "pipelines" not in subsystem:
                            subsystem["pipelines"] = {}
                        if ptype not in subsystem["pipelines"]:
                            subsystem["pipelines"][ptype] = {}
                        subsystem["pipelines"][ptype].update(counts)

def validate_excel_upload(df: pd.DataFrame, pipeline_type: str) -> tuple[bool, str]:
    """Validate Excel upload format"""
    if pipeline_type == "batch":
        required_fields = list(BATCH_FIELDS.keys())
    else:
        required_fields = list(STREAMING_FIELDS.keys())
    
    missing_fields = [field for field in required_fields if field not in df.columns]
    
    if missing_fields:
        return False, f"Missing required columns: {', '.join(missing_fields)}"
    
    if df.empty:
        return False, "Excel file is empty"
    
    return True, "Valid format"

def process_excel_upload(df: pd.DataFrame, pipeline_type: str) -> list:
    """Process Excel upload and convert to pipeline objects"""
    pipelines = []
    
    for _, row in df.iterrows():
        pipeline = {}
        for field in df.columns:
            if pd.notna(row[field]):
                pipeline[field] = str(row[field])
            else:
                pipeline[field] = ""
        pipelines.append(pipeline)
    
    return pipelines

# -------- Streamlit UI --------
st.set_page_config(page_title="Pipeline Onboarding Dashboard", layout="wide")
st.title("Pipeline Onboarding Dashboard")

# Load data and show status
data = load_data()
df = extract_pipelines(data)

# Show data source status and controls
col1, col2 = st.columns([3, 1])

with col1:
    if os.path.exists("data.json"):
        file_stats = os.stat("data.json")
        st.info(f"Data loaded from: data.json (Last modified: {pd.Timestamp(file_stats.st_mtime, unit='s').strftime('%Y-%m-%d %H:%M:%S')})")
    else:
        st.info("Using default data (data.json will be created on first save)")

with col2:
    if st.button("Refresh Data", key="refresh_dashboard_main"):
        st.rerun()

# Initialize session state for tracking changes
if 'data_modified' not in st.session_state:
    st.session_state.data_modified = False

# Sidebar navigation
st.sidebar.title("Navigation")
tab_selection = st.sidebar.radio("Select Tab", ["Dashboard", "Admin"])

# Sidebar filters populated from data
groups = df["Group"].dropna().unique()
selected_group = st.sidebar.selectbox("Select Level-1 Group", groups)
subsystems = df[df["Group"] == selected_group]["Subsystem"].dropna().unique()
selected_subsystem = st.sidebar.selectbox("Select Subsystem", subsystems)

sub_df = df[(df["Group"] == selected_group) & (df["Subsystem"] == selected_subsystem)]

if sub_df.empty:
    st.warning("No pipeline records found. Please check your data source.")
    st.stop()

# Dashboard Tab
if tab_selection == "Dashboard":
    st.header("Dashboard View")
    
    # Summary Metrics
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Finalized", int(sub_df[sub_df["StageKey"] == "finalized"]["Count"].sum()))
    col2.metric("UAT", int(sub_df[sub_df["StageKey"] == "uat"]["Count"].sum()))
    col3.metric("Planned", int(sub_df[sub_df["StageKey"] == "planned"]["Count"].sum()))
    col4.metric("Production", int(sub_df[sub_df["StageKey"] == "production"]["Count"].sum()))

    # Charts
    colA, colB = st.columns(2)
    fig1 = px.pie(sub_df, values="Count", names="PipelineType", title="Streaming vs Batch")
    colA.plotly_chart(fig1, use_container_width=True)
    fig2 = px.bar(
        sub_df,
        x="Stage",
        y="Count",
        color="PipelineType",
        barmode="stack",
        title="Pipelines by Stage (Stacked)",
    )
    colB.plotly_chart(fig2, use_container_width=True)

    group_df = df[df["Group"] == selected_group].groupby(["Subsystem"])['Count'].sum().reset_index()
    st.plotly_chart(
        px.bar(group_df, x="Subsystem", y="Count", title=f"Total Pipelines per Subsystem in {selected_group}"),
        use_container_width=True
    )

    # Issues Summary Table (Read-only view)
    node = get_subsystem_node(data, selected_group, selected_subsystem)
    if node and node.get("issues"):
        st.header("Issues Overview")
        
        # Prepare data for the table
        summary_data = []
        for issue in node.get("issues", []):
            start_date_str = issue.get("start_date", "")
            close_date_str = issue.get("close_date", "")
            blocked_days = calculate_blocked_days(start_date_str, close_date_str)
            
            # Get age-based color indicator
            age_color = get_issue_age_color(blocked_days)
            
            summary_data.append({
                "Issue ID": issue["id"],
                "Description": issue["description"][:50] + "..." if len(issue["description"]) > 50 else issue["description"],
                "Status": issue["status"],
                "Start Date": format_date_display(start_date_str),
                "Close Date": format_date_display(close_date_str),
                "Blocked Days": blocked_days,
                "Age Indicator": age_color,
                "Status": "Closed" if issue["status"] == "Closed" else "In Progress" if issue["status"] == "In Progress" else "Open"
            })
        
        # Create summary DataFrame
        summary_df = pd.DataFrame(summary_data)
        
        # Display the table with styling
        st.dataframe(
            summary_df,
            column_config={
                "Age Indicator": st.column_config.TextColumn("Age", help="Recent (≤7 days), Moderate (8-30 days), Old (>30 days)"),
                "Status": st.column_config.TextColumn("Status", help="Closed, In Progress, Open"),
                "Blocked Days": st.column_config.NumberColumn("Days Blocked", help="Total days from start to close (or today if open)")
            },
            hide_index=True,
            use_container_width=True
        )
        
        # Summary statistics
        total_open = len([i for i in node.get("issues", []) if i["status"] == "Open"])
        total_in_progress = len([i for i in node.get("issues", []) if i["status"] == "In Progress"])
        total_closed = len([i for i in node.get("issues", []) if i["status"] == "Closed"])
        total_blocked_days = sum([calculate_blocked_days(i.get("start_date", ""), i.get("close_date", "")) for i in node.get("issues", [])])
        
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Open Issues", total_open)
        col2.metric("In Progress", total_in_progress)
        col3.metric("Closed", total_closed)
        col4.metric("Total Blocked Days", total_blocked_days)

# Admin Tab
elif tab_selection == "Admin":
    st.header("Admin Panel")
    st.info("Use this panel to edit pipeline data, contacts, and issues. Changes will be saved to the JSON file.")
    
    node = get_subsystem_node(data, selected_group, selected_subsystem)
    if node:
        st.subheader(f"Edit {selected_subsystem} Details")

        # Pipeline Management Section
        st.subheader("Pipeline Management")
        
        # Pipeline Type Selection
        pipeline_type = st.selectbox("Select Pipeline Type", ["batch", "streaming"], key="pipeline_type_select")
        
        # Show existing pipelines
        if "pipelineDetails" not in node:
            node["pipelineDetails"] = {"streaming": [], "batch": []}
        
        existing_pipelines = node["pipelineDetails"].get(pipeline_type, [])
        
        # Display existing pipelines in a table with action columns
        if existing_pipelines:
            st.markdown(f"**Existing {pipeline_type.title()} Pipelines ({len(existing_pipelines)}):**")
            
            # Create table data with action columns
            table_data = []
            for idx, pipeline in enumerate(existing_pipelines):
                if pipeline_type == "batch":
                    table_data.append({
                        "Pipeline Name": pipeline.get('pipeline_name', 'N/A'),
                        "Data Name": pipeline.get('data_name', 'N/A'),
                        "Frequency": pipeline.get('frequency', 'N/A'),
                        "Run Day": pipeline.get('run_day', 'N/A'),
                        "Run Time": pipeline.get('run_timestamp', 'N/A'),
                        "File Size": f"{pipeline.get('file_size', 'N/A')} MB",
                        "Status": pipeline.get('status', 'N/A'),
                        "Edit": f"edit_{idx}_{pipeline_type}_{selected_subsystem}",
                        "Delete": f"delete_{idx}_{pipeline_type}_{selected_subsystem}"
                    })
                else:  # streaming
                    table_data.append({
                        "Pipeline Name": pipeline.get('pipeline_name', 'N/A'),
                        "Data Name": pipeline.get('data_name', 'N/A'),
                        "Start Time": pipeline.get('start_time', 'N/A'),
                        "End Time": pipeline.get('end_time', 'N/A'),
                        "Run Day": pipeline.get('run_day', 'N/A'),
                        "Volume": f"{pipeline.get('rough_volume', 'N/A')} MB",
                        "Status": pipeline.get('status', 'N/A'),
                        "Edit": f"edit_{idx}_{pipeline_type}_{selected_subsystem}",
                        "Delete": f"delete_{idx}_{pipeline_type}_{selected_subsystem}"
                    })
            
            # Create a custom table layout with action buttons in columns
            st.markdown("**Pipeline Table:**")
            
            # Create header row
            header_cols = st.columns([2, 2, 1, 1, 1, 1, 1, 1, 1])
            with header_cols[0]:
                st.markdown("**Pipeline Name**")
            with header_cols[1]:
                st.markdown("**Data Name**")
            with header_cols[2]:
                st.markdown("**Frequency**")
            with header_cols[3]:
                st.markdown("**Run Day**")
            with header_cols[4]:
                st.markdown("**Run Time**")
            with header_cols[5]:
                st.markdown("**File Size**")
            with header_cols[6]:
                st.markdown("**Status**")
            with header_cols[7]:
                st.markdown("**Edit**")
            with header_cols[8]:
                st.markdown("**Delete**")
            
            # Create data rows with action buttons
            for idx, pipeline in enumerate(existing_pipelines):
                row_cols = st.columns([2, 2, 1, 1, 1, 1, 1, 1, 1])
                
                if pipeline_type == "batch":
                    with row_cols[0]:
                        st.write(pipeline.get('pipeline_name', 'N/A'))
                    with row_cols[1]:
                        st.write(pipeline.get('data_name', 'N/A'))
                    with row_cols[2]:
                        st.write(pipeline.get('frequency', 'N/A'))
                    with row_cols[3]:
                        st.write(pipeline.get('run_day', 'N/A'))
                    with row_cols[4]:
                        st.write(pipeline.get('run_timestamp', 'N/A'))
                    with row_cols[5]:
                        st.write(f"{pipeline.get('file_size', 'N/A')} MB")
                    with row_cols[6]:
                        st.write(pipeline.get('status', 'N/A'))
                    with row_cols[7]:
                        if st.button("✏️ Edit", key=f"edit_{idx}_{pipeline_type}_{selected_subsystem}", 
                                   type="primary", help="Edit this pipeline"):
                            st.session_state.edit_pipeline = {
                                'index': idx,
                                'type': pipeline_type,
                                'data': pipeline
                            }
                            st.rerun()
                    with row_cols[8]:
                        if st.button("🗑️ Delete", key=f"delete_{idx}_{pipeline_type}_{selected_subsystem}", 
                                   type="secondary", help="Delete this pipeline"):
                            # Remove the pipeline
                            node["pipelineDetails"][pipeline_type].pop(idx)
                            update_pipeline_counts(data)
                            # Save immediately so the deletion persists
                            if save_data(data):
                                st.success(f"Pipeline {pipeline.get('pipeline_name', 'Unknown')} deleted!")
                            else:
                                st.error("Failed to delete pipeline. Please try again.")
                            st.rerun()
                else:  # streaming
                    with row_cols[0]:
                        st.write(pipeline.get('pipeline_name', 'N/A'))
                    with row_cols[1]:
                        st.write(pipeline.get('data_name', 'N/A'))
                    with row_cols[2]:
                        st.write(pipeline.get('start_time', 'N/A'))
                    with row_cols[3]:
                        st.write(pipeline.get('end_time', 'N/A'))
                    with row_cols[4]:
                        st.write(pipeline.get('run_day', 'N/A'))
                    with row_cols[5]:
                        st.write(f"{pipeline.get('rough_volume', 'N/A')} MB")
                    with row_cols[6]:
                        st.write(pipeline.get('status', 'N/A'))
                    with row_cols[7]:
                        if st.button("✏️ Edit", key=f"edit_{idx}_{pipeline_type}_{selected_subsystem}", 
                                   type="primary", help="Edit this pipeline"):
                            st.session_state.edit_pipeline = {
                                'index': idx,
                                'type': pipeline_type,
                                'data': pipeline
                            }
                            st.rerun()
                    with row_cols[8]:
                        if st.button("🗑️ Delete", key=f"delete_{idx}_{pipeline_type}_{selected_subsystem}", 
                                   type="secondary", help="Delete this pipeline"):
                            # Remove the pipeline
                            node["pipelineDetails"][pipeline_type].pop(idx)
                            update_pipeline_counts(data)
                            # Save immediately so the deletion persists
                            if save_data(data):
                                st.success(f"Pipeline {pipeline.get('pipeline_name', 'Unknown')} deleted!")
                            else:
                                st.error("Failed to delete pipeline. Please try again.")
                            st.rerun()
                
                # Add separator between rows
                st.markdown("---")
        else:
            st.info(f"No {pipeline_type.title()} pipelines found. Add your first pipeline below!")
        
        # Add New Pipeline Section
        st.markdown("---")
        st.subheader("Add New Pipeline")
        
        # Check if we're editing an existing pipeline
        editing_pipeline = st.session_state.get('edit_pipeline', None)
        is_editing = editing_pipeline and editing_pipeline['type'] == pipeline_type
        
        if is_editing:
            st.info(f"Editing pipeline: {editing_pipeline['data'].get('pipeline_name', 'Unknown')}")
            pipeline_data = editing_pipeline['data']
        else:
            pipeline_data = {}
        
        # Single Pipeline Addition
        with st.expander("Add Single Pipeline", expanded=True):
            if pipeline_type == "batch":
                col1, col2 = st.columns(2)
                with col1:
                    pipeline_name = st.text_input("Pipeline Name*", value=pipeline_data.get('pipeline_name', ''), 
                                                help=BATCH_FIELDS["pipeline_name"], key="new_batch_name")
                    data_name = st.text_input("Data Name*", value=pipeline_data.get('data_name', ''), 
                                            help=BATCH_FIELDS["data_name"], key="new_batch_data")
                    frequency = st.selectbox("Frequency*", ["daily", "weekly", "monthly"], 
                                          index=["daily", "weekly", "monthly"].index(pipeline_data.get('frequency', 'daily')), 
                                          help=BATCH_FIELDS["frequency"], key="new_batch_freq")
                    run_day = st.text_input("Run Day*", value=pipeline_data.get('run_day', ''), 
                                          placeholder="e.g., Monday, 1st", help=BATCH_FIELDS["run_day"], key="new_batch_day")
                    run_timestamp = st.time_input("Run Timestamp*", 
                                                value=datetime.strptime(pipeline_data.get('run_timestamp', '00:00'), '%H:%M').time() if pipeline_data.get('run_timestamp') else datetime.now().time(), 
                                                help=BATCH_FIELDS["run_timestamp"], key="new_batch_time")
                    file_size = st.number_input("File Size (MB)*", min_value=0.0, step=0.1, 
                                              value=float(pipeline_data.get('file_size', 0.0)), 
                                              help=BATCH_FIELDS["file_size"], key="new_batch_size")
                
                with col2:
                    uat_date = st.date_input("UAT Date", 
                                           value=datetime.strptime(pipeline_data.get('uat_date', '2025-01-01'), '%Y-%m-%d').date() if pipeline_data.get('uat_date') else datetime.now().date(), 
                                           help=BATCH_FIELDS["uat_date"], key="new_batch_uat")
                    prod_date = st.date_input("PROD Date", 
                                            value=datetime.strptime(pipeline_data.get('prod_date', '2025-01-01'), '%Y-%m-%d').date() if pipeline_data.get('prod_date') else datetime.now().date(), 
                                            help=BATCH_FIELDS["prod_date"], key="new_batch_prod")
                    status = st.selectbox("Status*", ["Planned", "UAT", "PROD", "Blocked"], 
                                       index=["Planned", "UAT", "PROD", "Blocked"].index(pipeline_data.get('status', 'Planned')), 
                                       help=BATCH_FIELDS["status"], key="new_batch_status")
                    comment = st.text_area("Comment", value=pipeline_data.get('comment', ''), 
                                         help=BATCH_FIELDS["comment"], key="new_batch_comment")
                
                if is_editing:
                    col1, col2 = st.columns(2)
                    with col1:
                        if st.button("Update Pipeline", key=f"update_batch_{selected_subsystem}", type="primary"):
                            if pipeline_name and data_name and frequency and run_day and file_size:
                                # Update existing pipeline
                                node["pipelineDetails"][pipeline_type][editing_pipeline['index']] = {
                                    "pipeline_name": pipeline_name,
                                    "data_name": data_name,
                                    "frequency": frequency,
                                    "run_day": run_day,
                                    "run_timestamp": run_timestamp.strftime("%H:%M"),
                                    "file_size": str(file_size),
                                    "uat_date": uat_date.strftime("%Y-%m-%d") if uat_date else "",
                                    "prod_date": prod_date.strftime("%Y-%m-%d") if prod_date else "",
                                    "status": status,
                                    "comment": comment
                                }
                                update_pipeline_counts(data)
                                # Clear edit mode
                                if 'edit_pipeline' in st.session_state:
                                    del st.session_state.edit_pipeline
                                # Save immediately so the update persists
                                if save_data(data):
                                    st.success(f"Pipeline updated!")
                                else:
                                    st.error("Failed to update pipeline. Please try again.")
                                st.rerun()
                            else:
                                st.error("Please fill in all required fields (marked with *)")
                    
                    with col2:
                        if st.button("Cancel Edit", key=f"cancel_batch_{selected_subsystem}", type="secondary"):
                            if 'edit_pipeline' in st.session_state:
                                del st.session_state.edit_pipeline
                            st.rerun()
                else:
                    if st.button("Add Batch Pipeline", key=f"add_batch_{selected_subsystem}", type="primary"):
                        if pipeline_name and data_name and frequency and run_day and file_size:
                            new_pipeline = {
                                "pipeline_name": pipeline_name,
                                "data_name": data_name,
                                "frequency": frequency,
                                "run_day": run_day,
                                "run_timestamp": run_timestamp.strftime("%H:%M"),
                                "file_size": str(file_size),
                                "uat_date": uat_date.strftime("%Y-%m-%d") if uat_date else "",
                                "prod_date": prod_date.strftime("%Y-%m-%d") if prod_date else "",
                                "status": status,
                                "comment": comment
                            }
                            node["pipelineDetails"][pipeline_type].append(new_pipeline)
                            update_pipeline_counts(data)
                            # Save immediately so the pipeline persists after rerun
                            if save_data(data):
                                st.success(f"Added new {pipeline_type} pipeline: {pipeline_name}")
                            else:
                                st.error("Failed to save pipeline. Please try again.")
                            st.rerun()
                        else:
                            st.error("Please fill in all required fields (marked with *)")
            
            else:  # streaming
                col1, col2 = st.columns(2)
                with col1:
                    pipeline_name = st.text_input("Pipeline Name*", value=pipeline_data.get('pipeline_name', ''), 
                                                help=STREAMING_FIELDS["pipeline_name"], key="new_stream_name")
                    data_name = st.text_input("Data Name*", value=pipeline_data.get('data_name', ''), 
                                            help=STREAMING_FIELDS["data_name"], key="new_stream_data")
                    start_time = st.time_input("Start Time*", 
                                             value=datetime.strptime(pipeline_data.get('start_time', '00:00'), '%H:%M').time() if pipeline_data.get('start_time') else datetime.now().time(), 
                                             help=STREAMING_FIELDS["start_time"], key="new_stream_start")
                    end_time = st.time_input("End Time*", 
                                           value=datetime.strptime(pipeline_data.get('end_time', '23:59'), '%H:%M').time() if pipeline_data.get('end_time') else datetime.now().time(), 
                                           help=STREAMING_FIELDS["end_time"], key="new_stream_end")
                    run_day = st.text_input("Run Day*", value=pipeline_data.get('run_day', ''), 
                                          placeholder="e.g., Monday, 1st", help=STREAMING_FIELDS["run_day"], key="new_stream_day")
                    rough_volume = st.number_input("Volume (MB)*", min_value=0.0, step=0.1, 
                                                 value=float(pipeline_data.get('rough_volume', 0.0)), 
                                                 help=STREAMING_FIELDS["rough_volume"], key="new_stream_volume")
                
                with col2:
                    uat_date = st.date_input("UAT Date", 
                                           value=datetime.strptime(pipeline_data.get('uat_date', '2025-01-01'), '%Y-%m-%d').date() if pipeline_data.get('uat_date') else datetime.now().date(), 
                                           help=STREAMING_FIELDS["uat_date"], key="new_stream_uat")
                    prod_date = st.date_input("PROD Date", 
                                            value=datetime.strptime(pipeline_data.get('prod_date', '2025-01-01'), '%Y-%m-%d').date() if pipeline_data.get('prod_date') else datetime.now().date(), 
                                            help=STREAMING_FIELDS["prod_date"], key="new_stream_prod")
                    status = st.selectbox("Status*", ["Planned", "UAT", "PROD", "Blocked"], 
                                       index=["Planned", "UAT", "PROD", "Blocked"].index(pipeline_data.get('status', 'Planned')), 
                                       help=STREAMING_FIELDS["status"], key="new_stream_status")
                    comment = st.text_area("Comment", value=pipeline_data.get('comment', ''), 
                                         help=STREAMING_FIELDS["comment"], key="new_stream_comment")
                
                if is_editing:
                    col1, col2 = st.columns(2)
                    with col1:
                        if st.button("Update Pipeline", key=f"update_stream_{selected_subsystem}", type="primary"):
                            if pipeline_name and data_name and start_time and end_time and run_day and rough_volume:
                                # Update existing pipeline
                                node["pipelineDetails"][pipeline_type][editing_pipeline['index']] = {
                                    "pipeline_name": pipeline_name,
                                    "data_name": data_name,
                                    "start_time": start_time.strftime("%H:%M"),
                                    "end_time": end_time.strftime("%H:%M"),
                                    "run_day": run_day,
                                    "rough_volume": str(rough_volume),
                                    "uat_date": uat_date.strftime("%Y-%m-%d") if uat_date else "",
                                    "prod_date": prod_date.strftime("%Y-%m-%d") if prod_date else "",
                                    "status": status,
                                    "comment": comment
                                }
                                update_pipeline_counts(data)
                                # Clear edit mode
                                if 'edit_pipeline' in st.session_state:
                                    del st.session_state.edit_pipeline
                                # Save immediately so the update persists
                                if save_data(data):
                                    st.success(f"Pipeline updated!")
                                else:
                                    st.error("Failed to update pipeline. Please try again.")
                                st.rerun()
                            else:
                                st.error("Please fill in all required fields (marked with *)")
                    
                    with col2:
                        if st.button("Cancel Edit", key=f"cancel_stream_{selected_subsystem}", type="secondary"):
                            if 'edit_pipeline' in st.session_state:
                                del st.session_state.edit_pipeline
                            st.rerun()
                else:
                    if st.button("Add Streaming Pipeline", key=f"add_stream_{selected_subsystem}", type="primary"):
                        if pipeline_name and data_name and start_time and end_time and run_day and rough_volume:
                            new_pipeline = {
                                "pipeline_name": pipeline_name,
                                "data_name": data_name,
                                "start_time": start_time.strftime("%H:%M"),
                                "end_time": end_time.strftime("%H:%M"),
                                "run_day": run_day,
                                "rough_volume": str(rough_volume),
                                "uat_date": uat_date.strftime("%Y-%m-%d") if uat_date else "",
                                "prod_date": prod_date.strftime("%Y-%m-%d") if prod_date else "",
                                "status": status,
                                "comment": comment
                            }
                            node["pipelineDetails"][pipeline_type].append(new_pipeline)
                            update_pipeline_counts(data)
                            # Save immediately so the pipeline persists after rerun
                            if save_data(data):
                                st.success(f"Added new {pipeline_type} pipeline: {pipeline_name}")
                            else:
                                st.error("Failed to save pipeline. Please try again.")
                            st.rerun()
                        else:
                            st.error("Please fill in all required fields (marked with *)")
