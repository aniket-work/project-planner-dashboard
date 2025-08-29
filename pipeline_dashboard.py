import streamlit as st
import json
import pandas as pd
import plotly.express as px
import os
from datetime import datetime, date

from pathlib import Path

# Default data structure if no file exists
SAMPLE_DATA = {
    "Level1Groups": [
        {
            "name": "GroupA",
            "subsystems": [
                {
                    "name": "SubsystemX",
                    "pipelines": {
                        "streaming": {"finalized": 10, "uat": 5, "planned": 3, "production": 2},
                        "batch": {"finalized": 20, "uat": 7, "planned": 4, "production": 9}
                    },
                    "contacts": {
                        "producerTech": ["Alice", "Bob"],
                        "producerBusiness": ["Eve"],
                        "ourTech": ["Charlie"],
                        "ourBusiness": ["Diana"]
                    },
                              "issues": [
            {"id": "ISS-101", "description": "Data delay from source", "status": "Open", "start_date": "2024-01-15", "close_date": None},
            {"id": "ISS-102", "description": "Schema mismatch on v2", "status": "In Progress", "start_date": "2024-01-20", "close_date": None}
          ]
                },
                {
                    "name": "SubsystemY",
                    "pipelines": {
                        "streaming": {"finalized": 5, "uat": 2, "planned": 1, "production": 1},
                        "batch": {"finalized": 8, "uat": 3, "planned": 2, "production": 2}
                    },
                    "contacts": {
                        "producerTech": ["Frank"],
                        "producerBusiness": ["Grace"],
                        "ourTech": ["Hank"],
                        "ourBusiness": ["Ivy"]
                    },
                    "issues": []
                },
                {
                    "name": "SubsystemZ",
                    "pipelines": {
                        "streaming": {"finalized": 2, "uat": 1, "planned": 6, "production": 0},
                        "batch": {"finalized": 3, "uat": 4, "planned": 5, "production": 1}
                    },
                    "contacts": {
                        "producerTech": ["Jai", "Kim"],
                        "producerBusiness": ["Lena"],
                        "ourTech": ["Mo", "Nia"],
                        "ourBusiness": ["Omar"]
                    },
                              "issues": [
            {"id": "ISS-201", "description": "Access to UAT blocked", "status": "Open", "start_date": "2024-01-25", "close_date": None}
          ]
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

def load_data() -> dict:
    """Load data from JSON file, create default if file doesn't exist"""
    data_file = "data.json"
    
    if os.path.exists(data_file):
        try:
            with open(data_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        except (json.JSONDecodeError, FileNotFoundError) as e:
            st.warning(f"Error loading data file: {e}. Using default data.")
            return SAMPLE_DATA
    else:
        # Create default data file if it doesn't exist
        save_data(SAMPLE_DATA)
        return SAMPLE_DATA

def save_data(data: dict) -> bool:
    """Save data to JSON file"""
    try:
        with open("data.json", 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        # Reset modification flag after successful save
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

# -------- Streamlit UI --------
st.set_page_config(page_title="Pipeline Onboarding Dashboard", layout="wide")
st.title("📊 Pipeline Onboarding Dashboard")

# Load data and show status
data = load_data()
df = extract_pipelines(data)

# Show data source status and controls
col1, col2 = st.columns([3, 1])

with col1:
    if os.path.exists("data.json"):
        file_stats = os.stat("data.json")
        st.info(f"📁 Data loaded from: data.json (Last modified: {pd.Timestamp(file_stats.st_mtime, unit='s').strftime('%Y-%m-%d %H:%M:%S')})")
    else:
        st.info("📁 Using default data (data.json will be created on first save)")

with col2:
    if st.button("🔄 Refresh Data"):
        st.rerun()

# Initialize session state for tracking changes
if 'data_modified' not in st.session_state:
    st.session_state.data_modified = False

# Sidebar filters populated from data
groups = df["Group"].dropna().unique()
selected_group = st.sidebar.selectbox("Select Level-1 Group", groups)
subsystems = df[df["Group"] == selected_group]["Subsystem"].dropna().unique()
selected_subsystem = st.sidebar.selectbox("Select Subsystem", subsystems)

sub_df = df[(df["Group"] == selected_group) & (df["Subsystem"] == selected_subsystem)]

if sub_df.empty:
    st.warning("No pipeline records found. Please check your data source.")
    st.stop()

# Summary Metrics
col1, col2, col3, col4 = st.columns(4)
col1.metric("Finalized", int(sub_df[sub_df["StageKey"] == "finalized"]["Count"].sum()))
col2.metric("UAT", int(sub_df[sub_df["StageKey"] == "uat"]["Count"].sum()))
col3.metric("Planned", int(sub_df[sub_df["StageKey"] == "planned"]["Count"].sum()))
col4.metric("Production", int(sub_df[sub_df["StageKey"] == "production"]["Count"].sum()))

# Editable UI
node = get_subsystem_node(data, selected_group, selected_subsystem)
if node:
    st.header(f"Edit {selected_subsystem} Details")

    st.subheader("Pipeline Counts")
    for ptype in node.get("pipelines", {}):
        st.markdown(f"**{ptype.title()} Pipelines:**")
        cols = st.columns(len(STAGE_DISPLAY))
        for idx, stag in enumerate(STAGE_DISPLAY):
            val = node["pipelines"][ptype].get(stag, 0)
            new_val = cols[idx].number_input(
                f"{ptype.title()} - {STAGE_DISPLAY[stag]}", min_value=0, max_value=1000,
                step=1, value=val, key=f"{selected_subsystem}-{ptype}-{stag}",
                on_change=mark_data_modified
            )
            if new_val != val:
                node["pipelines"][ptype][stag] = new_val
                mark_data_modified()

    st.subheader("Contacts")
    all_people = ["Alice", "Bob", "Charlie", "Diana", "Eve", "Frank", "Grace", "Hank", "Ivy", "Jai", "Kim", "Lena", "Mo", "Nia", "Omar"]
    for ctype, label in [
        ("producerTech", "Producer Tech"),
        ("producerBusiness", "Producer Business"),
        ("ourTech", "Our Tech"),
        ("ourBusiness", "Our Business/PMO"),
    ]:
        new_contacts = st.multiselect(
            label, all_people, default=node["contacts"].get(ctype, []), key=f"{selected_subsystem}-{ctype}",
            on_change=mark_data_modified
        )
        if new_contacts != node["contacts"].get(ctype, []):
            node["contacts"][ctype] = new_contacts
            mark_data_modified()

    st.subheader("Issues / Obstacles (Editable)")
    issues = node.get("issues", [])
    edited_issues = []
    for i, issue in enumerate(issues):
        exp = st.expander(f"Issue {i+1}: [{issue['id']}]")
        with exp:
            new_desc = st.text_input(f"Description", issue["description"], key=f"{selected_subsystem}-desc{i}", on_change=mark_data_modified)
            new_status = st.selectbox("Status", ["Open", "In Progress", "Closed"], index=["Open", "In Progress", "Closed"].index(issue["status"]), key=f"{selected_subsystem}-status{i}", on_change=mark_data_modified)
            
            # Date fields
            col1, col2 = st.columns(2)
            with col1:
                start_date = st.date_input(
                    "Start Date",
                    value=datetime.strptime(issue.get("start_date", get_today_string()), "%Y-%m-%d").date() if issue.get("start_date") else date.today(),
                    key=f"{selected_subsystem}-start{i}"
                )
            with col2:
                close_date = st.date_input(
                    "Close Date",
                    value=datetime.strptime(issue.get("close_date", get_today_string()), "%Y-%m-%d").date() if issue.get("close_date") else None,
                    key=f"{selected_subsystem}-close{i}"
                )
            
            # Auto-close date when status is "Closed"
            if new_status == "Closed" and not close_date:
                close_date = date.today()
            
            # Validate date range
            if not validate_date_range(start_date, close_date):
                st.error("⚠️ Close date cannot be before start date!")
                continue
            
            edited_issues.append({
                "id": issue["id"], 
                "description": new_desc, 
                "status": new_status,
                "start_date": start_date.strftime("%Y-%m-%d"),
                "close_date": close_date.strftime("%Y-%m-%d") if close_date else None
            })
    
    # Check if issues were modified
    if edited_issues != node.get("issues", []):
        node["issues"] = edited_issues
        mark_data_modified()

    if st.button("Add New Issue"):
        node["issues"].append({
            "id": f"ISS-{100+len(node['issues'])}", 
            "description": "", 
            "status": "Open",
            "start_date": get_today_string(),
            "close_date": None
        })
        mark_data_modified()

    # Issues Summary Table
    if issues:
        st.subheader("📊 Issues Summary Table")
        
        # Prepare data for the table
        summary_data = []
        for issue in issues:
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
                "Status": "🟢 Closed" if issue["status"] == "Closed" else "🟡 In Progress" if issue["status"] == "In Progress" else "🔴 Open"
            })
        
        # Create summary DataFrame
        summary_df = pd.DataFrame(summary_data)
        
        # Display the table with styling
        st.dataframe(
            summary_df,
            column_config={
                "Age Indicator": st.column_config.TextColumn("Age", help="🟢 Recent (≤7 days), 🟡 Moderate (8-30 days), 🔴 Old (>30 days)"),
                "Status": st.column_config.TextColumn("Status", help="🟢 Closed, 🟡 In Progress, 🔴 Open"),
                "Blocked Days": st.column_config.NumberColumn("Days Blocked", help="Total days from start to close (or today if open)")
            },
            hide_index=True,
            use_container_width=True
        )
        
        # Summary statistics
        total_open = len([i for i in issues if i["status"] == "Open"])
        total_in_progress = len([i for i in issues if i["status"] == "In Progress"])
        total_closed = len([i for i in issues if i["status"] == "Closed"])
        total_blocked_days = sum([calculate_blocked_days(i.get("start_date", ""), i.get("close_date", "")) for i in issues])
        
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("🔴 Open Issues", total_open)
        col2.metric("🟡 In Progress", total_in_progress)
        col3.metric("🟢 Closed", total_closed)
        col4.metric("📅 Total Blocked Days", total_blocked_days)

    # Save and Download buttons
    col1, col2 = st.columns(2)
    
    with col1:
        # Show save status
        if st.session_state.data_modified:
            st.warning("⚠️ You have unsaved changes!")
        
        if st.button("💾 Save Changes", type="primary", disabled=not st.session_state.data_modified):
            if save_data(data):
                st.success("✅ Changes saved successfully! Data will persist after server restart.")
            else:
                st.error("❌ Failed to save changes. Please try again.")
    
    with col2:
        st.download_button(
            label="📥 Download JSON",
            file_name="data_edited.json",
            mime="application/json",
            data=json.dumps(data, indent=2),
        )

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
