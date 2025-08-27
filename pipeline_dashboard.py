import streamlit as st
import json
import pandas as pd
import plotly.express as px
from pathlib import Path

# ----------------------------
# Minimal deps (for requirements.txt)
#   streamlit>=1.36
#   pandas>=2.0
#   plotly>=5.22
# ----------------------------

# ----------------------------
# Sample data used if data.json is missing
# ----------------------------
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
                        {"id": "ISS-101", "description": "Data delay from source", "status": "Open"},
                        {"id": "ISS-102", "description": "Schema mismatch on v2", "status": "In Progress"}
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
                        {"id": "ISS-201", "description": "Access to UAT blocked", "status": "Open"}
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

# ----------------------------
# Load JSON Data
# ----------------------------

def load_data(file_path: str = "data.json") -> dict:
    p = Path(file_path)
    if p.exists():
        with p.open("r", encoding="utf-8") as f:
            return json.load(f)
    # Fallback to sample if file not found
    return SAMPLE_DATA

# Flatten JSON into DataFrame for easy plotting

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

# ----------------------------
# Streamlit App
# ----------------------------
st.set_page_config(page_title="Pipeline Onboarding Dashboard", layout="wide")
st.title("📊 Pipeline Onboarding Dashboard")

with st.sidebar:
    st.caption("Upload a JSON to override the default sample. Edit your JSON and refresh to see changes.")
    uploaded_file = st.file_uploader("Upload JSON", type="json")

if uploaded_file:
    data = json.load(uploaded_file)
else:
    data = load_data()  # default JSON file or SAMPLE_DATA

df = extract_pipelines(data)

if df.empty:
    st.warning("No pipeline records found. Please upload a valid JSON.")
    st.stop()

# Sidebar filters populated from data
groups = df["Group"].dropna().unique()
selected_group = st.sidebar.selectbox("Select Level-1 Group", groups, index=0 if len(groups) else None)
subsystems = df[df["Group"] == selected_group]["Subsystem"].dropna().unique()
selected_subsystem = st.sidebar.selectbox("Select Subsystem", subsystems, index=0 if len(subsystems) else None)

# Filtered view
sub_df = df[(df["Group"] == selected_group) & (df["Subsystem"] == selected_subsystem)]

# ----------------------------
# Summary Metrics
# ----------------------------
col1, col2, col3, col4 = st.columns(4)
col1.metric("Finalized", int(sub_df[sub_df["StageKey"] == "finalized"]["Count"].sum()))
col2.metric("UAT", int(sub_df[sub_df["StageKey"] == "uat"]["Count"].sum()))
col3.metric("Planned", int(sub_df[sub_df["StageKey"] == "planned"]["Count"].sum()))
col4.metric("Production", int(sub_df[sub_df["StageKey"] == "production"]["Count"].sum()))

# Team engagement (from contacts)
node = get_subsystem_node(data, selected_group, selected_subsystem)
if node:
    contacts = node.get("contacts", {})
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Producer Tech", len(contacts.get("producerTech", [])))
    c2.metric("Producer Business", len(contacts.get("producerBusiness", [])))
    c3.metric("Our Tech", len(contacts.get("ourTech", [])))
    c4.metric("Our Business/PMO", len(contacts.get("ourBusiness", [])))

# ----------------------------
# Charts
# ----------------------------
colA, colB = st.columns(2)

# Pipeline Type Split (Streaming vs Batch)
fig1 = px.pie(sub_df, values="Count", names="PipelineType", title="Streaming vs Batch")
colA.plotly_chart(fig1, use_container_width=True)

# Stage Distribution (stacked by type)
fig2 = px.bar(
    sub_df,
    x="Stage",
    y="Count",
    color="PipelineType",
    barmode="stack",
    title="Pipelines by Stage (Stacked)",
)
colB.plotly_chart(fig2, use_container_width=True)

# Group-wide: Subsystems vs total pipelines
group_df = df[df["Group"] == selected_group].groupby(["Subsystem"])['Count'].sum().reset_index()
st.plotly_chart(px.bar(group_df, x="Subsystem", y="Count", title=f"Total Pipelines per Subsystem in {selected_group}"), use_container_width=True)

# ----------------------------
# Contacts & Issues
# ----------------------------
if node:
    st.subheader(f"Contacts for {selected_subsystem}")
    st.json(contacts)

    st.subheader("Issues / Obstacles")
    issues = node.get("issues", [])
    if issues:
        st.dataframe(pd.DataFrame(issues))
    else:
        st.info("No issues reported.")

# ----------------------------
# Utilities: Download current data as JSON
# ----------------------------
st.download_button(
    label="Download Current Data JSON",
    file_name="data.json",
    mime="application/json",
    data=json.dumps(data, indent=2),
)
