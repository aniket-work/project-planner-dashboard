# project-planner-dashboard
project-planner-dashboard


# 1) create venv (optional)
python -m venv .venv && .venv/Scripts/activate

# 2) install deps
pip install -r requirements.txt

# 3) put data.json beside dashboard.py (or upload via the app)
# 4) launch
streamlit run pipeline_dashboard.py
