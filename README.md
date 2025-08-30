# Pipeline Onboarding Dashboard

A comprehensive dashboard for managing pipeline onboarding processes, tracking pipeline details, and monitoring progress across different subsystems.

## Features

### 📊 Dashboard View
- **Pipeline Overview**: View pipeline counts by stage (Finalized, UAT, Planned, Production)
- **Visual Analytics**: Interactive charts showing streaming vs batch pipelines and stage distributions
- **Issue Tracking**: Monitor open issues, blocked days, and progress across subsystems

### ⚙️ Admin Panel
- **Pipeline Management**: Add and manage individual pipelines with detailed information
- **Excel Upload**: Bulk upload pipelines from Excel files
- **Contact Management**: Track producer and internal team contacts
- **Issue Management**: Create and track issues/obstacles

## Pipeline Types

### Batch Pipelines
Fields include:
- **Pipeline Name**: Actual name of pipeline
- **Data Name**: What business/producer team recognize this data flow
- **Frequency**: Frequency of file publish (daily, weekly, monthly)
- **Run Day**: Which day of week/month this file will be pushed
- **Run Timestamp**: Exact time of run
- **File Size**: File size in MB
- **UAT Date**: Planned UAT deploy date
- **PROD Date**: Planned PROD deploy date
- **UAT Status**: Current UAT status
- **PROD Status**: Current PROD status
- **Comment**: Additional comments or notes

### Streaming Pipelines
Fields include:
- **Pipeline Name**: Actual name of pipeline
- **Data Name**: What business/producer team recognize this data flow
- **Start Time**: Streaming start time
- **End Time**: Streaming end time
- **Run Day**: Which day of week/month this file will be pushed
- **Rough Volume**: Total size in MB between start and end time
- **UAT Date**: Planned UAT deploy date
- **PROD Date**: Planned PROD deploy date
- **UAT Status**: Current UAT status
- **PROD Status**: Current PROD status
- **Comment**: Additional comments or notes

## Getting Started

### Installation

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Run the application:
```bash
streamlit run pipeline_dashboard.py
```

### Adding Pipelines

#### Single Pipeline Addition
1. Navigate to the Admin Panel
2. Select the pipeline type (Batch or Streaming)
3. Use the "Add Single Pipeline" form to enter pipeline details
4. Fill in required fields (marked with *)
5. Click "Add [Type] Pipeline" to save

#### Excel Bulk Upload
1. Download the appropriate template:
   - `batch_pipeline_template.xlsx` for batch pipelines
   - `streaming_pipeline_template.xlsx` for streaming pipelines
2. Fill in the Excel file with your pipeline data
3. Upload the file using the "Upload Excel File" section
4. Review the preview and click "Process Excel Upload"

### Excel Template Format

Both templates include sample data showing the correct format. Make sure your Excel file includes all required columns with the exact column names as shown in the templates.

## Data Management

- **Auto-Save**: Changes are automatically tracked and require manual saving
- **Data Persistence**: All data is stored in `data.json`
- **Backup**: Use the "Download JSON" button to create backups
- **Pipeline Counts**: Automatically updated based on actual pipeline details and their status

## File Structure

```
project-planner-dashboard/
├── pipeline_dashboard.py          # Main application
├── data.json                      # Data storage
├── requirements.txt               # Python dependencies
├── batch_pipeline_template.xlsx   # Batch pipeline Excel template
├── streaming_pipeline_template.xlsx # Streaming pipeline Excel template
├── create_excel_templates.py     # Script to generate templates
└── README.md                      # This file
```

## Status Mapping

Pipeline counts are automatically calculated based on the UAT Status field:
- **Planned**: Status contains "plan", "draft", or "planned"
- **UAT**: Status contains "uat", "testing", or "in progress"
- **Production**: Status contains "prod", "production", or "completed"
- **Finalized**: All other statuses

## Support

For questions or issues, please refer to the tooltips in the application interface or contact the development team.
