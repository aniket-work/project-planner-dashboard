import pandas as pd

# Create sample batch pipeline Excel template
batch_data = {
    "pipeline_name": ["Daily Sales Report", "Weekly Inventory Update"],
    "data_name": ["sales_daily.csv", "inventory_weekly.xlsx"],
    "frequency": ["daily", "weekly"],
    "run_day": ["Monday", "Friday"],
    "run_timestamp": ["09:00", "15:30"],
    "file_size": ["50", "120"],
    "uat_date": ["2024-01-15", "2024-01-20"],
    "prod_date": ["2024-01-25", "2024-01-30"],
    "uat_status": ["completed", "in progress"],
    "prod_status": ["planned", "planned"],
    "comment": ["Daily sales aggregation", "Weekly inventory snapshot"]
}

# Create sample streaming pipeline Excel template
streaming_data = {
    "pipeline_name": ["Real-time User Analytics", "Live Sensor Data"],
    "data_name": ["user_events.json", "sensor_readings.xml"],
    "start_time": ["00:00", "08:00"],
    "end_time": ["23:59", "18:00"],
    "run_day": ["Daily", "Weekdays"],
    "rough_volume": ["2000", "500"],
    "uat_date": ["2024-01-10", "2024-01-12"],
    "prod_date": ["2024-01-20", "2024-01-22"],
    "uat_status": ["completed", "in progress"],
    "prod_status": ["completed", "planned"],
    "comment": ["User behavior tracking", "IoT sensor monitoring"]
}

# Create Excel files
batch_df = pd.DataFrame(batch_data)
streaming_df = pd.DataFrame(streaming_data)

batch_df.to_excel("batch_pipeline_template.xlsx", index=False)
streaming_df.to_excel("streaming_pipeline_template.xlsx", index=False)

print("Excel templates created:")
print("- batch_pipeline_template.xlsx")
print("- streaming_pipeline_template.xlsx")
