# FrostyTracking - Streamlit-Powered Audit Analytics in Snowflake
FrostyTracking provides a SiS (Streamlit in Snowflake) skeleton dashboard that you can customize according to your pattern monitoring requirements.

FrostyTracking, at least within this proposed prototype, is fairly easy to walk through. It provides 3 default sections:
- Login Tracking
- Query Tracking
- DDL Tracking

![alt text](https://github.com/matteo-consoli/frostytracking/blob/main/screenshot.png?raw=true)

The interesting aspect of this dashboard goes beyond the simple data represented. For this use case, input fields on the left side are used to apply dynamic filtering on the queries behind the shown graphs. Graphs are automatically updated according to:
- the timeframe of the analysis, 
- definition of the top "n" results to extract,
- the possibility of filtering out some users from the analysis.

### Streamlit in Snowflake Deployment
1) Download "frostytracking_sis.py" and "logo.png" from the GitHub repository.
2) Create a new Streamlit app on your Snowflake account
3) Paste the code into your new app.
4) Upload the "logo.png" in the Streamlit application stage.

NOTE: FrostyTracking is not intended to be a ready-to-use artifact but it merely provides a foundation on available features in Snowflake & Streamlit.
