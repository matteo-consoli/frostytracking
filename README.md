# FrostyTracking - Streamlit-Powered Audit Analytics in Snowflake
FrostyTracking is a Streamlit in Snowflake (SiS) dashboard, fairly easy to surf and it provides 3 default sections: 
- Login Tracking
- Query Tracking
- DDL Tracking

![alt text](https://github.com/matteo-consoli/frostytracking/blob/main/screenshot.png?raw=true)

The interesting aspect of this project is how to have dynamic filtering and parametrised queries getting inputs from the sidebar fields configuration area. It's possible to parametrize queries on:
- timeframe of the analysis
- definition of the top "n" results to extract
- possibility to filter out some users from the analysis.

### Streamlit in Snowflake Deployment
1) Download "frostytracking_sis.py" and "logo.png" from the GitHub repository.
2) Create a new Streamlit app on your Snowflake account
3) Paste the code into your new app.
4) Upload the "logo.png" in the Streamlit application stage.

NOTE: FrostyTracking is not intended to be a ready-to-use artifact but it merely provides a foundation on available features in Snowflake & Streamlit.
