London Bicycle Hires Analysis Dashboard

A Streamlit web application for analysing capacity optimisation opportunities in London's Santander Cycle Hire Scheme.

This dashboard provides comprehensive analysis of London's bicycle hire system, focusing on identifying and solving capacity issues that impact user experience. The application analyses historical bike hire data from the Greater London Authority (available in Google BigQuery's public datasets) to identify problematic stations, usage patterns, and system imbalances.


Data Source

The application uses the public London bicycle hire dataset available in Google BigQuery:

    bigquery-public-data.london_bicycles.cycle_hire
    bigquery-public-data.london_bicycles.cycle_stations


Requirements

    Python 3.8+
    Streamlit
    Google Cloud BigQuery client
    Pandas
    NumPy
    Altair

See requirements.txt for complete dependencies.

Setup and Installation

    1. Clone this repository:

        git clone https://github.com/niu02/London-Bike-Analysis.git
        cd London-Bike-Analysis

    2. Install required packages:

        pip install -r requirements.txt

    3. Authenticate with Google Cloud:
    
        Ensure you have installed the Google Cloud CLI
        If needed, initialise gcloud by running "gcloud init" in your terminal to configure the project
        Authenticate with your user account by running "gcloud auth application-default login"

    4. Run the Streamlit app:

        streamlit run app.py

Usage

    1. Use the date selector in the sidebar to choose your analysis period
    2. Select the appropriate analysis interval (Daily, Weekly, Monthly, etc.)
    3. Explore the capacity issues section to identify problematic stations
    4. Select specific stations for detailed hourly analysis
    5. Review system imbalance data to understand bike redistribution needs
    6. Consider the recommended solutions for implementation

Project Structure

    London-Bike-Analysis/
    ├── app.py                # Main Streamlit application
    ├── requirements.txt      # Project dependencies
    └── README.md             # Project documentation

