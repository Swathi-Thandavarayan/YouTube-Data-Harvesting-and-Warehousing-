# YouTube-Data-Harvesting-and-Warehousing
This project aims to harvest data from YouTube using the YouTube Data API, process it, and store it in a MongoDB data lake. The processed data is then migrated to MySQL for structured storage. The web interface is built using Streamlit to provide a user-friendly experience.

## Table of Contents
-Overview
- Features
- Prerequisites
- Installation
- Configuration.
- Usage
- Technologies Used

## Overview

This project focuses on collecting YouTube data, including information about channels, playlists, videos, and comments. The collected data is stored initially in a MongoDB data lake for its flexibility with unstructured data. Later, the processed data is migrated to MySQL to provide a structured and relational representation.

## Features

- **YouTube Data Harvesting:** Utilize the YouTube Data API to fetch information about channels, playlists, videos, and comments.

- **Data Processing:** Clean and preprocess the harvested data for optimal storage and analysis.

- **MongoDB :** Store the processed data in a MongoDB database, taking advantage of its ability to handle unstructured and semi-structured data.

- **MySQL Migration:** Move the data to a MySQL database for structured storage and relational representation.

- **Web Interface with Streamlit:** Provide a user-friendly interface for interacting with the project using Streamlit.

## Prerequisites

Before you begin, ensure you have the following prerequisites installed:

- Python
- MongoDB
- MySQL
- Google API credentials for the YouTube Data API

## Configuration
Obtain Google API credentials for the YouTube Data API.
Create configuration files for MongoDB and MySQL connections.
Update the configuration files with your API credentials and connection details.

## Usage
Run the Streamlit web interface:
streamlit run app.py4
Access the interface in your web browser at http://localhost:8501.
Select the channels, playlists, or videos you want to harvest and migrate to MySQL.
Monitor the progress and view the results.

## Technology used
Python
Google API (YouTube Data API)
MongoDB
MySQL
Streamlit.


