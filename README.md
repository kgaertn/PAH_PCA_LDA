# PAH_PCA_LDA

## Structure
### PAH_PCA_LDA/
### ├── data/                           # database location
### ├── src/
### │   ├── main.py                     
### │   ├── data_access/                # Data Access Layer (These classes handle the database access)
### │   │   └── experiment_repository.py
### │   │   └── participant_repository.py
### │   │   └── measurement_repository.py
### │   │   └── datapoint_repository.py
### │   ├── models/                     # data models (corresponding to the database tables)
### │   │   └── experiment.py
### │   │   └── participant.py
### │   │   └── measurement.py
### │   │   └── datapoint.py
### │   └── db/                         # database connection
### │       └── connection.py
### ├── .gitignore
### ├── requirements.txt
### └── README.md