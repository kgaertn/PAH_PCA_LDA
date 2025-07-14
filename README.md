# PAH_PCA_LDA

## Project Structure
### PAH_PCA_LDA/
 ├── data/                           # database location
 ├── output/                         # output folder for plots and other output files
 ├── src/
 │   ├── main.py     
 │   ├── core/                       # Layer for combining the logic of data processing and analysis
 │   │   └── data_parser.py   
 │   │   └── uploader_service.py      
 │   │   └── setup_and_upload.py   
 │   │   └── setup_analyse_pca.py
 │   │   └── reconstruct_pca.py
 │   ├── data_processing/                # Data processing Layer (These classes the data (pre-)processing)
 │   │   └── data_preprocess.py
 │   │   └── pca_analysis.py
 │   │   └── lda_analysis.py              
 │   ├── data_access/                # Data Access Layer (These classes handle the database access)
 │   │   └── experiment_repository.py
 │   │   └── participant_repository.py
 │   │   └── measurement_repository.py
 │   │   └── sample_repository.py
 │   │   └── datapoint_repository.py
 │   │   └── scaler_repository.py
 │   │   └── pc_ranked_repository.py
 │   │   └── pc_scores_repository.py
 │   ├── models/                     # data models (corresponding to the database tables)
 │   │   └── experiment.py
 │   │   └── participant.py
 │   │   └── measurement.py
 │   │   └── sample.py
 │   │   └── datapoint.py
 │   │   └── scaler.py
 │   │   └── pc_ranked.py
 │   │   └── pc_scores.py
 │   └── db/                         # database connection
 │       └── connection.py
 │       └── setup.py
 ├── .gitignore
 ├── requirements.txt
 └── README.md

## Database Structure
### experiment
 ├── id (PK)                     # (int) this is an internal database id, used as a unique identifier/private key (PK) of the experiment
 ├── name                        # (str) this is the name of the experiment in lower case letters (e.g. 'mpa')
 ├── data_state                  # (str) this is a description of the data state, 'clean' or 'raw'
 ├── data_folder                 # (str) this is a relative location of the original folder path (from Sample_Data_PAH)
 ├── upload_complete             # (bool/int) 1 if the upload of the experiment is complete, 0 or None if not complete
 
### participant
 ├── id (PK)                     # (int) this is an internal database id, used as a PK of the participant
 ├── experiment_id (FK)          # (int) this is a reference to the PK of experiment table
 ├── participant_id              # (str) this is the actual participant_id, given by the experimenters
 ├── age                         # (int) optional variable to store the age (currently empty)
 ├── height_cm                   # (float) optional variable to store the height (currently empty)
 ├── weight_kg                   # (float) optional variable to store the weight (currently empty)
 ├── instrument                  # (str) the instrument name the participant played
 ├── PRMD_shoulder_neck_right    # (bool) whether the participant experienced pain in the right shoulder/neck (0 = no, 1 = yes)
 ├── PRMD_shoulder_neck_left     # (bool) whether the participant experienced pain in the left shoulder/neck (0 = no, 1 = yes)
 ├── PRMD_upper_arm_right        # (bool) whether the participant experienced pain in the right upper arm (0 = no, 1 = yes)
 ├── PRMD_upper_arm_left         # (bool) whether the participant experienced pain in the left upper arm (0 = no, 1 = yes)
 ├── PRMD_ever                   # (bool) whether the participant experienced pain ever (can also be for other categories, e.g. lower extremity) (0 = no, 1 = yes)
 
### measurement
 ├── id (PK)                     # (int) this is an internal database id, PK of the measurement
 ├── participant_id (FK)         # (int) this is a reference to the PK of participant table
 ├── timepoint                   # (str) this is the timepoint (t0, t1, etc.) of the measurement (e.g. 'pre' or 'post')
 ├── device                      # (str) this is the name of the measurement method (e.g. 'mocap' or 'emg')
 ├── target                      # (str) this is the name of the 'location' of the measurement (e.g. 'left elbow joint angle')
 ├── axis                        # (str) this is the axis of the measurement (e.g. 'X'); N/A for emg!
 ├── unit                        # (str) this is the unit of the measurement (e.g. 'degree' for motion capture)
 ├── measurement_type_id (FK)    # (int) this is a reference to the PK of the measurement_type table
 
### measurement_type
 ├── id (PK)                     # (int) this is an internal database id, PK of the measurement_type
 ├── experiment_id (FK)          # (int) this is a reference to the PK of experiment table
 ├── meas_time_point             # (str) this is the timepoint (t0, t1, etc.) of the measurement (e.g. 'pre' or 'post')
 ├── device                      # (str) this is the name of the measurement method (e.g. 'mocap' or 'emg')
 ├── target                      # (str) this is the name of the 'location' of the measurement (e.g. 'left elbow joint angle')
 ├── axis                        # (str) this is the axis of the measurement (e.g. 'X'); 'N/A' for emg
 ├── rotation_sequence           # (str) this is the translation of the axis into the actual movement, can be used to sort out carrying angles
 (experiment_id, device, meas_time_point, target, axis) must be unique!

### sample
 ├── id (PK)                     # (int) this is an internal database id, PK of the sample
 ├── measurement_id (FK)         # (int) this is a reference to the PK of measurement table
 ├── bow_stroke_start            # (int) this is the number of the first bow stroke of each sample (per participant/measurement)
 ├── bow_stroke_end              # (int) this is the number of the last bow stroke for this sample
 [measurement_id, bow_stroke_start] must be unique!

### datapoint
 ├── id (PK)                     # (int) this is an internal database id, PK of the datapoint
 ├── measurement_id (FK)         # (int) this is a reference to the PK of measurement table
 ├── bow_stroke                  # (int) this is the number of the bow stroke (per participant/measurement)
 ├── up_down                     # (bool) whether it is an up- or down-stroke (0 = 'up', 1 = 'down')
 ├── time_point                  # (int) the timepoint of the bowstroke (101 timepoints per bowstroke)
 ├── value                       # (float) value for the specific timepoint
 ├── sample_id                   # (int) this is a reference to the PK of the sample table

### scaler
 ├── scaler_id (PK)              # (int) this is an internal database id, PK of the scaler
 ├── measurement_type_id (FK)    # (int) this is a reference to the PK of measurement_type table
 ├── scaler_type                 # (str) this is a description of the type of scaler that was used for the data (e.g. 'standard_scaler')
 ├── mean                        # (str) this is the list of mean values for the scaler, in json string format (can be transverted back into a list[float] 
                                 # using the class function of scaler.py)
 ├── scale                       # (str) this is the list of scale values for the scaler, in json string format (can be transverted back into a list[float] 
                                 # using the class function of scaler.py)
 ├── created_at                  # timestamp for when the scaler was created (auto-generated)
 (measurement_type_id, scaler_type) must be unique!

### pcs_ranked
 ├── id (PK)                     # (int) this is an internal database id, PK of the principal component (pc)
 ├── measurement_type_id (FK)    # (int) this is a reference to the PK of measurement_type table
 ├── scaler_id (FK)              # (int) this is a reference to the PK of scaler table
 ├── pc_index                    # (int) this is the component index of the pc (e.g. 1 for PC1, 2 for PC2, etc.)
 ├── loading_vector              # (str) this is the list of values for the loading vector, in json string format (can be transverted back into a list[float] 
                                 # using the class function of pc_ranked.py)
 ├── rank                        # (int) the rank assigned to the component by the t-test (sorted by absolute t-value)
 ├── explained_variance          # (float) the explained variance for this component
 ├── group_mean_pain             # (float) calculated group mean for the pc scores of this component for pain participants 
 ├── group_mean_no_pain          # (float) calculated group mean for the pc scores of this component for no pain participants 
 ├── group_std_pain              # (float) calculated group std for the pc scores of this component for pain participants 
 ├── group_std_no_pain           # (float) calculated group std for the pc scores of this component for no pain participants 
 ├── t_value                     # (float) t-test calculated t_value for the pc scores of this component
 ├── p_value                     # (float) t-test calculated p_value for the pc scores of this component
 ├── pca_info                    # (str) column for additional info about the conducted pca
 ├── distribution_info           # (str) column for info about the distribution of pc scores for this component (e.g. 'normal_distribution')
 ├── shap_wilk_w_pain            # (float) calculated w value of the shapiro wilk test for the pain population
 ├── shap_wilk_w_no_pain         # (float) calculated w value of the shapiro wilk test for the no pain population
 ├── shap_wilk_p_pain            # (float) calculated p value of the shapiro wilk test for the pain population
 ├── shap_wilk_p_no_pain         # (float) calculated p value of the shapiro wilk test for the no pain population
 (measurement_type_id, scaler_id, pc_index, data_scaled) must be unique!

### pc_scores
 ├── id (PK)                     # (int) this is an internal database id, PK of the pc_scores
 ├── sample_id (FK)              # (int) this is a reference to the PK of sample table
 ├── pc_id (FK)                  # (int) this is a reference to the PK of pcs_ranked table
 ├── pc_score                    # (float) the pc score for this component/sample

## Database Views

### datapoints_adjusted: 
View of the datapoint table, time_point is adjusted to combine half cycles into full cycles (202 timepoints per sample, instead of 101 -> up and down stroke are combined, only full cycles are kept)
### Datapoints MPA Clean: 
View of the datapoints of the MPA clean experiment, joins the tables participant, measurement, measurement_type, sample und datapoints_adjusted
### Participants PCs:
View of the PCs and scores combined with experiment meta data, joins the tables participant, measurement, measurement_type, pcs_ranked und pc_scores