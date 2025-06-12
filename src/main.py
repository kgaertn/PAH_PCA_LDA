from data_access.experiment_repository import ExperimentRepository
from data_access.participant_repository import ParticipantRepository
from data_access.measurement_repository import MeasurementRepository
from data_access.datapoint_repository import DatapointRepository

# intialize the database repos
experiment_repo = ExperimentRepository()
participant_repo = ParticipantRepository()
measurement_repo = MeasurementRepository()
datapoint_repo = DatapointRepository()          # this should be enough, if you only want to access the actual values

###########################################################################################################################
#                                           EXAMPLE DATA ACCESS
###########################################################################################################################
# load the data from the database (there are different functions for different data-subsets.
#       more functions for data access can be created, if needed)
exp_id = experiment_repo.get_experiment_id_by_name_and_data_state('mpa', 'clean')           # selects the database id from the database, 
                                                                                            # that corresponds to the experiment with 'clean mpa' data (you can also replace it with 1, as there is currently only one id)  

df = datapoint_repo.get_datapoints_by_exp_id_device_and_timepoint(exp_id, 'mocap', 'pre')   # selects all datapoints for mocap of the pre-measurement

df_nopain = datapoint_repo.get_datapoints_nopain_by_exp_id_device_and_timepoint(exp_id, 'mocap', 'pre') # selects all datapoints for mocap of the pre-measurement, 
                                                                                                        # but without the pain information 
meas_target = measurement_repo.get_measurement_target_by_id(1)                                          # selects a target (e.g. 'left elbow joint angle', by the measurement id, you can also replace this variable with the actual name of the target)                                                 

df_target = datapoint_repo.get_datapoints_by_exp_id_device_timepoint_target(exp_id, 'mocap', 'pre', meas_target)    # selects all datapoints one target for mocap of the pre-measurement, 

print("")



