
import pandas as pd
import re
from pathlib import Path
from typing import Protocol
from models.experiment import Experiment
from models.participant import Participant
from models.measurement import Measurement
from models.datapoint import Datapoint

# Define an Interface (Protocol) for Parser
class DataParser(Protocol):
    def parse(self, file_path: str) -> dict:
        """
        TODO TO BE IMPLEMENTED
        """
        ...


# Beispielhafte Parser-Implementierung für Format A
class FormatRawMPAParser:
    def extract_mpa_raw_measurement_info(self, data, column):
        """
        Extract participant and pre or post measurement from the raw data

        Args:
            data (Dataframe): The loaded dataframe 
            column (int): the column id of the current datapoints
        
        Returns:
            str: The participant id
            int: Pre- or post measurement (0 is pre measurement, 1 is post)
        """
        column_name = data.columns[column]
        
        # Muster definieren
        pattern_participant = r"P\d{3}"           # Für PXXX
        pattern_pre_post = r"_(0[1-2])_"     # Für die Zahl 01 oder 02
        
        # Extraktion
        participant_match = re.search(pattern_participant, column_name)
        pre_post_match = re.search(pattern_pre_post, column_name)
        
        participant = participant_match.group() if participant_match else None
        pre_post = pre_post_match.group(1)[1] if pre_post_match else None
        pre_post = int(pre_post)-1
        
        return participant, pre_post
    
    def extract_mpa_raw_dp_info(self, data, bow_stroke, column):
        """
        Extracts information about location, up/down movement, axis and the 100 datapoints (per up/ down movement) from the raw data. Creates combined datapoints from this information

        Args:
            data (Dataframe): The loaded dataframe 
            bow_stroke (int): current bow stroke that corresponds with the datapoints
            column (int): the column id of the current datapoints
        
        Returns:
            list: A list of the combined datapoints
        """
        up_down = (column-1)%2
        location = data.iloc[:, column][0]
        axis = data.iloc[:, column][3]
        datapoints = list(data.iloc[:, column][4:data.shape[0]])


        data_points = [
            (location, axis, bow_stroke, up_down, j, value)
            for j, value in enumerate(datapoints)
                ]
        
        return data_points   
       
    def parse(self, file_path: str) -> dict:
        
        source = "mocap" if ('joints' in str(file_path)) | ('JOINT' in str(file_path)) else 'emg'
        data_state = 'raw'
        experiment_name = 'mpa'
        data = pd.read_excel(self.file_path)
        if data.empty:
            return []
        else:
            last_participant = ''
            last_pre_post = None
            bow_stroke = 0
            for i in range (1, data.shape[1]):
                participant, pre_post = self.extract_mpa_raw_measurement_info(data, i) 
                if (participant != last_participant) | (pre_post != last_pre_post):
                    extracted_data = self.extract_mpa_raw_dp_info(data, bow_stroke, i)
                    
                    experiment_data = (experiment_name, data_state)
                    participant_data = participant
                    measurement_data = (participant, pre_post, source, )
   
                    last_participant = participant    
                    last_pre_post = pre_post   
                    bow_stroke = 0   

        return {
            "experiments": [...],
            "participants": [...],
            "measurements": [...],
            "datapoints": [...],
        }

# Beispielhafte Parser-Implementierung für Format B
class FormatCleanMPAParser:
        
    def parse_experiment(self, file_path: str) -> dict:

        #file_path = Path(file_locations[0])  # Use Path for consistency

        # Infer source and unit
        #file_name = file_path.name.lower()
        # Parsen von Format B, z.B. 100 Datenpunkte pro Spalte etc.
        source = "mocap" if ("joints" in file_path) or ("JOINT" in file_path) else "emg"
        unit = "degree" if (source == "mocap") else "mV"
        data_state = "clean"
        experiment_name = "mpa"

        #df = pd.read_csv(file_path, delimiter='\t')
#
        #if df.empty:
        #    return {"experiments": []}

        experiments = []

        experiment_ext_id = f"{experiment_name}_{data_state}"

        # Nur ein Experimentobjekt in diesem Fall
        experiments.append(Experiment(
            id=experiment_ext_id,
            name=experiment_name,
            data_state=data_state
        ))     

        return {
            "experiments": experiments,
        }
    
    def parse(self, file_path: str) -> dict:
        # Parsen von Format B, z.B. 100 Datenpunkte pro Spalte etc.
        source = "mocap" if ("joints" in file_path) or ("JOINT" in file_path) else "emg"
        unit = "degree" if (source == "mocap") else "mV"
        data_state = "clean"
        experiment_name = "mpa"

        df = pd.read_csv(file_path, delimiter='\t')

        if df.empty:
            return {"experiments": [], "participants": [], "measurements": [], "datapoints": []}

        experiments = []
        participants = []
        measurements = []
        datapoints = []

        experiment_ext_id = f"{experiment_name}_{data_state}"

        # Nur ein Experimentobjekt in diesem Fall
        experiments.append(Experiment(
            id=experiment_ext_id,
            name=experiment_name,
            data_state=data_state
        ))

        for subject_id in df["subject"].unique():
            participant_id = f"{experiment_ext_id}_{subject_id}"
            participants.append(Participant(
                id=participant_id, participant_id=subject_id,
                experiment_id=experiment_ext_id
            ))

            for pre_post in [0, 1]:
                meas_ext_id = f"{participant_id}_prepost_{pre_post}"
                data_part = df[(df["subject"] == subject_id) & (df["pre_post"] == pre_post)].copy()

                if data_part.empty:
                    continue

                # Beispielhafte Info-Extraktion (kann angepasst werden)
                location = self._extract_location(file_path)
                axis = None
                if source == 'mocap':
                    axis = self._extract_axis(file_path)

                measurements.append(Measurement(
                    id=meas_ext_id,
                    participant_id=participant_id,
                    timepoint="pre" if pre_post == 0 else "post",
                    device = source,
                    target = location,
                    axis = axis,
                    unit = unit
                ))
                
                
                for index, row in data_part.iterrows():
                    datapoints.append(Datapoint(
                        id = index,
                        measurement_id=meas_ext_id,
                        bow_stroke=row.get("bow_stroke"),
                        up_down=row.get("up_down"),
                        key=row.get("key"),
                        time_point=int(row["point"]),
                        value=float(row["value"]),
                    ))       

        return {
            "experiments": experiments,
            "participants": participants,
            "measurements": measurements,
            "datapoints": datapoints,
        }
    
    @staticmethod
    def _extract_location(file_path:str) -> str:
        # TODO: fix the location extraction for EMG
        # Remove the axis and extension (e.g., "_Z.tsv")
        file_name = Path(file_path).name
        name = re.sub(r'_[XYZ]\.tsv$', '', file_name)
        # Replace underscores with spaces and convert to lower case
        return name.replace('_', ' ').lower()
    @staticmethod
    def _extract_axis(file_path):
        file_path = Path(file_path)        # e.g., 'LEFT_ELBOW_JOINT_ANGLE_A.tsv'
        last_letter = file_path.stem[-1]    # e.g., 'LEFT_ELBOW_JOINT_ANGLE_A'
        return last_letter

class FormatRawRefLabParser:
    def parse(self, file_path: str) -> dict:
        """TODO: TO BE IMPLEMENTED"""
        ...
        return {
            "experiments": [...],       
            "participants": [...],      
            "measurements": [...],      
            "datapoints": [...],        
        }

# Beispielhafte Parser-Implementierung für Format B
class FormatCleanRefLabParser:
    def parse(self, file_path: str) -> dict:
        """TODO: TO BE IMPLEMENTED"""
        ...
        return {
            "experiments": [...],
            "participants": [...],
            "measurements": [...],
            "datapoints": [...],
        }


class FormatPainMPAParser:
   
    def parse(self, file_path: str) -> dict:
        # Parsen von Format B, z.B. 100 Datenpunkte pro Spalte etc.
        pain_data = pd.read_excel(file_path)
        
        mapping = {
        1: 'Violin',
        2: 'Viola',
        3: 'Both', 
        10: 'Violin',
    }

        pain_data['Erstes_Instrument'] = pain_data['Erstes_Instrument'].replace(mapping)
        
        participants = []
        for _,row in pain_data.iterrows():
            
            if row[0]<10:
                subject_id = 'P00' + str(row["Probanden_ID"])
            else:
                subject_id = 'P0' + str(row["Probanden_ID"])
            
            #subject_id = 'P00' + str(row["Probanden_ID"])
            participant_ext_id = f"mpa_pain_{subject_id}"
            participants.append(Participant(
                id=participant_ext_id, 
                participant_id=subject_id,
                experiment_id='mpa_pain', instrument = row['Erstes_Instrument'], 
                PRMD_shoulder_neck_right = row['PRMD_Schulter_Nacken_rechts'], 
                PRMD_shoulder_neck_left = row['PRMD_Schulter_Nacken_links'], 
                PRMD_upper_arm_right = row['PRMD_Oberarm_rechts'], PRMD_upper_arm_left = row['PRMD_Oberarm_links'], 
                PRMD_ever = row['Schmerzen_jemals']
            ))
        
        #pain_data_as_lists = pain_data.values.tolist()
        #for row in pain_data_as_lists:
        #    if row[0]<10:
        #        participant_ID = 'P00' + str(row[0])
        #    else:
        #        participant_ID = 'P0' + str(row[0])
        #    
        #    #datapoint = row[1:] + [experiment, participant_ID]
        #    #datapoint.append(experiment)
        #    #datapoint.append(participant_ID)
        #    #self.db_manager.update_pain_data(datapoint)
        
        print("")
        
        return {
            "participants": participants
        }
    