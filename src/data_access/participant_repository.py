import pandas as pd
from db.connection import get_connection
from models.participant import Participant

class ParticipantRepository:
    def __init__(self):
        """
        Initializes the ParticipantRepository with a database connection.
        """
        self.conn = get_connection()

# region Setter
    def insert_participant(self, participant: Participant):
        """
        Inserts a new participant into the database for a specific experiment.

        Args:
            participant (Participant): A Participant object containing the external experiment ID
                and participant identifier (e.g., 'P001').

        Returns:
            int: The ID of the newly inserted participant record.
        """
        cursor = self.conn.cursor()
        cursor.execute("""
            INSERT INTO participant (experiment_id, participant_id)
            VALUES (?, ?)
        """, (participant.ext_experiment_id, participant.participant_id))
        self.conn.commit()
        return cursor.lastrowid

    def update_pain_data(self, participant: Participant, exp_id):
        """
        Updates pain-related fields for a specific participant in a given experiment.

        Args:
            participant (Participant): A Participant object containing the pain data fields to update:
                - instrument
                - PRMD_shoulder_neck_right
                - PRMD_shoulder_neck_left
                - PRMD_upper_arm_right
                - PRMD_upper_arm_left
                - PRMD_ever
                and the participant identifier (`participant_id`).
            exp_id (int): The ID of the experiment the participant belongs to.

        Returns:
            None
        """
        
        cursor = self.conn.cursor()
        cursor.execute("""
            UPDATE participant
            SET instrument = ?, PRMD_shoulder_neck_right = ?, PRMD_shoulder_neck_left = ?, 
            PRMD_upper_arm_right = ?, PRMD_upper_arm_left = ?, PRMD_ever = ?
            WHERE experiment_id = ? AND participant_id = ?
        """, (participant.instrument, participant.PRMD_shoulder_neck_right, participant.PRMD_shoulder_neck_left,
              participant.PRMD_upper_arm_right, participant.PRMD_upper_arm_left, participant.PRMD_ever,
              exp_id, participant.participant_id))
        self.conn.commit()        
# endregion Setter

#region Getter
# use these functions to access data from the participant table, depending on the needs

    def get_participants_by_exp_name(self, exp_name: str) -> pd.DataFrame:
        """
        Retrieves all participants associated with a given experiment name,
        including experiment metadata such as name and data state.

        Args:
            exp_name (str): The name of the experiment.

        Returns:
            pd.DataFrame: A DataFrame containing participant data joined with experiment info.
        """
        cursor = self.conn.cursor()
        query = """
            SELECT 
                participant.*, 
                experiment.name AS experiment_name,
                experiment.data_state AS experiment_data_state
            FROM participant
            JOIN experiment ON participant.experiment_id = experiment.id
            WHERE experiment.name = ?
        """
        cursor.execute(query, (exp_name.lower(),))
        rows = cursor.fetchall()

        if not rows:
            return pd.DataFrame()

        columns = [desc[0] for desc in cursor.description]
        return pd.DataFrame(rows, columns=columns)     
        
    
    def get_participant_by_id(self, participant_id: int) -> Participant | None:
        """
        Retrieves a participant by their internal database ID.

        Args:
            participant_id (int): The internal database ID of the participant.

        Returns:
            Participant | None: A Participant object if found, otherwise None.
        """
        cursor = self.conn.cursor()
        cursor.execute("SELECT * FROM participant WHERE id = ?", (participant_id,))
        row = cursor.fetchone()
        if row:
            # currently does not retrieve age, height, etc., as this information is currently not stored in the DB
            return Participant(id=row["id"], participant_id=row["participant_id"], 
                               experiment_id=row["experiment_id"], instrument= row["instrument"], 
                               PRMD_shoulder_neck_right=row["PRMD_shoulder_neck_right"],
                               PRMD_shoulder_neck_left=row["PRMD_shoulder_neck_left"],
                               PRMD_upper_arm_right=row["PRMD_upper_arm_right"],
                               PRMD_upper_arm_left=row["PRMD_upper_arm_left"],
                               PRMD_ever=row["PRMD_ever"],)
        return None

    def get_participant_db_id(self, participant_id: int, exp_id: int) -> Participant | None:
        """
        Retrieves the internal database ID of a participant using their participant ID
        (e.g., 'P001') and the associated experiment ID.

        Args:
            participant_id (str): The participant ID (not the database ID).
            exp_id (int): The ID of the experiment the participant belongs to.

        Returns:
            int | None: The internal database ID of the participant if found, otherwise None.
        """
        
        cursor = self.conn.cursor()
        cursor.execute("SELECT id FROM participant WHERE participant_id = ? AND experiment_id = ?", 
                       (participant_id, exp_id))
        row = cursor.fetchone()
        if row:
            return row[0]
        return None
    
    def get_participant_ids(self, exp_id: int) -> Participant | None:
        """
        Retrieves all unique participant identifiers (e.g., 'P001', 'P002') for a given experiment.

        Args:
            exp_id (int): The ID of the experiment.

        Returns:
            list[str] | None: A list of participant IDs if any exist, otherwise None.
        """
        
        cursor = self.conn.cursor()
        cursor.execute("SELECT DISTINCT participant_id FROM participant WHERE experiment_id = ?", (exp_id,))
        rows = cursor.fetchall()
        if rows:
            return [row[0] for row in rows]
        return None
#endregion Getter