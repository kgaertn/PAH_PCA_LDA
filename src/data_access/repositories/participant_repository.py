from data_access.repositories.base_repository import BaseRepository
from data_access.models.participant import Participant

class ParticipantRepository(BaseRepository):
    def __init__(self):
        """
        Initializes the ParticipantRepository with a database connection by calling the parent constructor.
        """
        super().__init__()

# region Setter
    def insert_participant(self, participant: Participant) -> int:
        """
        Inserts a new participant record into the 'participant' table.

        Args:
            participant (Participant): The Participant model instance to insert.

        Returns:
            int: The ID of the newly inserted participant record.
        """
        data = {
            "experiment_id": participant.experiment_id,
            "participant_id": participant.participant_id,
        }
        return self.insert_one("participant", data)

    def update_pain_data(self, participant: Participant, exp_id:int):
        """
        Updates pain-related data fields for a participant in the database.

        This method uses a generic update function to set several PRMD pain columns
        for the participant identified by experiment_id and participant_id.

        Args:
            participant (Participant): The Participant instance containing updated pain data.
            exp_id (int): The experiment ID to which the participant belongs.
        """
        self.update(
            table="participant",
            values={"instrument": participant.instrument, "PRMD_shoulder_neck_right":participant.PRMD_shoulder_neck_right, 
                    "PRMD_shoulder_neck_left":participant.PRMD_shoulder_neck_left,"PRMD_upper_arm_right":participant.PRMD_upper_arm_right, 
                    "PRMD_upper_arm_left":participant.PRMD_upper_arm_left, "PRMD_ever":participant.PRMD_ever},
            where={"experiment_id": exp_id, "participant_id": participant.participant_id}
        )
      
# endregion Setter

#region Getter
    def get_participant_ids(self, exp_id: int) -> list[str] | None:
        """
        Retrieves all unique participant IDs for a given experiment.

        Args:
            exp_id (int): The experiment ID.

        Returns:
            list[str] | None: List of participant IDs if found, otherwise None.
        """
        rows = self.get_advanced(
            table_or_view="participant",
            columns=["participant_id"],
            experiment_id=exp_id,  # <- direkt, nicht als dict
            distinct=True,
            return_df=False
        )
        return [row[0] for row in rows] if rows else None
    
    def get_participant_db_id(self, participant_id: str, exp_id: int) -> int | None:
        """
        Retrieves the internal database ID of a participant given their "external" participant ID and experiment ID.

        Args:
            participant_id (str): The participant's external ID (e.g., 'P001').
            exp_id (int): The experiment ID.

        Returns:
            int | None: Internal database ID of the participant, or None if not found.
        """
        rows = self.get_advanced(
            table_or_view="participant",
            participant_id = participant_id,
            experiment_id = exp_id,
            columns=["id"],
            return_df=False
        )
        return rows[0][0] if rows else None
    
    def get_participant_column_names(self) -> list[str]:
        """
        Retrieves the list of column names in the 'participant' table.

        Returns:
            list[str]: List of column names.
        """
        return self.get_column_names('participant')
    
    def get_pain_participants(self, pain_columns: list[str], pain: int) -> list[int] | None:
        """
        Returns a list of participant IDs where at least one of the specified pain columns
        matches the given pain value (e.g., 0 or 1).

        Args:
            pain_columns (list[str]): List of column names related to pain.
            pain (int): Desired pain value to filter by.

        Returns:
            list[int] | None: List of participant database IDs matching the criteria, or None.
        """
        or_filter = {col: pain for col in pain_columns}
        
        rows = self.select_with_or(
            table_or_view="participant",
            or_filters=or_filter,
            columns=["id"],
            return_df=False
        )
    
        return [row[0] for row in rows] if rows else None
    
#endregion Getter