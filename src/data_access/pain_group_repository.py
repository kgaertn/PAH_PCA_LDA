#import pandas as pd
#from db.connection import get_connection
from data_access.base_repository import BaseRepository
from models.pain_group import PainGroup

class PainGroupRepository(BaseRepository):
    def __init__(self):
        """
        Initializes the MeasurementRepository with a database connection.
        """
        super().__init__()

    def insert_pain_group(self, pain_group: str):
        data = {
            "pain_type": pain_group
        }
        return self.insert_one("pain_group", data)
    
    def insert_participant_pain_group(self, participant_id, pain_group_id):
        data = {
            "participant_id": participant_id,
            "pain_group_id": pain_group_id
        }
        self.insert_one("participant_pain_group", data)

    def insert_participant_pain_groups(self, participant_ids: list[int], pain_group_id: int):
        """
        Inserts multiple participant-pain group assignments.

        Args:
            participant_ids (list[int]): List of participant IDs.
            pain_group_id (int): The pain group ID to assign.
        """
        data = [{"participant_id": pid, "pain_group_id": pain_group_id} for pid in participant_ids]
        self.insert_many("participant_pain_group", data)

    def get_existing_pain_groups(self) -> list[PainGroup] | None:
        """
        Retrieves all existing pain groups from the database.

        Returns:
            list[PainGroup] | None: A list of PainGroup objects, or None if no entries exist.
        """
        rows = self.get_advanced(
            table_or_view="pain_group",
            return_df=False
        )
        if rows:
            return [PainGroup(id=row[0], pain_group=row[1]) for row in rows]
        return None

    def get_participant_ids_by_pain_group(self, pain_group_id: int) -> list[int] | None:
        """
        Gibt eine Liste von Teilnehmer-IDs zurück, bei denen mindestens eine der angegebenen Schmerzspalten
        den gewünschten Wert (0 oder 1) hat.
        """
        # OR-Filter mit gemeinsamem Wert
        #or_filter = {col: pain for col in pain_columns}
        
        rows = self.get_advanced(
            table_or_view="participant_pain_group",
            columns=["participant_id"],
            pain_group_id = pain_group_id,
            return_df=False
        )
    
        return [row[0] for row in rows] if rows else None