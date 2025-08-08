from data_access.base_repository import BaseRepository
from models.pain_group import PainGroup

class PainGroupRepository(BaseRepository):
    def __init__(self):
        """
        Initializes the PainGroupRepository with a database connection by calling the parent constructor.
        """
        super().__init__()

# region Setter
    def insert_pain_group(self, pain_group: str) -> int:
        """
        Inserts a new pain group into the 'pain_group' table.

        Args:
            pain_group (str): Name or type of the pain group.

        Returns:
            int: The ID of the newly inserted pain group.
        """
        data = {
            "pain_type": pain_group
        }
        return self.insert_one("pain_group", data)
    
    def insert_participant_pain_group(self, participant_id:int, pain_group_id:int):
        """
        Inserts an association between a participant and a pain group.

        Args:
            participant_id (int): The ID of the participant.
            pain_group_id (int): The ID of the pain group.
        """
        data = {
            "participant_id": participant_id,
            "pain_group_id": pain_group_id
        }
        self.insert_one("participant_pain_group", data)

    def insert_pc_pain_group(self, pc_id:int, pain_group_id:int):
        """
        Inserts an association between a principal component (PC) and a pain group.

        Args:
            pc_id (int): The ID of the PC.
            pain_group_id (int): The ID of the pain group.
        """
        data = {
            "pc_id": pc_id,
            "pain_group_id": pain_group_id
        }
        self.insert_one("pcs_ranked_pain_group", data)

    def insert_participant_pain_groups(self, participant_ids: list[int], pain_group_id: int):
        """
        Inserts multiple participant-pain group associations in bulk.

        Args:
            participant_ids (list[int]): List of participant IDs.
            pain_group_id (int): The pain group ID to assign.
        """
        data = [{"participant_id": pid, "pain_group_id": pain_group_id} for pid in participant_ids]
        self.insert_many("participant_pain_group", data)

# endregion Setter

# region Getter

    def get_existing_pain_groups(self) -> list[PainGroup] | None:
        """
        Retrieves all existing pain groups from the database.

        Returns:
            list[PainGroup] | None: A list of PainGroup objects if any exist, otherwise None.
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
        Retrieves a list of participant IDs associated with the specified pain group.

        Args:
            pain_group_id (int): The pain group ID to filter participants by.

        Returns:
            list[int] | None: List of participant IDs belonging to the pain group, or None if none found.
        """
        
        rows = self.get_advanced(
            table_or_view="participant_pain_group",
            columns=["participant_id"],
            pain_group_id = pain_group_id,
            return_df=False
        )
    
        return [row[0] for row in rows] if rows else None

# endregion Getter