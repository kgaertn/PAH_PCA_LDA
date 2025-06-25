from db.connection import get_connection

def create_tables():
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS experiment (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            data_state TEXT,
            data_folder TEXT,
            upload_complete INTEGER
        )
    """)
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS participant (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            experiment_id INTEGER,
            participant_id TEXT,
            age INTEGER,
            height_cm REAL,
            weight_kg REAL,
            instrument TEXT,
            PRMD_shoulder_neck_right INTEGER,
            PRMD_shoulder_neck_left INTEGER,
            PRMD_upper_arm_right INTEGER,
            PRMD_upper_arm_left INTEGER,
            PRMD_ever INTEGER,
            FOREIGN KEY (experiment_id) REFERENCES experiment(id)
        )
    """)
    
    # Add measurement table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS measurement (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            participant_id INTEGER,
            timepoint TEXT,
            device TEXT,
            target TEXT,
            axis TEXT,
            unit TEXT,
            FOREIGN KEY (participant_id) REFERENCES participant(id)
        )
    """)
    
    # Add sample table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS sample (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            measurement_id INTEGER,
            bow_stroke_start INTEGER,
            bow_stroke_end INTEGER,
            UNIQUE(measurement_id, bow_stroke_start),
            FOREIGN KEY (measurement_id) REFERENCES measurement(id)
)
    """)

    # Add datapoint table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS datapoint (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            measurement_id INTEGER,
            sample_id INTEGER,
            bow_stroke INTEGER,
            up_down INTEGER,
            key TEXT,
            time_point INTEGER,
            value REAL,
            FOREIGN KEY (measurement_id) REFERENCES measurement(id),
            FOREIGN KEY (sample_id) REFERENCES sample(id)
        )
    """)
    
    # Add index on measurement_id in datapoint
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_datapoint_measurement_id
        ON datapoint (measurement_id)
    """)
    
    # Add index on measurement_id in sample
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_sample_measurement_id
        ON sample (measurement_id)
    """)

    conn.commit()


def add_columns_if_missing(table_name, new_columns):
    """
    Adds new columns to an existing table if they don't already exist.

    Parameters:
    - connection: SQLite connection object
    - table_name: Name of the table as string
    - new_columns: Dict with column names as keys and SQL types as values
                   e.g., {"new_col1": "TEXT", "new_col2": "INTEGER"}
    """
    conn = get_connection()
    cursor = conn.cursor()

    # Get current column names
    cursor.execute(f"PRAGMA table_info({table_name})")
    existing_columns = {row[1] for row in cursor.fetchall()}

    # Add missing columns
    for col_name, col_type in new_columns.items():
        if col_name not in existing_columns:
            alter_stmt = f"ALTER TABLE {table_name} ADD COLUMN {col_name} {col_type}"
            cursor.execute(alter_stmt)
            print(f"Added column: {col_name} ({col_type})")
        else:
            print(f"Column already exists: {col_name}")

    conn.commit()
 
def create_adjusted_view():
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        CREATE VIEW IF NOT EXISTS datapoint_adjusted AS
            SELECT
                id,
                measurement_id,
                sample_id,
                bow_stroke,
                up_down,
                key,
                CASE
                    WHEN up_down = 1 THEN time_point + 101
                    ELSE time_point
                END AS time_point
            FROM datapoint;
    """)
    
 
def create_datapoints_MPA_view():
    #TODO
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        CREATE VIEW IF NOT EXISTS "Datapoints MPA Clean" AS
            SELECT
                e.id AS experiment_id, e.name,
                p.id AS participant_id, p.participant_id AS ext_participant_id, p.instrument, p.PRMD_shoulder_neck_right, 
                p.PRMD_shoulder_neck_left, p.PRMD_upper_arm_right, 
                p.PRMD_upper_arm_left, p.PRMD_ever,
                m.id AS measurement_id, m.timepoint, m.device,
                m.target, m.axis, m.unit,
                d.id AS dp_id, d.sample_id, d.bow_stroke, d.up_down,
                d.key, d.time_point AS dp_time_point

            FROM experiment e
            JOIN participant p ON p.experiment_id = e.id
            JOIN measurement m ON m.participant_id = p.id
            JOIN datapoint_adjusted d ON d.measurement_id = m.id;
    """)

def create_datapoints_MPA_device_view(device_table_name:str, device:str):
    #TODO
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(f"""
        CREATE VIEW IF NOT EXISTS "Datapoints MPA Clean {device_table_name}" AS
            SELECT
                e.id AS experiment_id, e.name,
                p.id AS participant_id, p.participant_id AS ext_participant_id, p.instrument, p.PRMD_shoulder_neck_right, 
                p.PRMD_shoulder_neck_left, p.PRMD_upper_arm_right, 
                p.PRMD_upper_arm_left, p.PRMD_ever,
                m.id AS measurement_id, m.timepoint, m.device,
                m.target, m.axis, m.unit,
                d.id AS dp_id, d.sample_id, d.bow_stroke, d.up_down,
                d.key, d.time_point AS dp_time_point

            FROM experiment e
            JOIN participant p ON p.experiment_id = e.id
            JOIN measurement m ON m.participant_id = p.id
            JOIN datapoint_adjusted d ON d.measurement_id = m.id
            WHERE m.device = "{device}";
    """)

if __name__ == "__main__":
    create_tables()
    print("Database tables created.")