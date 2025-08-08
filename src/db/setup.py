from db.connection import get_connection

def db_setup():
    """
    Sets up the database by:
    - Creating all necessary tables if they don't exist.
    - Adding missing columns to the 'datapoint' table.
    - Creating multiple database views for adjusted and aggregated data.
    """
    create_tables()
    add_columns_if_missing('datapoint', new_columns = {'sample_id': 'INTEGER'})
    create_adjusted_view()
    create_complete_datapoints_view()
    create_PCA_View()

def add_measurement_type_info():
    """
    Updates the 'measurement_type' table and related tables by:
    - Populating the measurement_type table with data from the measurement table.
    - Updating measurement records with the corresponding measurement_type_id.
    - Adding rotation sequence information based on target and axis data.
    """
    fill_measurement_type_table()
    add_measurement_type_id_to_measurement()    
    add_rotation_sequence()    

def create_tables():
    """
    Creates all required database tables if they do not already exist.
    Includes all tables, such as experiment, participant, pain_group, measurement_type,
    measurement, sample, datapoint, pcs_ranked, pc_scores, scaler, pca_rotation,
    and pain group association tables.
    """    
    
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
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS pain_group (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            pain_type TEXT UNIQUE
        )
    """)    
   
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS measurement_type (
            id INTEGER PRIMARY KEY AUTOINCREMENT,  
			experiment_id INTEGER,
            device TEXT,
			meas_time_point TEXT,
			target TEXT,
            axis TEXT,  
            rotation_sequence TEXT,        
			FOREIGN KEY (experiment_id) REFERENCES experiment(id)
            UNIQUE (experiment_id, device, meas_time_point, target, axis)
        );
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
            measurement_type_id INTEGER,
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
            UNIQUE(measurement_id, bow_stroke, time_point),
            FOREIGN KEY (measurement_id) REFERENCES measurement(id),
            FOREIGN KEY (sample_id) REFERENCES sample(id)
        )
    """)
    
    # Add ranked pcs table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS pcs_ranked (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            measurement_type_id INTEGER,
            scaler_id INTEGER NULL,
            rotation_id INTEGER,
            parent_id INTEGER,
            pc_index INTEGER,
            loading_vector TEXT,
            explained_variance REAL,
            group_mean_pain REAL, 
            group_mean_no_pain REAL,
            group_std_pain REAL, 
            group_std_no_pain REAL,
            t_value REAL,
            p_value REAL,
            data_scaled INTEGER,
            pca_info TEXT,
            distribution_info TEXT,
            shap_wilk_w_pain REAL,
            shap_wilk_w_no_pain REAL,
            shap_wilk_p_pain REAL,
            shap_wilk_p_no_pain REAL,
            FOREIGN KEY (measurement_type_id) REFERENCES measurement_type(id)
            FOREIGN KEY (parent_id) REFERENCES pcs_ranked(id)
            UNIQUE (measurement_type_id, scaler_id, rotation_id, parent_id, pc_index, data_scaled)
        )
    """)
    
    # Add pc_scores table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS pc_scores (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sample_id INTEGER,
            pc_id INTEGER,
            pc_score REAL,
            FOREIGN KEY (sample_id) REFERENCES sample(id),
            FOREIGN KEY (pc_id) REFERENCES pcs_ranked(id)
        )
    """)
    
    # Add scaler table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS scaler (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            measurement_type_id INTEGER,
            scaler_type TEXT,
            mean TEXT,
            scale TEXT,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (measurement_type_id) REFERENCES measurement_type(id)
            UNIQUE (measurement_type_id, scaler_type)
        );
    """)

    # Add pca rotation table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS pca_rotation (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            rotation_type TEXT UNIQUE
        );
    """)  
      
    # Add pain_group table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS pain_group (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            pain_group TEXT UNIQUE NOT NULL
        );
    """)   
     
    # Add pain_group table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS participant_pain_group (
            participant_id INTEGER NOT NULL,
            pain_group_id INTEGER NOT NULL,
            FOREIGN KEY (participant_id) REFERENCES participant(id),
            FOREIGN KEY (pain_group_id) REFERENCES pain_group(id),
            PRIMARY KEY (participant_id, pain_group_id)
        );
    """)    

    # Add pain_group table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS pcs_ranked_pain_group (
            pc_id INTEGER NOT NULL,
            pain_group_id INTEGER NOT NULL,
            FOREIGN KEY (pc_id) REFERENCES pcs_ranked(id),
            FOREIGN KEY (pain_group_id) REFERENCES pain_group(id),
            PRIMARY KEY (pc_id, pain_group_id)
        );
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

def add_columns_if_missing(table_name:str, new_columns:dict[str, str]):
    """
    Adds new columns to an existing table if they are missing.

    Args:
        table_name (str): The name of the table to modify.
        new_columns (dict): A dictionary where keys are column names and values are SQL data types,
                            e.g. {'sample_id': 'INTEGER'}.
    """
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute(f"PRAGMA table_info({table_name})")
    existing_columns = {row[1] for row in cursor.fetchall()}

    for col_name, col_type in new_columns.items():
        if col_name not in existing_columns:
            alter_stmt = f"ALTER TABLE {table_name} ADD COLUMN {col_name} {col_type}"
            cursor.execute(alter_stmt)
            print(f"Added column: {col_name} ({col_type})")
        else:
            print(f"Column already exists: {col_name}")

    conn.commit()
 
def create_adjusted_view():
    """
    Creates the 'datapoint_adjusted' view that shifts timepoints by 101 units when up_down=1
    and includes only datapoints with valid sample_id.
    """
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
                END AS time_point, 
                value
            FROM datapoint
            WHERE sample_id IS NOT NULL;
    """)
     
def create_complete_datapoints_view():
    """
    Creates the 'Complete Data' view that combines experiment, participant, measurement,
    and datapoint information and filters for valid rotation sequences.
    """
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        CREATE VIEW IF NOT EXISTS "Complete Data" AS
            SELECT
                e.id AS experiment_id, e.name,
                p.id AS participant_id, p.participant_id AS ext_participant_id, p.instrument, p.PRMD_shoulder_neck_right, 
                p.PRMD_shoulder_neck_left, p.PRMD_upper_arm_right, 
                p.PRMD_upper_arm_left, p.PRMD_ever,
                m.id AS measurement_id, m.measurement_type_id, m.timepoint, m.device,
                m.target, m.axis, m.unit, mt.rotation_sequence,
                d.id AS dp_id, d.sample_id, d.bow_stroke, d.up_down,
                d.key, d.time_point AS dp_time_point, d.value

            FROM experiment e
            JOIN participant p ON p.experiment_id = e.id
            JOIN measurement m ON m.participant_id = p.id
            JOIN datapoint_adjusted d ON d.measurement_id = m.id
            JOIN measurement_type mt ON m.measurement_type_id = mt.id
            WHERE mt.rotation_sequence NOT IN ('carrying_angle', 'redundant');
    """)
    
def create_PCA_View():
    """
    Creates the 'Participants PCs' view combining PCA-related data with pain group and participant information.
    """
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        CREATE VIEW IF NOT EXISTS "Participants PCs" AS
            WITH pain_groups_agg AS (
                SELECT 
                    prpg.pc_id,
                    GROUP_CONCAT(pg.pain_type, ', ') AS pain_groups
                FROM pcs_ranked_pain_group prpg
                JOIN pain_group pg ON prpg.pain_group_id = pg.id
                GROUP BY prpg.pc_id
            )       
            SELECT
                e.id AS exp_id, 
                p.id AS participant_id, 
                p.participant_id AS ext_participant_id, 
                p.PRMD_ever, 
                pg_agg.pain_groups, 
                m.device, 
                mt.id AS meas_type_id, 
                m.timepoint AS meas_time_point, 
                m.target, 
                m.axis, 
                pcr.id AS pc_id, 
                pcr.parent_id,
                pcr.pc_index, 
                pcr.loading_vector, 
                pcr.explained_variance, 
                pcr.group_mean_pain, 
                pcr.group_mean_no_pain, 
                pcr.group_std_pain, 
                pcr.group_std_no_pain, 
                pcr.t_value, 
                pcr.p_value, 
                pcr.data_scaled, 
                rot.rotation_type,
                pcr.distribution_info, 
                pcr.shap_wilk_w_pain, 
                pcr.shap_wilk_w_no_pain, 
                pcr.shap_wilk_p_pain, 
                pcr.shap_wilk_p_no_pain, 
                s.id AS sample_id, 
                pcs.pc_score

            FROM experiment e
            JOIN participant p ON p.experiment_id = e.id
            JOIN measurement m ON m.participant_id = p.id
            JOIN measurement_type mt ON m.measurement_type_id = mt.id
            JOIN pcs_ranked pcr ON mt.id = pcr.measurement_type_id
            JOIN pca_rotation AS rot ON pcr.rotation_id = rot.id
            JOIN sample s ON m.id = s.measurement_id
            JOIN pc_scores pcs ON s.id = pcs.sample_id AND pcr.id = pcs.pc_id
            LEFT JOIN pain_groups_agg pg_agg ON pg_agg.pc_id = pcr.id

            WHERE mt.rotation_sequence NOT IN ('carrying_angle', 'redundant'); 
    """)

def fill_measurement_type_table():
    """
    Inserts distinct combinations of experiment_id, device, timepoint, target, and axis from the
    measurement table into the measurement_type table, ignoring duplicates.
    """
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(f"""
        INSERT OR IGNORE INTO measurement_type (experiment_id, device, meas_time_point, target, axis)
        SELECT DISTINCT
        p.experiment_id,
        m.device,
        m.timepoint,
        m.target,
        m.axis
        FROM measurement m
        JOIN participant p ON m.participant_id = p.id;
    """)
    conn.commit()
    
def add_measurement_type_id_to_measurement():
    """
    Updates measurement records with a NULL measurement_type_id by assigning the matching measurement_type.id
    based on experiment_id, device, timepoint, target, and axis.
    """
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(f"""
        UPDATE measurement
            SET measurement_type_id = (
            SELECT mt.id
            FROM measurement_type mt
            JOIN participant p ON measurement.participant_id = p.id
            WHERE measurement.timepoint = mt.meas_time_point
                AND measurement.target = mt.target
                AND measurement.axis IS mt.axis
                AND measurement.device = mt.device
                AND p.experiment_id = mt.experiment_id
            )
            WHERE measurement_type_id IS NULL;
    """)
    conn.commit()    

def add_rotation_sequence():
    """
    Updates the rotation_sequence column in the measurement_type table based on predefined
    target and axis combinations.
    """
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(f"""UPDATE measurement_type SET rotation_sequence = CASE
    WHEN target = 'left elbow joint angle' AND axis = 'Z' THEN 'flexion'
	WHEN target = 'left gh joint angle' AND axis = 'X' THEN 'abduction'
	WHEN target = 'left gh joint angle' AND axis = 'Y' THEN 'internal_rotation'
    WHEN target = 'left gh joint angle' AND axis = 'Z' THEN 'flexion'
	WHEN target = 'left ht joint angle' AND axis = 'X' THEN 'abduction'
    WHEN target = 'left ht joint angle' AND axis = 'Y' THEN 'internal_rotation'
	WHEN target = 'left ht joint angle' AND axis = 'Z' THEN 'flexion'
	WHEN target = 'left humeroulnar joint angle' AND axis = 'Z' THEN 'redundant'
	WHEN target = 'left radioulnar joint angle' AND axis = 'Y' THEN 'pronation'
	WHEN target = 'left st joint angle' AND axis = 'X' THEN 'upward_rotation'
	WHEN target = 'left st joint angle' AND axis = 'Y' THEN 'protraction'
	WHEN target = 'left st joint angle' AND axis = 'Z' THEN 'posterior_tilt'
	WHEN target = 'left wrist joint angle' AND axis = 'X' THEN 'radial_abduction'
	WHEN target = 'left wrist joint angle' AND axis = 'Y' THEN 'carrying_angle'
	WHEN target = 'left wrist joint angle' AND axis = 'Z' THEN 'flexion'
	WHEN target = 'neck joint angle' AND axis = 'X' THEN 'lateral_flexion'
	WHEN target = 'neck joint angle' AND axis = 'Y' THEN 'axial_rotation'
	WHEN target = 'neck joint angle' AND axis = 'Z' THEN 'flexion'
    WHEN target = 'right elbow joint angle' AND axis = 'Z' THEN 'flexion'
	WHEN target = 'right gh joint angle' AND axis = 'X' THEN 'abduction'
	WHEN target = 'right gh joint angle' AND axis = 'Y' THEN 'internal_rotation'
    WHEN target = 'right gh joint angle' AND axis = 'Z' THEN 'flexion'
	WHEN target = 'right ht joint angle' AND axis = 'X' THEN 'abduction'
    WHEN target = 'right ht joint angle' AND axis = 'Y' THEN 'internal_rotation'
	WHEN target = 'right ht joint angle' AND axis = 'Z' THEN 'flexion'
	WHEN target = 'right humeroulnar joint angle' AND axis = 'Z' THEN 'redundant'
	WHEN target = 'right radioulnar joint angle' AND axis = 'Y' THEN 'pronation'
	WHEN target = 'right st joint angle' AND axis = 'X' THEN 'upward_rotation'
	WHEN target = 'right st joint angle' AND axis = 'Y' THEN 'protraction'
	WHEN target = 'right st joint angle' AND axis = 'Z' THEN 'posterior_tilt'
	WHEN target = 'right wrist joint angle' AND axis = 'X' THEN 'radial_abduction'
	WHEN target = 'right wrist joint angle' AND axis = 'Y' THEN 'carrying_angle'
	WHEN target = 'right wrist joint angle' AND axis = 'Z' THEN 'flexion'
	WHEN target = 'sp1 pelvis joint angle' AND axis = 'X' THEN 'lateral_flexion'
	WHEN target = 'sp1 pelvis joint angle' AND axis = 'Z' THEN 'flexion'
	WHEN target = 'sp2 sp1 joint angle' AND axis = 'X' THEN 'lateral_flexion'
	WHEN target = 'sp2 sp1 joint angle' AND axis = 'Z' THEN 'flexion'
	WHEN target = 'sp3 sp2 joint angle' AND axis = 'X' THEN 'lateral_flexion'
	WHEN target = 'sp3 sp2 joint angle' AND axis = 'Z' THEN 'flexion'
	WHEN target = 'sp4 sp3 joint angle' AND axis = 'X' THEN 'lateral_flexion'
	WHEN target = 'sp4 sp3 joint angle' AND axis = 'Z' THEN 'flexion'
	WHEN target = 'sp5 sp4 joint angle' AND axis = 'X' THEN 'lateral_flexion'
	WHEN target = 'sp5 sp4 joint angle' AND axis = 'Z' THEN 'flexion'
	WHEN target = 'thx pel joint angle' AND axis = 'X' THEN 'lateral_flexion'
	WHEN target = 'thx pel joint angle' AND axis = 'Y' THEN 'axial_rotation'
	WHEN target = 'thx pel joint angle' AND axis = 'Z' THEN 'flexion'
    ELSE rotation_sequence 
END;""")
    conn.commit()

# TODO: calculate the angular velocity    
"""SELECT 
    da.measurement_id,
    da.sample_id,
    da.time_point,
	prev.time_point AS prev_time_point,
    da.value,
    
    CASE
        WHEN da.time_point = 0 THEN
            0
        ELSE
            (da.value - prev.value)
    END AS ang_velocity
FROM datapoint_adjusted da
LEFT JOIN datapoint_adjusted prev ON
    da.measurement_id = prev.measurement_id AND
    da.sample_id = prev.sample_id AND
    da.time_point = prev.time_point + 1
LEFT JOIN measurement meas ON
	da.measurement_id = meas.id
WHERE meas.device = 'mocap' AND meas.timepoint = 'pre'
ORDER BY da.measurement_id, da.sample_id, da.time_point;"""
        
if __name__ == "__main__":
    create_tables()
    print("Database tables created.")