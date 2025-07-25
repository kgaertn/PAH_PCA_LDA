import pandas as pd

def subtract_meanwave_key_difference(df: pd.DataFrame) -> pd.DataFrame:
    """
    Centers 'value' by subtracting the difference between the global mean 
    (per point) and the key-specific mean (per bow_stroke, key, point),
    computed separately for each PRMD group.

    Returns a DataFrame with added columns:
    - 'key_mean_value', 'mean_value', 'key_difference', 'value_centered'

    Args:
        df (pd.DataFrame): Input with required columns including 'PRMD_ever', 
                            'bow_stroke', 'key', 'point', and 'value'.

    Returns:
        pd.DataFrame: Data with centered values and intermediate computations.
    """
    df_res = pd.DataFrame()
    for group in df['PRMD_ever'].unique():
        df_group = df[df['PRMD_ever'] == group]
    
        df_mean_key = (
            df_group.groupby(['bow_stroke', 'key', 'point'], as_index=False)['value']
            .mean()
            .rename(columns={'value': 'key_mean_value'})
        )
        
        df_mean_group = (
            df_group.groupby(['point'], as_index=False)['value']
            .mean()
            .rename(columns={'value': 'mean_value'})
        )
        
        df_mean_merged = df_mean_key.merge(df_mean_group, on=['point'])
        df_mean_merged['key_difference'] = df_mean_merged['mean_value'] - df_mean_merged['key_mean_value']
        
        df_group_merged = df_group.merge(df_mean_merged, on=['bow_stroke', 'key', 'point'])
        df_group_merged['value_centered'] = df_group_merged['value'] - df_group_merged['key_difference']
        df_res = pd.concat([df_res, df_group_merged], axis=0, ignore_index=True)

    return df_res  