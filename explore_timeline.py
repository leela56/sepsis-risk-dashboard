import pandas as pd

chartevents = pd.read_csv('data/icu/chartevents.csv')
labevents   = pd.read_csv('data/hosp/labevents.csv')
icustays    = pd.read_csv('data/icu/icustays.csv')

VITALS_IDS = [220045, 220052, 220277, 220210]
LAB_IDS    = [50813, 50912, 51301, 51265, 50885]

vitals = chartevents[chartevents['itemid'].isin(VITALS_IDS)].copy()
labs   = labevents[labevents['itemid'].isin(LAB_IDS)].copy()

# Convert timestamps
vitals['charttime'] = pd.to_datetime(vitals['charttime'])
labs['charttime']   = pd.to_datetime(labs['charttime'])
icustays['intime']  = pd.to_datetime(icustays['intime'])
icustays['outtime'] = pd.to_datetime(icustays['outtime'])

# ICU stay duration
icustays['los_hours'] = (
    icustays['outtime'] - icustays['intime']
).dt.total_seconds() / 3600

print("=== ICU STAY DURATION ===")
print(f"Average stay:  {icustays['los_hours'].mean():.1f} hours")
print(f"Shortest stay: {icustays['los_hours'].min():.1f} hours")
print(f"Longest stay:  {icustays['los_hours'].max():.1f} hours")

# Vitals frequency per patient
vitals_per_patient = vitals.groupby('subject_id').agg(
    total_readings=('charttime', 'count'),
    first_reading=('charttime', 'min'),
    last_reading=('charttime', 'max')
).reset_index()

vitals_per_patient['span_hours'] = (
    vitals_per_patient['last_reading'] -
    vitals_per_patient['first_reading']
).dt.total_seconds() / 3600

vitals_per_patient['readings_per_hour'] = (
    vitals_per_patient['total_readings'] /
    vitals_per_patient['span_hours']
)

print("\n=== VITALS FREQUENCY PER PATIENT ===")
print(f"Avg readings per patient:    {vitals_per_patient['total_readings'].mean():.0f}")
print(f"Avg span of readings:        {vitals_per_patient['span_hours'].mean():.1f} hours")
print(f"Avg readings per hour:       {vitals_per_patient['readings_per_hour'].mean():.2f}")

# Sample patient timeline
sample_patient = vitals['subject_id'].iloc[0]
sample = vitals[vitals['subject_id'] == sample_patient].sort_values('charttime')

print(f"\n=== SAMPLE PATIENT {sample_patient} TIMELINE ===")
label_map = {220045: 'Heart Rate', 220052: 'MAP', 220277: 'SpO2', 220210: 'Resp Rate'}
sample['label'] = sample['itemid'].map(label_map)
print(sample[['charttime', 'label', 'valuenum']].head(12).to_string())
