import pandas as pd

chartevents = pd.read_csv('data/icu/chartevents.csv')
labevents   = pd.read_csv('data/hosp/labevents.csv')

VITALS_IDS = [220045, 220052, 220277, 220210]
LAB_IDS    = [50813, 50912, 51301, 51265, 50885]

vitals = chartevents[chartevents['itemid'].isin(VITALS_IDS)].copy()
labs   = labevents[labevents['itemid'].isin(LAB_IDS)].copy()

# Convert timestamps
vitals['charttime'] = pd.to_datetime(vitals['charttime'])
labs['charttime']   = pd.to_datetime(labs['charttime'])

# Find patients who have lactate specifically
lactate = labs[labs['itemid'] == 50813]
patients_with_lactate = set(lactate['subject_id'].unique())
print(f"Patients with lactate readings: {len(patients_with_lactate)}")

# Pick one patient with lactate and show their combined timeline
sample_id = lactate['subject_id'].iloc[0]

# Their vitals
sample_vitals = vitals[vitals['subject_id'] == sample_id].copy()
sample_vitals = sample_vitals.sort_values('charttime')

# Their labs
sample_labs = labs[labs['subject_id'] == sample_id].copy()
sample_labs = sample_labs.sort_values('charttime')

label_map = {
    220045: 'Heart Rate',
    220052: 'MAP',
    220277: 'SpO2',
    220210: 'Resp Rate',
    50813:  'Lactate',
    50912:  'Creatinine',
    51301:  'WBC',
    51265:  'Platelets',
    50885:  'Bilirubin'
}

sample_vitals['label'] = sample_vitals['itemid'].map(label_map)
sample_labs['label']   = sample_labs['itemid'].map(label_map)

print(f"\n=== PATIENT {sample_id} — VITALS (first 8 rows) ===")
print(sample_vitals[['charttime', 'label', 'valuenum']].head(8).to_string())

print(f"\n=== PATIENT {sample_id} — LABS (all) ===")
print(sample_labs[['charttime', 'label', 'valuenum']].to_string())

print(f"\n=== TIME OVERLAP ===")
print(f"Vitals from: {sample_vitals['charttime'].min()} to {sample_vitals['charttime'].max()}")
print(f"Labs from:   {sample_labs['charttime'].min()} to {sample_labs['charttime'].max()}")
