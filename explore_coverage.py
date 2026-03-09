import pandas as pd

# Load files
patients    = pd.read_csv('data/hosp/patients.csv')
icustays    = pd.read_csv('data/icu/icustays.csv')
labevents   = pd.read_csv('data/hosp/labevents.csv')
chartevents = pd.read_csv('data/icu/chartevents.csv')

# Confirmed item IDs
VITALS_IDS = [220045, 220052, 220277, 220210]
LAB_IDS    = [50813, 50912, 51301, 51265, 50885]

# Filter only the rows we need
vitals = chartevents[chartevents['itemid'].isin(VITALS_IDS)]
labs   = labevents[labevents['itemid'].isin(LAB_IDS)]

# Find patients who have BOTH vitals AND labs
patients_with_vitals = set(vitals['subject_id'].unique())
patients_with_labs   = set(labs['subject_id'].unique())
patients_with_both   = patients_with_vitals & patients_with_labs

print(f"Total patients:              {len(patients)}")
print(f"Patients with vitals:        {len(patients_with_vitals)}")
print(f"Patients with labs:          {len(patients_with_labs)}")
print(f"Patients with BOTH:          {len(patients_with_both)}")
print(f"\nVitals rows after filter:    {len(vitals)}")
print(f"Labs rows after filter:      {len(labs)}")

# Vitals breakdown
print("\n=== VITALS BREAKDOWN ===")
label_map = {
    220045: 'Heart Rate',
    220052: 'MAP',
    220277: 'SpO2',
    220210: 'Resp Rate'
}
vitals = vitals.copy()
vitals['label'] = vitals['itemid'].map(label_map)
print(vitals.groupby('label')['subject_id'].count())

# Labs breakdown
print("\n=== LABS BREAKDOWN ===")
lab_map = {
    50813: 'Lactate',
    50912: 'Creatinine',
    51301: 'WBC',
    51265: 'Platelets',
    50885: 'Bilirubin'
}
labs = labs.copy()
labs['label'] = labs['itemid'].map(lab_map)
print(labs.groupby('label')['subject_id'].count())
