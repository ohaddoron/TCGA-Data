import os

import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import MinMaxScaler
from sklearn.preprocessing import LabelEncoder
from lifelines import KaplanMeierFitter
import streamlit as st
from pathlib import Path
import plotly.express as px


class Parser:
    def __init__(self):
        self.clinical: pd.DataFrame = self.get_clincal_data_file()
        self.clinical_file_info()
        self.explore_data()
        self.select_variables()
        self.cleanup_inconsistencies()

    
    def get_clincal_data_file(self):
        file = st.file_uploader(label='Uploaded Clinical Data TSV File', accept_multiple_files=False) or Path(__file__).parent.joinpath('../clinical.tsv')
        if file is None:
            st.info('Please upload TSV file to parse')
            st.stop()
            
        clinical = pd.read_csv('clinical.tsv', sep='\t', na_values=['not reported', 'Not Reported'], low_memory=False)
        return clinical
    
    def clinical_file_info(self):
        st.write(f'Clinical File Shape: {self.clinical.shape}')
        st.write(self.clinical.head())
        
    def explore_data(self):
        with st.expander(label='Data Exploration'):

            st.markdown('''
            # Load and explore data

            General exploration of data to remove features:
            * missing all data
            * with as many levels as there are patients
            ''')
            st.write('~~ MISSING DATA ~~')
            

            n = self.clinical.shape[0]

            for col in self.clinical.columns:
                if col == 'submitter_id':
                    continue

                n_levels = len(self.clinical[col].value_counts())
                
                if n_levels == n:
                    self.clinical = self.clinical.drop(columns=[col])
                else:
                    n_missing = sum(self.clinical[col].isnull())
                    if n_missing > 0:
                        if n_missing == n:
                            self.clinical = self.clinical.drop(columns=[col])
                        else:
                            st.write(f'{col}: {n_missing} ({round(n_missing / n * 100, 2)}%)')
                    
            self.clinical_file_info()
            
    def select_variables(self):
        with st.expander('Variable Selection'):
            st.markdown('''
            # Select variables

            Select a few variables to keep and drop the remaining ones.
            ''')
            st.write(self.clinical.columns)
            label_cols = ['submitter_id', 'days_to_last_follow_up', 'vital_status', 'days_to_death']
            keep_cols = ['ajcc_pathologic_stage', 'age_at_diagnosis', 'prior_treatment', 'prior_malignancy',
                        'synchronous_malignancy', 'gender', 'race', 'ethnicity', 'disease',
                        'treatments_pharmaceutical_treatment_or_therapy',
                        'treatments_radiation_treatment_or_therapy']

            columns_to_drop = [col for col in self.clinical.columns if col not in label_cols + keep_cols]
            self.clinical = self.clinical.drop(columns=columns_to_drop)
            st.write('~~ MISSING DATA ~~')

            n = self.clinical.shape[0]

            for v in self.clinical.columns:
                n_missing = sum(self.clinical[v].isnull())
                if n_missing > 0:
                    if n_missing == n:
                        self.clinical = self.clinical.drop(columns=[v])
                    else:
                        st.write(f'{v}: {n_missing} ({round(n_missing / n * 100, 2)}%)')
                        
            self.clinical_file_info()
            st.markdown('''Selected clinical columns''')
            st.write(self.clinical.columns)
            
            st.write(self.clinical['gender'].value_counts())
            st.write(self.clinical['race'].value_counts())
            st.write(self.clinical['ethnicity'].value_counts())
            st.write(self.clinical['prior_malignancy'].value_counts())
            st.write(self.clinical['vital_status'].value_counts())
            st.write(self.clinical['ajcc_pathologic_stage'].value_counts())

            
            st.plotly_chart(px.histogram(y=self.clinical['days_to_death'], title='Days to Death'))
            st.plotly_chart(px.box(self.clinical, x='days_to_death', title='Days to Death'))
            
            
            st.plotly_chart(px.histogram(self.clinical, x='days_to_last_follow_up', title='Days to Last Follow Up'))
            st.plotly_chart(px.box(self.clinical, x='days_to_last_follow_up', title='Days to Last Follow Up'))
            
            st.plotly_chart(px.histogram(x=self.clinical['age_at_diagnosis'].apply(lambda x: x/365), title='Age at Diagnosis'))
            st.plotly_chart(px.box(x=self.clinical['age_at_diagnosis'].apply(lambda x: x/365), title='Age at Diagnosis'))
            
            st.write(self.clinical.describe())
            st.write(self.clinical.info())
            self.clinical = self.clinical.rename(columns={'disease': 'project_id'})
            self.clinical = self.clinical.set_index('submitter_id')
            
    def cleanup_inconsistencies(self):
        with st.expander('Clean Up Inconsistencies'):
            st.markdown('''
                        # Consolidate `race` and `ethnicity`
                        ''')    
            st.write('Whenever race value is "white" or missing replace it by ethnicity value (if present). Then drop ethnicity column.')
        
            race_subset = self.clinical['race'].isnull()
            ethnicity_subset = ~self.clinical['ethnicity'].isnull()
            subset = race_subset & ethnicity_subset
            self.clinical.loc[subset, 'race'] = self.clinical.loc[subset, 'ethnicity']
            
            race_subset = (self.clinical['race'] == 'white')
            ethnicity_subset = (~self.clinical['ethnicity'].isnull() &
                                (self.clinical['ethnicity'] == 'hispanic or latino'))
            subset = race_subset & ethnicity_subset
            self.clinical.loc[subset, 'race'] = self.clinical.loc[subset, 'ethnicity']
            
            st.write(self.clinical.loc[self.clinical['race'] == 'white', ].shape)
            
            self.clinical = self.clinical.drop('ethnicity', axis=1)
            
            
            st.markdown('''
                        # Missing label data
                        ''')
            st.write('The data show some inconsistencies, such as patients missing `vital_status` information, showing negative `days_to_last_follow_up` values, or missing days_to_death values')
            
            st.write('`## Vital Status`')
            st.write('~~ MISSING DATA ~~')
            
            skip = ['project_id', 'gender', 'race', 'ethnicity', 'prior_malignancy',
                    'age_at_diagnosis', 'days_to_death', 'days_to_last_follow_up']

            n = self.clinical.shape[0]

            for v in self.clinical.columns:
                if v not in skip:
                    n_missing = sum(self.clinical[v].isnull())
                    st.write(f'{v}: {n_missing} ({round(n_missing / n * 100, 2)}%)')
            
            # Drop patients missing "vital_status" information        
            subset = ~self.clinical.vital_status.isna()
            self.clinical = self.clinical.loc[subset] 
            
            
            st.write('`## Both duration values')
            st.write('Patients missing both time to death and time to last follow up variables cannot be included in a survival study.')
            
            missing_duration_data = self.clinical[
            self.clinical['days_to_death'].isna() &
            self.clinical['days_to_last_follow_up'].isna()]

            st.write('patients missing both duration columns:', missing_duration_data.shape[0])
            
            st.write(missing_duration_data.head())

            
            # Remove missing data
            subset = ~(self.clinical['days_to_death'].isna() &
                    self.clinical['days_to_last_follow_up'].isna())
            self.clinical = self.clinical.loc[subset]
            
            st.write(self.clinical.shape)
            
            st.write('## Required Duration Value')
            st.write('Patients alive at the end of the study require time to last follow up information. Dead patients require time to death information.')
            
            
            val = self.clinical[(self.clinical.vital_status == 'Alive') &
                    self.clinical.days_to_last_follow_up.isna()].shape[0]
            st.write(f'patients `missing days_to_last_follow_up` when `vital_status` is `Alive`: `{val}`')
            
            val = self.clinical[(self.clinical.vital_status == 'Dead') &
               self.clinical.days_to_death.isna()].shape[0]
            st.write(f'patients missing `days_to_death` when `vital_status` is `Dead`: `{val}`')
            
            # Remove missing data
            subset = ~((self.clinical.vital_status == 'Dead') &
                    self.clinical.days_to_death.isna())
            self.clinical = self.clinical.loc[subset]
            
            st.markdown('''## Not missing `days_to_last_follow_up` when `vital_status` is `Dead`''')
            val = all(self.clinical[self.clinical.vital_status == 'Alive'].days_to_death.isna())
            st.write(f'Days to death" variable missing for all patients still alive? `{val}`')
            
            val = all(self.clinical[self.clinical.vital_status == 'Dead'].days_to_last_follow_up.isna())
            
            
            # Insert "NaN" in "days_to_last_follow_up" when "vital_status" is "Dead" 
            subset = self.clinical.vital_status == 'Dead'
            self.clinical.loc[subset, 'days_to_last_follow_up'] = None
            
            val = all(self.clinical[self.clinical.vital_status == 'Dead'].days_to_last_follow_up.isna())
            st.write(f'`Days to last follow up` variable missing for all dead patients? `{val}`')
                        
                        
            st.markdown('''## Negative durations''')
            st.write(self.clinical[self.clinical.days_to_last_follow_up < 0])
            # Remove data
            subset = ~((self.clinical.days_to_last_follow_up < 0) &
                    (self.clinical.vital_status == 'Alive'))
            self.clinical = self.clinical.loc[subset]
            st.write(self.clinical.shape)

            


Parser()

        
# clinical.columns
# label_cols = ['submitter_id', 'days_to_last_follow_up', 'vital_status', 'days_to_death']

# keep_cols = ['tumor_stage', 'age_at_diagnosis', 'prior_treatment', 'prior_malignancy',
#              'synchronous_malignancy', 'gender', 'race', 'ethnicity', 'disease',
#              'treatments_pharmaceutical_treatment_or_therapy',
#              'treatments_radiation_treatment_or_therapy']

# columns_to_drop = [col for col in clinical.columns if col not in label_cols + keep_cols]
# clinical = clinical.drop(columns=columns_to_drop)
# print('~~ MISSING DATA ~~')
# print()

# n = clinical.shape[0]

# for v in clinical.columns:
#     n_missing = sum(clinical[v].isnull())
#     if n_missing > 0:
#         if n_missing == n:
#             clinical = clinical.drop(columns=[v])
#         else:
#             print(f'{v}: {n_missing} ({round(n_missing / n * 100, 2)}%)')
# clinical.shape
# clinical.columns

# clinical['gender'].value_counts()

# clinical['race'].value_counts()

# clinical['ethnicity'].value_counts()

# clinical['prior_malignancy'].value_counts()

# clinical['vital_status'].value_counts()

# clinical['days_to_last_follow_up'].plot(kind='hist')

# clinical['days_to_death'].plot(kind='box')

# clinical['days_to_death'].sort_values(ascending=False).plot(use_index=False)

# clinical['days_to_last_follow_up'].plot(kind='box')

# clinical['age_at_diagnosis'].apply(lambda x: -x/365).plot(kind='box')

# clinical['age_at_diagnosis'].sort_values(ascending=False).plot(use_index=False)

# clinical.describe()

# clinical.info()

# clinical = clinical.rename(columns={'disease': 'project_id'})

# clinical = clinical.set_index('submitter_id')

# race_subset = clinical['race'].isnull()
# ethnicity_subset = ~clinical['ethnicity'].isnull()
# subset = race_subset & ethnicity_subset
# clinical.loc[subset, 'race'] = clinical.loc[subset, 'ethnicity']
# race_subset = (clinical['race'] == 'white')
# ethnicity_subset = (~clinical['ethnicity'].isnull() &
#                     (clinical['ethnicity'] == 'hispanic or latino'))
# subset = race_subset & ethnicity_subset
# clinical.loc[subset, 'race'] = clinical.loc[subset, 'ethnicity']
# clinical.loc[clinical['race'] == 'white', ].shape

# clinical = clinical.drop('ethnicity', axis=1)

# print('~~ MISSING DATA ~~')
# print()
# skip = ['project_id', 'gender', 'race', 'ethnicity', 'prior_malignancy',
#         'age_at_diagnosis', 'days_to_death', 'days_to_last_follow_up']

# n = clinical.shape[0]

# for v in clinical.columns:
#     if v not in skip:
#         n_missing = sum(clinical[v].isnull())
#         print(f'{v}: {n_missing} ({round(n_missing / n * 100, 2)}%)')
# # Drop patients missing "vital_status" information
# subset = ~clinical.vital_status.isna()
# clinical = clinical.loc[subset]
# missing_duration_data = clinical[
#     clinical['days_to_death'].isna() &
#     clinical['days_to_last_follow_up'].isna()]

# print('# patients missing both duration columns:', missing_duration_data.shape[0])
# missing_duration_data.head()

# # Remove missing data
# subset = ~(clinical['days_to_death'].isna() &
#            clinical['days_to_last_follow_up'].isna())
# clinical = clinical.loc[subset]
# clinical.head()

# print('# patients missing "days_to_last_follow_up" when "vital_status" is "Alive":',
#       clinical[(clinical.vital_status == 'Alive') &
#                clinical.days_to_last_follow_up.isna()].shape[0])
# print('# patients missing "days_to_death" when "vital_status" is "Dead":',
#       clinical[(clinical.vital_status == 'Dead') &
#                clinical.days_to_death.isna()].shape[0])
# # Remove missing data
# subset = ~((clinical.vital_status == 'Dead') &
#            clinical.days_to_death.isna())
# clinical = clinical.loc[subset]
# print('"Days to death" variable missing for all patients still alive?',
#       all(clinical[clinical.vital_status == 'Alive'].days_to_death.isna()))
# print('"Days to last follow up" variable missing for all dead patients?',
#       all(clinical[clinical.vital_status == 'Dead'].days_to_last_follow_up.isna()))
# # Insert "NaN" in "days_to_last_follow_up" when "vital_status" is "Dead" 
# subset = clinical.vital_status == 'Dead'
# clinical.loc[subset, 'days_to_last_follow_up'] = None
# print('"Days to last follow up" variable missing for all dead patients?',
#       all(clinical[clinical.vital_status == 'Dead'].days_to_last_follow_up.isna()))
# clinical[clinical.days_to_last_follow_up < 0]
# # Remove data
# subset = ~((clinical.days_to_last_follow_up < 0) &
#            (clinical.vital_status == 'Alive'))
# clinical = clinical.loc[subset]