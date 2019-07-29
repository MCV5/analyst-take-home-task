
# coding: utf-8

# In[16]:


import os.path
import os
import glob
import pandas as pd

Data1='C:\\Data\\CHOP\\datasets\\allergies.csv'
Data2='C:\\Data\\CHOP\\datasets\\encounters.csv'
Data3='C:\\Data\\CHOP\\datasets\\medications.csv'
Data4='C:\\Data\\CHOP\\datasets\\patients.csv'
Data5='C:\\Data\\CHOP\\datasets\\procedures.csv'
allergies=pd.read_csv(Data1)
encounters=pd.read_csv(Data2)
medications=pd.read_csv(Data3)
patients=pd.read_csv(Data4)
procedure=pd.read_csv(Data5)


# In[284]:


procedure


# In[283]:


medications=medications[medications['START']>'1999-07-01']
Df_medications=pd.DataFrame(medications,columns=['START','PATIENT','ENCOUNTER','CODE','DESCRIPTION','REASONCODE','REASONDESCRIPTION'])
Df_medications=Df_medications.rename(columns={'ENCOUNTER':'Id'})

# Df_medications_temp=pd.merge(Df_medications,df_patients,how='inner',on=['PATIENT'])
# pd.merge(Df_medications_temp,df_Drug_Overdose,how='left',on=['Id','PATIENT'])


# In[44]:


Df_patien=pd.DataFrame(patients,columns=['Id','FIRST','LAST','BIRTHDATE','DEATHDATE'])
df_patients=Df_patien.rename(columns={'Id':'PATIENT'})


# In[300]:


medications[medications['DESCRIPTION']=='Fentanyl 100 MCG']


# In[177]:


Drug_Overdose=encounters[encounters['REASONDESCRIPTION']=='Drug overdose']
df_Drug_Overdose=pd.DataFrame(Drug_Overdose,columns={'Id','START','STOP','PATIENT','PROVIDER','CODE','REASONCODE','REASONDESCRIPTION'})
df_Drug_Overdose['START'] = pd.to_datetime(df_Drug_Overdose['START'])
df_Drug_Overdose['STOP'] = pd.to_datetime(df_Drug_Overdose['STOP'])
df_Drug_Overdose[df_Drug_Overdose['START']>'1999-07-01']
df_Drug_Overdose[df_Drug_Overdose['STOP']>'1999-07-01']


# In[264]:


pd.merge(df_patients,df_Drug_Overdose,on='PATIENT')


# In[481]:


overdose_patient=pd.merge(df_patients,df_Drug_Overdose,how='inner',on='PATIENT')
overdose_patient=overdose_patient.rename(columns={'PATIENT':'PATIENT_ID','Id':'Encounter_Id'})
overdose_patient['BIRTHDATE'] = pd.to_datetime(overdose_patient['BIRTHDATE'])
overdose_patient['DEATHDATE'] = pd.to_datetime(overdose_patient['DEATHDATE'])
overdose_patient['START'] = pd.to_datetime(overdose_patient['START'])
overdose_patient['STOP'] = pd.to_datetime(overdose_patient['STOP'])
overdose_patient[overdose_patient['START']>'1999-07-01']
overdose_patient=overdose_patient[overdose_patient['STOP']>'1999-07-01']


# In[482]:


temp1=pd.DatetimeIndex(overdose_patient['BIRTHDATE']).year  
temp2=pd.DatetimeIndex(overdose_patient['START']).year 
overdose_patient['Age']=temp2-temp1
overdose_patient['START']=[datetime.datetime.date(d) for d in overdose_patient['START']]
overdose_patient['STOP']=[datetime.datetime.date(d) for d in overdose_patient['STOP']]
overdose_patient=overdose_patient[(overdose_patient['Age'] >= 18) & (overdose_patient['Age'] <=35)]
overdose_patient


# In[483]:


medications=medications[medications['START']>'1999-07-01']
Df_medications=pd.DataFrame(medications,columns=['START','PATIENT','ENCOUNTER','DESCRIPTION'])
Df_medications=Df_medications.rename(columns={'START':'Medication_START','PATIENT':'PATIENT_ID','ENCOUNTER':'Encounter_Id'})
overdose_medica=pd.merge(overdose_patient,Df_medications,how='left',on=['PATIENT_ID','Encounter_Id'])


# In[484]:


overdose_medica


# In[485]:


import numpy as np
OPIOID =['Hydromorphone 325 MG','Fentanyl 100 MCG','Oxycodone-acetaminophen 100 Ml']
overdose_medica['COUNT_CURRENT_MEDS']= np.where(overdose_medica['DESCRIPTION'].isnull(),0,1)
overdose_medica['CURRENT_OPIOID_IND']=np.where(overdose_medica['DESCRIPTION'].isin(OPIOID),1,0)


# In[486]:


overdose_medica


# In[487]:


overdose_medica['START'] = pd.to_datetime(overdose_medica['START'])
overdose_medica['STOP'] = pd.to_datetime(overdose_medica['STOP'])
overdose_medica.sort_values(['PATIENT_ID','START'])


# In[488]:


overdose_medica['READMISSION_180_DAY_IND']=overdose_medica.groupby('PATIENT_ID').apply(lambda x : (x['START'].shift(-1)-x['STOP']).dt.days.le(180).astype(int)).reset_index(drop=True)
overdose_medica['READMISSION_90_DAY_IND']=overdose_medica.groupby('PATIENT_ID').apply(lambda x : (x['START'].shift(-1)-x['STOP']).dt.days.le(90).astype(int)).reset_index(drop=True)
overdose_medica['READMISSION_30_DAY_IND']=overdose_medica.groupby('PATIENT_ID').apply(lambda x : (x['START'].shift(-1)-x['STOP']).dt.days.le(30).astype(int)).reset_index(drop=True)


# In[489]:


overdose_medica


# In[490]:


overdose_medica['First_READMISSION']=overdose_medica.groupby('PATIENT_ID')['START'].rank(ascending=True)


# In[491]:


overdose_medica


# In[492]:


overdose_medica['First_READMISSION'] = overdose_medica.apply(lambda x: x['START'] if x['First_READMISSION']==1.0 else 'N/A', axis=1)


# In[437]:


overdose_medica


# In[493]:


overdose_medica['DEATH_AT_VISIT_IND']=np.where((overdose_medica["DEATHDATE"] > overdose_medica["START"]) & (overdose_medica["DEATHDATE"] <= overdose_medica["STOP"]),1,0)


# In[510]:


overdose_medica


# In[511]:


overdose_medica['DEATH_AT_VISIT_IND']=np.where((pd.isnull(pd.to_datetime(overdose_medica['DEATHDATE']))) & (overdose_medica['DEATH_AT_VISIT_IND']==0),'N/A',overdose_medica['DEATH_AT_VISIT_IND'])


# In[513]:


Finla_overdose_patients=pd.DataFrame(overdose_medica,columns={'PATIENT_ID','Encounter_Id','START','Age','DEATH_AT_VISIT_IND','COUNT_CURRENT_MEDS',
                                                              'CURRENT_OPIOID_IND','READMISSION_180_DAY_IND','READMISSION_90_DAY_IND','READMISSION_30_DAY_IND','First_READMISSION'})
Finla_overdose_patients=Finla_overdose_patients.rename(columns={'START':'HOSPITAL_ENCOUNTER_DATE','Age':'AGE_AT_VISIT'})
Finla_overdose_patients=Finla_overdose_patients[['PATIENT_ID','Encounter_Id','HOSPITAL_ENCOUNTER_DATE','AGE_AT_VISIT','DEATH_AT_VISIT_IND','COUNT_CURRENT_MEDS',
                                                              'CURRENT_OPIOID_IND','READMISSION_180_DAY_IND','READMISSION_90_DAY_IND','READMISSION_30_DAY_IND','First_READMISSION']]


# In[525]:


Finla_overdose_patients.to_csv(r'C:\Data\CHOP\datasets\final_data.csv',sep='|',header=True)

