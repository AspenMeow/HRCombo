
# coding: utf-8

# In[307]:


get_ipython().run_line_magic('run', '"H:\\Python\\connect to database.ipynb"')


# In[308]:


import numpy as np
import pandas as pd
import re


# In[309]:


#pull primary assignments with jobs poistion pay info
#inner join with cx, job,base pay
def primds(hrexdt, ooiexdt, dtfilter):
    PRIM =  pd.read_sql("select * from HR_"+hrexdt+".HR_PA_EMP_PRIMARY_ASSIGNMENT_V                    where '"+dtfilter+"' between start_date and end_date", EDW)
    PSV = pd.read_sql("select * from HR_"+hrexdt+".HR_PA_PERSON_SV                    where '"+dtfilter+"' between start_date and end_date", EDW)
    POS = pd.read_sql("select * from HR_"+hrexdt+".HR_OM_POSITIONS_V                    where '"+dtfilter+"' between start_date and end_date", EDW)
    JOB = pd.read_sql("select * from HR_"+hrexdt+".HR_OM_JOB_V                    where '"+dtfilter+"' between start_date and end_date", EDW)
    ACTION = pd.read_sql("select * from HR_"+hrexdt+".HR_PA_ACTIONS_V                    where '"+dtfilter+"' between start_date and end_date and EMP_STATUS_CD in ('1','3') ", EDW)

    CX = pd.read_sql("select * from HR_"+hrexdt+".HR_PA_PERNR_CROSSWALK_V ", EDW)
    DSPEC = pd.read_sql("select * from HR_"+hrexdt+".HR_PA_DATE_SPECIFICATIONS_SV                    where '"+dtfilter+"' between start_date and end_date", EDW)

    PDETAIL = pd.read_sql("select * from HR_"+hrexdt+".HR_PA_PERSON_DETAIL_SV                    where '"+dtfilter+"' between start_date and end_date", EDW)
    RES = pd.read_sql("select * from HR_"+hrexdt+".HR_PA_RESIDENCE_STATUS_SV                    where '"+dtfilter+"' between start_date and end_date", EDW)

    BN = pd.read_sql("select * from HR_"+hrexdt+".HR_BN_BNFT_SVC_INFO_V                    where '"+dtfilter+"' between start_date and end_date", EDW)
    ED = pd.read_sql("select * from HR_"+hrexdt+".HR_PA_PERSON_EDUCATION_V", EDW)

    BASEPAY = pd.read_sql("select * from HR_"+hrexdt+".HR_PA_BASE_PAY_V                    where '"+dtfilter+"' between base_pay_start_date and base_pay_end_date and SRC_STYPE = '0'", EDW)

    FASDETAIL = pd.read_sql("select * from HR_"+hrexdt+".HR_PA_FAS_DETAILS_V                    where '"+dtfilter+"' between start_date and end_date", EDW)
    
    Newhire = pd.read_sql("select * from HR_"+hrexdt+".HR_PA_ACTIONS_V                    where ACTN_TYP_CD='ZA' and   EMP_STATUS_CD in ('1','3') and "+                     "start_date between '01-JULY-"+dtfilter[7:]+"' and '30-SEP-"+dtfilter[7:]+"'"  , EDW)
    
    LTD = pd.read_sql("select distinct pers_nbr, max(start_date) as max_actn_dt from HR_"+hrexdt+".HR_PA_ACTIONS_V                    where ACTN_TYP_CD='ZQ' and   EMP_STATUS_CD in ('1') and cust_status_cd in (6,8) group by pers_nbr having "+                     "max(start_date) <= '01-OCT-"+dtfilter[7:]+"'" , EDW)
    LTD['LTD']='Y'

    var = [x for x in PDETAIL.columns if re.match(r'racial_cat_cd_\d+',x) or x=='ethnc_cd']
    PDETAIL= PDETAIL.set_index('pers_id').filter(items=var)

    ActivePerson = ACTION[['pers_nbr','emp_status_cd','emp_status_nm']].drop_duplicates()
    ActivePerson['Active']='Y'

    EDPrsn = ED.groupby('pers_id')['cert_cd'].max().reset_index()
    Newhire = Newhire[['pers_nbr']].drop_duplicates()
    Newhire['NewHire']='Y'
#EDPrsn.head()

    return PRIM.drop(['curr_ind', 'src_last_updated_by', 'src_last_update_date', 'created_by',                   'create_date', 'last_updated_by', 'last_update_date'], axis=1).       join(CX.set_index('pers_nbr').loc[:,['pers_id']], on='pers_nbr', how='inner').       join(JOB.set_index('job_cd').loc[:,['job_ttl','eeo_cd','eeo_nm','emp_cat_cd','emp_cat_nm','fac_rnk_cd']], on='job_cd', how='inner').        join(PSV.set_index('pers_id').loc[:,['ssn','uuid','fst_nm','mid_nm', 'lst_nm','birth_date', 'gndr_cd', 'gndr_nm','zpid']], on='pers_id').        join(PDETAIL, on='pers_id').        join(RES.set_index('pers_id').loc[:,['res_status_cd','res_status_nm']] , on='pers_id').        join(DSPEC.set_index('pers_nbr').loc[:,['cont_svc_date']], on='pers_nbr').        join(ActivePerson.set_index('pers_nbr'), on='pers_nbr').        join(BN.set_index('pers_nbr').loc[:,['acum_fte_svc_mths','vstd_date']], on='pers_nbr').        join(EDPrsn.set_index('pers_id'), on='pers_id').        join(BASEPAY.set_index('pers_nbr').loc[:,['pay_scale_typ_cd','pay_scale_area_cd','wage_typ_cd_2','pymt_wage_typ_amt_2',                                                  'pay_scale_grp_cd','pay_scale_lvl_cd','capc_util_lvl', 'anl_sal']], on='pers_nbr', how='inner').        join(POS.set_index('pos_nbr').loc[:,['pos_ttl']], on='pos_nbr').        join(FASDETAIL.set_index('pers_id').loc[:,['status_cd', 'status_nm','tenure_cont_grant_dept','tenure_cont_status_grant_date',                                                   'tenure_exmp_agr_date','tenure_cont_sys_entr_date','promo_to_cur_rnk_entr_date']], on='pers_id').        join(Newhire.set_index('pers_nbr'), on='pers_nbr').        join(LTD.set_index('pers_nbr'), on='pers_nbr')


# In[310]:


##########get addtional ranks from addtional assignment#########
def adtrank(hrexdt, ooiexdt, dtfilter):
    ASGN = pd.read_sql("select * from HR_"+hrexdt+".hr_pa_fas_assignment_v                    where '"+dtfilter+"' between start_date and end_date", EDW)
    #transpose the columns to rows
    asgncol  =[x for x in ASGN.columns if not re.match(r'prim_asgn_also_rpt_to_\d',x) and not re.match(r'\w+_org_cd_\d+',x) ]
    asgndf = ASGN.filter(items=asgncol).set_index(['pers_nbr','start_date','end_date']).            filter(regex= r'\w+_\d')
    asgndfL = asgndf.stack().reset_index().rename(columns={'level_3':'variable',0:'value'})
    asgndfL['seq']= asgndfL['variable'].str.extract(r'\w+_(\d)$') 
    asgndfL['grp']= asgndfL['variable'].str.extract(r'(\w+)_\d$')
    asgndfL =  asgndfL.drop('variable',axis=1).set_index(['pers_nbr','start_date','end_date','seq','grp']).unstack()
    asgndfL.columns=asgndfL.columns.droplevel()
    asgndfL= asgndfL.reset_index()

    
    #fac_rnk_cd doesn't exsit in 2011, if fas_rnk_cd exist use fac_rnk_cd, if not use fas_asgn_nm
    #4 is professor 1 is instructor

    if 'fas_rnk_cd' in asgndfL.columns:
        asgndfL['DerivedRnk']=  asgndfL['fas_rnk_cd'].str.slice(3,4).astype(int)
    elif 'fas_asgn_nm' in asgndfL.columns:
        asgndfL['DerivedRnk']= np.where(asgndfL['fas_asgn_nm'].str.contains('PROFESSORIAL'), np.nan,
                                 np.where(asgndfL['fas_asgn_nm'].str.contains('ASSISTANT INSTRUCTOR'),  np.nan,
                                   np.where( asgndfL['fas_asgn_nm'].str.contains('INSTRUCTOR'), 1,
                                       np.where(asgndfL['fas_asgn_nm'].str.contains('ASSISTANT PROFESSOR'),2,
                                           np.where(asgndfL['fas_asgn_nm'].str.contains('ASSOCIATE PROFESSOR'),3,
                                                   np.where(asgndfL['fas_asgn_nm'].str.contains('PROFESSOR'),4, 
                                                           np.where(asgndfL['fas_asgn_nm'].str.startswith('PROF'),4, np.nan)))))))
    else:
        print('no addtl rank field')

    #BC Derived Rank, exclude certain asgn_type and add lecturer
    asgndfL['DerivedRnk_BC']= np.where(asgndfL['asgn_typ_cd'].isin(['2','4','8','3','9','A','B']), np.nan,
                                  np.where( asgndfL['DerivedRnk'].isnull()==False, asgndfL['DerivedRnk'] ,
                                          np.where(asgndfL['fas_asgn_nm'].str.contains('LECTURER'), 0, np.nan)))
    return asgndfL


# In[311]:


##OOI for org-hirachery##########

def ooihr(hrexdt, ooiexdt, dtfilter):
    #get HR org hirachy from ooi_ooi_org_struct_cd_lvl_rv
    OOIV = pd.read_sql('select * from OOI_'+ooiexdt+'.ooi_ooi_org_struct_cd_lvl_rv ', EDW)
    OOIHR = OOIV.query('structure_type=="HR"').loc[:,['org_code','org_full_name', 'org_type', 'mau_code','mau_name','dept_code','dept_name']]
    OOIHR['type']=1
    #get HR org hirachy from OOI_OOI_ORG_STRUCT_CODE_AV
    ORG_V = pd.read_sql('select * from OOI_'+ooiexdt+'.OOI_OOI_ORG_V', EDW)
    STRUCT = pd.read_sql('select * from OOI_'+ooiexdt+'.OOI_OOI_ORG_STRUCT_CODE_AV', EDW)
    STRUCTHR = STRUCT.query('struct_type=="HR"').    join(ORG_V.query('org_type=="MAU"').set_index('org_ref_code').loc[:,['org_full_name']], rsuffix='_mau', on='mau').    join(ORG_V.query('org_type=="DP"').set_index('org_ref_code').loc[:,['org_full_name']], rsuffix='_dept', on='dept').    loc[:,['org_code','org_full_name','org_type','mau','org_full_name_mau','dept','org_full_name_dept']]
    STRUCTHR['type']=2
    #put 2 sources together and use ooi_ooi_org_struct_cd_lvl_rv answer first if mutliple
    STRUCTHR.columns = OOIHR.columns
    HROOI = pd.concat([OOIHR,STRUCTHR  ])
    HROOI['r']=HROOI.groupby('org_code')['type'].transform(min)
    HROOI = HROOI.query('r==type').drop(['type','r'], axis=1)
    return HROOI



# In[312]:


def primpop(hrexdt, ooiexdt, dtfilter):
    #call primary function to pull base table containing primary position job pay info 
    prim = primds(hrexdt=hrexdt, ooiexdt=ooiexdt, dtfilter=dtfilter)
    #filter include only academics, support staff and Grad assistant
    #remove non-employee/no-pay
    #remove non-active
    #remove PRIM_ASGN_END_DATE before frozen date
    prim = prim[ prim['emp_cat_cd'].str.slice(0,1).isin(['F','S','G'])].query('emp_grp_cd != "5"').query('Active=="Y"')
    prim['cutdt']= np.datetime64(pd.to_datetime(dtfilter))
    #since 2013, prim_asgn_end_date datatype changed "datetime64[ns]"
    #pers_id=="00140406" in 2015 frozn have prim_asgn_end_date of 9999-12-31
    if prim['prim_asgn_end_date'].dtype == 'object':
        prim['prim_asgn_end_date'] = np.where(prim['prim_asgn_end_date'].astype(str).str.slice(0,4)=='9999', None,                           prim['prim_asgn_end_date'].astype(str) )
    prim['prim_asgn_end_date']= pd.to_datetime(prim['prim_asgn_end_date'])
    prim = prim[(prim['prim_asgn_end_date'] == np.datetime64('1900-01-01')) | (prim['prim_asgn_end_date'].isnull()) | ( prim['prim_asgn_end_date'] >=  prim['cutdt'])]
    #first character of emp_cat_cd
    prim['emp_cat_cd_1']= prim['emp_cat_cd'].str.slice(0,1)
    
    #2011 job table do not have fac_rnk_cd, code fac_rnk_cd based on job_ttl text field
    # when fac_rnk_cd exist from source table, pick the last character and convert to numeric field
    if prim['fac_rnk_cd'].isnull().sum() < len(prim):
        prim['fac_rnk_cd'] = np.where(prim['fac_rnk_cd'].isnull(), np.nan,  prim['fac_rnk_cd'].str.slice(3,4).astype(float))
        
    else:
        prim['fac_rnk_cd']= np.where(prim['job_ttl'].str.contains('professorial',case=False), np.nan,
                                np.where(prim['job_ttl'].str.contains('ASSISTANT INSTRUCTOR', case=False),  np.nan,
                                   np.where( prim['job_ttl'].str.contains('INSTRUCTOR' , case=False), 1,
                                       np.where(prim['job_ttl'].str.contains('ASSISTANT PROFESSOR',case=False),2,
                                           np.where(prim['job_ttl'].str.contains('ASSOCIATE PROFESSOR',case=False),3,
                                                   np.where(prim['job_ttl'].str.contains('PROF,',case=False),4, 
                                                           np.where(prim['job_ttl'].str.upper().str.endswith('PROF'),4,
                                                                   np.where(prim['job_ttl'].str.contains('PROFESSOR',case=False),4,
                                                                           np.where(prim['job_ttl'].str.contains('PROF-',case=False),4,np.nan)))))))))
        
    #add total FTE (cul) by first character of emp_cat_cd_1
    CUL= prim[['pers_id','pers_nbr','emp_cat_cd','capc_util_lvl','asgn_typ_cd','emp_sgrp_cd','emp_cat_cd_1']].        dropna(subset=['pers_id', 'capc_util_lvl','emp_cat_cd'])


    CULag = CUL.groupby(['pers_id','emp_cat_cd_1'])['capc_util_lvl'].sum().reset_index().        rename(columns={'capc_util_lvl':'cul'})
        
    prim = prim.join(CULag.set_index(['pers_id','emp_cat_cd_1']), on=['pers_id','emp_cat_cd_1'] )
    
    #add additional ranks, max rank across rows
    DerRank = adtrank(hrexdt=hrexdt, ooiexdt=ooiexdt, dtfilter=dtfilter).groupby('pers_nbr')['DerivedRnk'].max().reset_index()
    DerRankBC = adtrank(hrexdt=hrexdt, ooiexdt=ooiexdt, dtfilter=dtfilter).groupby('pers_nbr')['DerivedRnk_BC'].max().reset_index()
    DerRank= DerRank.join(DerRankBC.set_index('pers_nbr'), on='pers_nbr')
    prim = prim.join(DerRank.set_index(['pers_nbr']), on=['pers_nbr'] )
    #derived_rank is max rank among primary job rank or addtional rank
    prim['DerivedRnk_BC1']= np.where(prim['DerivedRnk_BC']==0, np.nan,prim['DerivedRnk_BC'] )
    prim['derived_rank']= prim[['fac_rnk_cd','DerivedRnk']].apply(np.nanmax, axis=1)
    prim['derived_rank_rev']= prim[['fac_rnk_cd','DerivedRnk_BC1']].apply(np.nanmax, axis=1)
    
    #hard code derived rank for certain job_cd for 2011
    
    if dtfilter.endswith('11'):
        prim['derived_rank'] = np.where(prim['job_cd'].isin(['20001600','20001601']),4,prim['derived_rank'])
        prim['derived_rank_rev'] = np.where(prim['job_cd'].isin(['20001600','20001601']),4,prim['derived_rank_rev'])
        
    #code for Employee_category
    #status_cd='COTR' and have rank continuning staff?
    #BC fixed term faculty restrict on emp_cat_cd in('FMM','FFF','FXE','FHF','FNF') exclude specialist with rank
    prim['Employee_category']= np.where(prim['emp_cat_cd_1']=='G','Grad Asst',
                                       np.where(prim['emp_cat_cd_1']=='S', 'Non Acad',
                                               np.where(prim['status_cd'].isin(['TENR','TPRO']),'Tenure Sys',
                                                       np.where((prim['derived_rank_rev'].notnull()) & (prim['status_cd'].isnull()) ,'Fix Fac',
                                                               np.where((prim['derived_rank_rev'].notnull()) & (prim['status_cd'].str.endswith('E')), 'Fix Fac',
                                                                       np.where((prim['status_cd'].notnull()) & ( prim['status_cd'].str.endswith('E') == False),'Cont Staff',
                                                                               np.where(prim['derived_rank_rev'].isnull(),'Fix Staff', None)))))))
    
    #rank for academics= RPNAME in Acad
    prim['RANK']= np.where( (prim['status_cd'].isin(['TENR','TPRO']) ) & (prim['derived_rank_rev'].isnull()), 'Professor',
                        np.where(prim['derived_rank_rev']==4, 'Professor',
                                np.where(prim['derived_rank_rev']==3, 'Assoc Professor',
                                        np.where(prim['derived_rank_rev']==2,'Asst Professor',
                                                np.where(prim['derived_rank_rev']==1, 'Instructor', None)))))


    #newhire
    prim['New_Hires'] = np.where( (prim['cont_svc_date']>= np.datetime64(str(int(hrexdt[0:4])-1)+'-10-01') ) &                              (prim['cont_svc_date']<= np.datetime64(hrexdt[0:4]+ '-10-01') )  ,1,0 )
    #new hire using action
    prim['New_Hires_Act'] = np.where(prim['NewHire']=='Y',1, 0)
    #age
    prim['Age']= (( prim['cutdt']-prim['birth_date']).dt.days)/365
    #Citizen
    prim['CITIZEN']= np.where(prim['res_status_cd']=='C','1',
                         np.where(prim['res_status_cd']=='N','3',
                                 np.where(prim['res_status_cd'].isin(['S','T','A']),'2','1')))
    #gender
    #no gndr_cd given Male by default
    prim['GENDER']= np.where(prim['gndr_cd']=='1','M',
                        np.where(prim['gndr_cd']=='2','F','M'))
    
    #ethnic need to recheck coding for ethnic, records without racial_cat_cd_1 given white?
    prim['ETHNIC']= np.where(prim['ethnc_cd']=='E1',3,
                        np.where(prim['racial_cat_cd_1'] == 'R1',4,
                                np.where(prim['racial_cat_cd_1']=='R2',7,
                                        np.where(prim['racial_cat_cd_1']=='R3',1,
                                                np.where(prim['racial_cat_cd_1']=='R4',6,
                                                        np.where(prim['racial_cat_cd_1']=='R5',5, 5))))))
    #other ethnic
    
    def racerd(s):
        if s=='R1':
            r='4'
        elif s=='R2':
            r='7'
        elif s=='R3':
            r='1'
        elif s=='R4':
            r='6'
        elif s=='R5':
            r='5'
        else:
            r=None
        return r

    prim['ETHNIC1']= prim['racial_cat_cd_1'].apply(racerd)
    prim['ETHNIC2']= prim['racial_cat_cd_2'].apply(racerd)
    prim['ETHNIC3']= prim['racial_cat_cd_3'].apply(racerd)
    prim['ETHNIC4']= prim['racial_cat_cd_4'].apply(racerd)
    prim['ETHNIC5']= prim['racial_cat_cd_5'].apply(racerd)
    #this revision was done in later years in Acad but in nonAcadFTE was done from 2011
    prim['ETHNIC1']= np.where(prim['ethnc_cd']=='E1', prim['ETHNIC1'],prim['ETHNIC2'])
    prim['ETHNIC2']= np.where(prim['ethnc_cd']=='E1', prim['ETHNIC2'],prim['ETHNIC3'])
    prim['ETHNIC3']= np.where(prim['ethnc_cd']=='E1', prim['ETHNIC3'],prim['ETHNIC4'])
    prim['ETHNIC4']= np.where(prim['ethnc_cd']=='E1', prim['ETHNIC4'],prim['ETHNIC5'])
    prim['ETHNIC5']= np.where(prim['ethnc_cd']=='E1', prim['ETHNIC5'], None)
    
    #first character of EEO
    prim['EEO_1']= prim['eeo_cd'].str.slice(0,1)
    #name
    prim['Name']= prim['lst_nm'].str.strip()+', '+prim['fst_nm']
    
    
    #add OOI for primary job org
    HROOI= ooihr(hrexdt=hrexdt, ooiexdt=ooiexdt, dtfilter=dtfilter)
    prim = prim.join(HROOI.set_index('org_code').                     loc[:,['org_full_name','mau_code','mau_name','dept_code','dept_name']], on='org_cd')
    prim['CYEAR'] = hrexdt[:4]

    return prim
        
    
    


# In[313]:


import warnings
warnings.filterwarnings('ignore')
prim11 = primpop(hrexdt='20111025', ooiexdt='201110', dtfilter='01-OCT-11')

prim12 = primpop(hrexdt='201210', ooiexdt='201210', dtfilter='01-OCT-12')
#primds(hrexdt, ooiexdt, dtfilter)
prim13 = primpop(hrexdt='20131018', ooiexdt='20131018', dtfilter='01-OCT-13')

prim14 = primpop(hrexdt='20141025', ooiexdt='20141025', dtfilter='01-OCT-14')

prim15 = primpop(hrexdt='20151027', ooiexdt='20151027', dtfilter='01-OCT-15')

prim16 = primpop(hrexdt='20161019', ooiexdt='20161019', dtfilter='01-OCT-16')

prim17 = primpop(hrexdt='20171026', ooiexdt='20171026', dtfilter='01-OCT-17')


# In[ ]:


###########combining together####################


# In[314]:


prim = pd.concat([prim11,prim12,prim13,prim14,prim15,prim16,prim17])


# In[315]:


#prim= prim[prim17.columns.tolist()]


# In[316]:


prim.shape


# In[317]:


prim['emp_status_cd'].value_counts()


# In[318]:


prim['TempOnCall']= np.where(prim['emp_sgrp_cd'].isin(['AC','AW']),'Y','N')


# In[319]:


#support staff with mulitple poistion
mstaff = prim[prim['emp_cat_cd_1'].isin(['F','S'])].groupby(['pers_id','CYEAR']).filter(lambda x: len(x) > 1)


# In[320]:


mstaff.shape


# In[321]:


#current tenure org
tenOrg = pd.read_sql("select distinct org_cd, tenure_org_flag from OPB.HR_PA_CURR_EMP_ORG_RELA_RV  where tenure_org_flag='Y'", EDW)


# In[322]:


mstaff= mstaff.join(tenOrg.set_index('org_cd'), on='org_cd')
mstaff['Acad']= np.where(mstaff['emp_cat_cd_1']=='F',1,0)
mstaff['LTDTempexlusion']= np.where(mstaff['TempOnCall']=='Y',0,
                             np.where(mstaff['LTD'].notnull(),0,1))
mstaff['acadunit']= np.where(mstaff['tenure_org_flag'].isnull(),0,1)
mstaff['paid']= np.where(mstaff['anl_sal']>0,1,0)
mstaff['ContEndDate']=np.where( mstaff['prim_asgn_end_date']==np.datetime64('1900-01-01'),1,0 ) 


# In[323]:


varlist = ['Acad','LTDTempexlusion','paid','capc_util_lvl','pay_scale_lvl_cd','acadunit','ContEndDate','start_date','pers_nbr']
def rowselect(df):
    df=df.reset_index(drop=True)
    for i in varlist:
        if i in ('start_date','pers_nbr'):
            df=df[df[i]==df[i].min()]
        else:
            df=df[df[i]==df[i].max()]
            
        
        df['deterfac']=i
        if len(df)==1:
            break
    return df
    
    


# In[324]:


mlist = mstaff.groupby(['CYEAR','pers_id']).apply(rowselect).loc[:,['pers_nbr','deterfac']].reset_index().drop('level_2', axis=1)


# In[325]:


mlist


# In[326]:


prim = prim.join(mlist.set_index(['CYEAR','pers_id','pers_nbr']), on=['CYEAR','pers_id','pers_nbr'])


# In[327]:


prim['FacStaff']= np.where(prim['emp_cat_cd_1'].isin(['F','S']),'Y','N')
prim['PosNum']=prim.groupby(['CYEAR','pers_id','FacStaff'])['pers_nbr'].transform('count')


# In[328]:


prim.shape


# In[329]:


prim['FacStafPosFlag']= np.where( (prim['emp_cat_cd_1'].isin(['S','F']) ) & (prim['PosNum']==1),1,
                              np.where((prim['emp_cat_cd_1'].isin(['S','F'])) & (prim['PosNum']>1) & (prim['deterfac'].notnull()),1,0))


# In[330]:


prim['FacStafPosFlag'].value_counts()


# In[331]:


prim['IPEDS_Flag']= np.where( prim['TempOnCall']=='Y','N',
                            np.where(prim['LTD']=='Y','N',
                                    np.where((prim['emp_cat_cd_1'].isin(['S','F'])) & (prim['FacStafPosFlag']==1),'Y','N' )))
#(prim['emp_cat_cd_1']=='F') | (prim['Staff']) 


# In[332]:


prim.groupby(['CYEAR','IPEDS_Flag']).size()


# In[244]:


#t= prim.query('CYEAR=="2017" & IPEDS_Flag=="Y"')
#t.to_excel("S:/Institutional Research/Chen/HR/testr2017.xlsx", sheet_name="dat")
#prim.query('CYEAR=="2017" & pers_id=="00076535"')


# In[ ]:


#############Acad#################


# In[248]:


Acad = prim[prim['emp_sgrp_cd'].isin(['AT','AS','B4','AC','A1','AW','AD','AX','B7','B8'])==False].query('emp_cat_cd_1=="F"').query('(asgn_typ_cd !=asgn_typ_cd) | (asgn_typ_cd != "4")').query('anl_sal>0')

#further exclude LTD and TempOncall
Acad = Acad[Acad['LTD'].isnull()]
Acad = Acad.query('TempOnCall=="N"')


# In[249]:


Acad.shape


# In[250]:


Acad['STATC']= np.where(Acad['status_cd'].isin(['TENR','TPRO']),'FW',
                       np.where( (Acad['derived_rank_rev'].notnull()) & (Acad['status_cd'].isnull()) , 'FT',
                                np.where( (Acad['derived_rank_rev'].notnull() & (Acad['status_cd'].str.endswith('E')))  , 'FT',
                                        np.where(Acad['status_cd'].isin(['CLIB','CPLB']),'LW',
                                           np.where(Acad['emp_cat_cd']=='FLF','LT',
                                               np.where(Acad['status_cd'].isin(['CEXT','CPEX']), 'MW',
                                                       np.where(Acad['emp_cat_cd']=='FEF','MT',
                                                               np.where(Acad['status_cd'].isin(['CNSC','CPNS']), 'NW',
                                                                       np.where(Acad['emp_cat_cd']=='FNF','NT',
                                                                               np.where(Acad['status_cd'].isin(['CPSP','CSPC']), 'SW',
                                                                                       np.where(Acad['emp_cat_cd']=='FSF','ST',
                                                                                               np.where(Acad['status_cd']=='COTR','UW','UT' ))))))))))
                              
                               ))

Acad['FULLPART']= np.where(Acad['cul']>=100,'1','2')
Acad['EMPPERC']= Acad['cul']
Acad['endt']= np.where(Acad['end_date'].astype(str).str.slice(0,4)=='9999', np.nan, Acad['end_date'].astype(str))
Acad['endt']= (pd.to_datetime(Acad['endt'],errors = 'coerce') - Acad['start_date'] ).dt.days
Acad['POSMO'] = np.where(Acad['endt'].isnull(), 12 ,
                        np.where(Acad['endt']/365*12>=12,12,
                              np.floor(Acad['endt']/365*12)  ))
#Acad['HANDICAP']= np.nan

Acad['APPTBAS']= np.where(Acad['emp_sgrp_cd'].isin(['AN','AO','AQ']),'AN',
                         np.where(Acad['emp_sgrp_cd'].isin(['AP','AR']),'AY', np.nan ))

Acad['SAL_FTE'] = (Acad['anl_sal']/Acad['cul'])*100
Acad['RANKPC']= np.where(Acad['job_ttl'].str.contains('Post'), 'XPF0',
                        np.where( (Acad['emp_cat_cd'].str.startswith('FX') ) & ( ~ Acad['status_cd'].isin(['TENR','TPRO'])  ) & (Acad['derived_rank_rev'].isnull()),'0000',
                                np.where((Acad['emp_cat_cd'].str.startswith('FM') ) & (~ Acad['status_cd'].isin(['TENR','TPRO']) ) & (Acad['derived_rank_rev'].isnull()),'0000',
                                        np.where(Acad['derived_rank_rev']==4, 'AA00',
                                                np.where( (Acad['status_cd'].isin(['TENR','TPRO'])) & (Acad['derived_rank_rev'].isnull()),'AA00',
                                                        np.where(Acad['derived_rank_rev']==3, 'BB00',
                                                                np.where(Acad['derived_rank_rev']==2,'CC00',
                                                                        np.where(Acad['derived_rank_rev']==1,'DD00',
                                                                                np.where(Acad['job_ttl'].str.contains('- Advisor'),'SSA0',
                                                                                        np.where(Acad['job_ttl'].str.contains('- Curric'),'SSC0',
                                                                                                np.where(Acad['job_ttl'].str.contains('- Outreach'),'SSP0',
                                                                                                        np.where(Acad['job_ttl'].str.contains('- Research'),'SSR0',
                                                                                                                np.where(Acad['job_ttl'].str.contains('- Teacher'),'SST0',
                                                                                                                        np.where(Acad['emp_cat_cd'].isin(['MT','MW']),'MU00',
                                                                                                                                np.where(Acad['emp_cat_cd'].isin(['FLF','FLC']),'LLD0',
                                                                                                                                        np.where(Acad['emp_cat_cd'].isin(['NT','NW']), 'NEC0',
                                                                                                                                                np.where(Acad['job_ttl'].str.contains('Coach'),'VJ00',
                                                                                                                                                np.where(Acad['job_ttl'].str.contains('Athlet'),'VJ00',
                                                                                                                                                np.where(Acad['job_ttl'].str.contains('Lectur'),'TL00',
                                                                                                                                                np.where(Acad['job_ttl'].str.contains('Intern-'),'Ul00',
                                                                                                                                                np.where(Acad['job_ttl'].str.contains('Resident-'),'UR00',
                                                                                                                                                np.where(Acad['job_ttl'].str.contains('Scholar-'),'UVSV',
                                                                                                                                                np.where(Acad['job_ttl'].str.contains('Assistant Instructor'),'TA00',
                                                                                                                                                np.where(Acad['job_ttl'].str.contains('Research Associate'),'URA0','YZ00'))))))))))))))))))))))))

Acad['TBDATE'] = np.where(Acad['status_cd'].isnull(), Acad['start_date'],
                         np.where(Acad['status_cd'].str.endswith('E'),Acad['start_date'], None ))

Acad['TEDATE'] = np.where(Acad['status_cd'].isnull(), Acad['prim_asgn_end_date'],
                         np.where(Acad['status_cd'].str.endswith('E'),Acad['prim_asgn_end_date'], None ))
Acad['vstdt']= np.where(Acad['vstd_date'].astype(str).str.slice(0,4)=='9999', None, Acad['vstd_date'])
Acad['Retire_Elig'] = np.where( (pd.to_datetime(Acad['vstdt'],errors = 'coerce') <= pd.to_datetime((Acad['CYEAR'].astype(int) +1).astype(str)+'-07-01' ) ) &                               (Acad['vstdt'].notnull()) ,1,0 )
Acad['FixTerm_Exp']= np.where((Acad['prim_asgn_end_date'] <= pd.to_datetime((Acad['CYEAR'].astype(int) +1).astype(str) +'-07-01' ) ) &                              (Acad['prim_asgn_end_date'] != pd.to_datetime('1900-01-01')),1,0)
Acad['AR1']= np.where(Acad['Age']<35,1,0)
Acad['AR2']= np.where( (Acad['Age']>=35) & (Acad['Age']<40),1,0)
Acad['AR3']= np.where( (Acad['Age']>=40) & (Acad['Age']<45),1,0)
Acad['AR4']= np.where( (Acad['Age']>=45) & (Acad['Age']<50),1,0)
Acad['AR5']= np.where( (Acad['Age']>=50) & (Acad['Age']<55),1,0)
Acad['AR6']= np.where( (Acad['Age']>=55) & (Acad['Age']<60),1,0)
Acad['AR7']= np.where( (Acad['Age']>=60) & (Acad['Age']<65),1,0)
Acad['AR8']= np.where(Acad['Age']>=65,1,0)

#code for degree

degF ={'07':'S0',
'08':'U0',
'10':'V0',
'12':'L0',
'15':'N0',
'20':'Y0',
'24':'M0',
'28':'H0',
'32':'R0',
'34':'F0',
'36':'W0',
'38':'Z0',
'44':'X0',
'46':'I0',
'50':'Q0',
'65':'J0',
'66':'B0',
'82':'C0',
'86':'A0',
'92':'K0',
'98':'D0',
'99':'P0'}



degF = pd.Series(degF)
degF.name ='DEGREE'
degF.index.name='certcd'
degF= degF.reset_index()

Acad = Acad.join(degF.set_index('certcd'), on='cert_cd')

Acad['Total']=1
Acad['Tenure_System'] = np.where(Acad['Employee_category']=='Tenure Sys',1,0)
Acad['Fixed_Term_Faculty'] = np.where(Acad['Employee_category']=='Fix Fac',1,0)
Acad['Continuing_Staff'] = np.where(Acad['Employee_category']=='Cont Staff',1,0 )
Acad['Fixed_Term_Staff'] = np.where(Acad['Employee_category']=='Fix Staff',1,0)
Acad['Ranked_Faculty']= Acad['Tenure_System'] +Acad['Fixed_Term_Faculty']
Acad['Unranked_Faculty']= np.where(Acad['Ranked_Faculty']==1,0,1)
Acad['CUC']= Acad['mau_code']+Acad['dept_code']
Acad['NCUC']= Acad['mau_code']+Acad['dept_code']+'00'
Acad['FYEAR']= (pd.to_numeric( Acad['CYEAR'])-2000).astype(str) +(pd.to_numeric( Acad['CYEAR'])-1999).astype(str)
#Acad['AYEAR']= None
Acad['TYPE']=4


# In[251]:


Acad=Acad.loc[:,['zpid','pers_id','NCUC','CUC','FYear','Sem' ,'CYEAR', 'AYEAR','ssn','NAME','TYPE','STATC','FULLPART',           'EMPPERC','POSMO','EEO_1','ETHNIC','GENDER','HANDICAP','CITIZEN','DEGREE','APPTBAS','anl_sal','SAL_FTE',           'acum_fte_svc_mths','RANKPC' ,'RANK','birth_date','cont_svc_date','vstd_date','TBDATE','TEDATE','tenure_cont_sys_entr_date',          'Age','New_Hires','Retire_Elig','FixTerm_Exp','AR1','AR2','AR3','AR4','AR5','AR6','AR7','AR8',           'ETHNIC1','ETHNIC2','ETHNIC3','ETHNIC4','ETHNIC5','Total','Tenure_System','Fixed_Term_Faculty',           'Continuing_Staff','Fixed_Term_Staff','Ranked_Faculty','Unranked_Faculty']].rename(columns={'zpid':'ZPid', 'pers_id':'Pers_ID','ssn':'EID','EEO_1':'EEO','anl_sal':'SALARY','acum_fte_svc_mths':'SERVMOS',
               'RANK':'RPNAME', 'birth_date':'BDATE','cont_svc_date':'CDATE','vstd_date':'VDATE','tenure_cont_sys_entr_date':'CEDATE'})


# In[252]:


###following is to compare with existing Acad###


# In[253]:


tst = pd.read_sql('select * from Non_Aggregated.dbo.Acad where CYEAR>=2011', OPBDB2)


# In[254]:


tst = Acad.join(tst.set_index(['Pers_ID','CYEAR']), on=['Pers_ID','CYEAR'], rsuffix='.y')


# In[255]:


#those in the new Acad output but not in existing is because the left join of the HR_PA_DATE_SPECIFICATIONS_V 
tst[tst['Total.y'].isnull()].loc[:,['Pers_ID','CYEAR']]


# In[264]:


#this is due to use job_ttl and FAS_ASGN_NM_2,FAS_ASGN_NM_3,FAS_ASGN_NM_4,FAS_ASGN_NM_5 to determine the rank vs
# fac_rnk_cd the FAC_RNK_cd2 to 5
diff= tst[ (tst['Fixed_Term_Faculty'] !=tst['Fixed_Term_Faculty.y']) & (tst['Total.y'].notnull()) ].loc[:,['Pers_ID','CYEAR','Fixed_Term_Faculty','Fixed_Term_Staff','Fixed_Term_Faculty.y','Fixed_Term_Staff.y']]
#diff
diff.to_csv("S:/Institutional Research/Chen/HR/difffx.csv")


# In[ ]:


tst[ (tst['Fixed_Term_Staff'] !=tst['Fixed_Term_Staff.y']) & (tst['Total.y'].notnull()) ].loc[:,['Pers_ID','CYEAR']]


# In[ ]:


#this is due to use of job_ttl to code rank in Acd2015
tst[ (tst['RPNAME'] !=tst['RPNAME.y']) & (tst['Total.y'].notnull()) & (tst['Ranked_Faculty']==1) ].loc[:,['Pers_ID','CYEAR','RPNAME','RPNAME.y']].head(100)


# In[ ]:


prim.query('CYEAR=="2015"').query('pers_id=="00269238"')[['pers_id','job_ttl','fac_rnk_cd','derived_rank']]


# In[ ]:


#this is due to FXE FME with rank
tst[ (tst['RANKPC'] !=tst['RANKPC.y']) & (tst['Total.y'].notnull()) & (tst['Ranked_Faculty']==1)  ].loc[:,['Pers_ID','CYEAR','RANKPC','RANKPC.y','RPNAME','Fixed_Term_Faculty']]


# In[ ]:


prim.query('CYEAR=="2015"').query('pers_id=="00005070"')[['pers_id','job_ttl','fac_rnk_cd','derived_rank','emp_cat_cd','status_cd']]


# In[ ]:


tst[ ((tst['SAL_FTE'] -tst['SAL_FTE.y']).abs()>1 ) & (tst['Total.y'].notnull()) ].loc[:,['Pers_ID','CYEAR','SAL_FTE','SAL_FTE.y']]
tst[ (tst['CITIZEN'] !=tst['CITIZEN.y']) & (tst['Total.y'].notnull()) ].loc[:,['Pers_ID','CYEAR','ETHNIC','ETHNIC.y']]
#default to 5?
tst[ (tst['ETHNIC'] !=tst['ETHNIC.y'].astype(float)) & (tst['Total.y'].notnull()) ].loc[:,['Pers_ID','CYEAR','ETHNIC','ETHNIC.y']]


# In[ ]:


#this is due to difference in derived rank, existing use
#job_ttl and FAS_ASGN_NM_2,FAS_ASGN_NM_3,FAS_ASGN_NM_4,FAS_ASGN_NM_5 to determine the rank vs
# fac_rnk_cd the FAC_RNK_cd2 to 5
tst[ (tst['STATC'] !=tst['STATC.y']) & (tst['Total.y'].notnull())  ].loc[:,['Pers_ID','CYEAR','STATC','STATC.y']]


# In[ ]:


###########Following is for getting the high level FTE #########################


# In[265]:


def costdist(hrexdt, ooiexdt, dtfilter):
    FM = pd.read_sql("select * from HR_"+hrexdt+".HR_PY_FUND_MASTER_V                    where '"+dtfilter+"' between start_date and end_date", EDW)
    CD = pd.read_sql("select * from HR_"+hrexdt+".HR_PA_COST_DISTRIBUTION_V                    where '"+dtfilter+"' between start_date and end_date and LBR_DSTR_TBL_TYP='DIST' ", EDW)
    BASEPAY = pd.read_sql("select * from HR_"+hrexdt+".HR_PA_BASE_PAY_V                    where '"+dtfilter+"' between base_pay_start_date and base_pay_end_date and SRC_STYPE = '0'", EDW)
    COSTDist = FM.loc[:,['org_cd','acct_pub_nm','fund_grp_cd','fund']].            join(CD.set_index('fund').loc[:,['pers_nbr','wt_pct','pymt_wage_typ_amt']], on='fund' , how='inner').            join(BASEPAY[['pers_nbr']].set_index('pers_nbr'), on='pers_nbr', how='inner').reset_index().            groupby(['pers_nbr','org_cd','fund_grp_cd','fund'])['wt_pct','pymt_wage_typ_amt'].sum()
    COSTDist['sum_pymt_wage_typ_amt']=COSTDist.groupby(['pers_nbr'])['pymt_wage_typ_amt'].transform(sum)   
    HROOI = ooihr(hrexdt=hrexdt, ooiexdt=ooiexdt, dtfilter=dtfilter)
    COSTDist = COSTDist.reset_index().join(HROOI.set_index('org_code').                     loc[:,['org_full_name','mau_code','mau_name','dept_code','dept_name']], on='org_cd')
    COSTDist['Weight_Percent']= COSTDist['pymt_wage_typ_amt']/COSTDist['sum_pymt_wage_typ_amt']*100
    COSTDist['FUND_TYP'] = np.where(COSTDist['fund'].str.startswith('MSG'),'1',
                           np.where(COSTDist['fund'].str.startswith('MSR'),'2',
                                   np.where( (COSTDist['fund'].str.startswith('MSD') ) & (COSTDist['fund'].str.startswith('MSDC')== False),'3',
                                           np.where(COSTDist['fund'].str.startswith('MSX'),'4',
                                                   np.where(COSTDist['fund'].str.startswith('MSA'),'9',
                                                           np.where(COSTDist['fund'].str.startswith('MSL'),'5','5'))))))
    COSTDist['CYEAR'] = hrexdt[:4]
    
    return COSTDist
    #COSTDist = COSTDist.reset_index().join(prim.set_index('pers_nbr').loc[:,['pers_id']], on='pers_nbr', how='inner')


# In[266]:


cost11 = costdist(hrexdt='20111025', ooiexdt='201110', dtfilter='01-OCT-11')

cost12 = costdist(hrexdt='201210', ooiexdt='201210', dtfilter='01-OCT-12')

cost13 = costdist(hrexdt='20131018', ooiexdt='20131018', dtfilter='01-OCT-13')
cost14 = costdist(hrexdt='20141025', ooiexdt='20141025', dtfilter='01-OCT-14')

cost15 = costdist(hrexdt='20151027', ooiexdt='20151027', dtfilter='01-OCT-15')

cost16 = costdist(hrexdt='20161019', ooiexdt='20161019', dtfilter='01-OCT-16')
cost17 = costdist(hrexdt='20171026', ooiexdt='20171026', dtfilter='01-OCT-17')


# In[267]:


cost= pd.concat([cost11,cost12,cost13,cost14,cost15,cost16,cost17])
cost = cost.join(prim.set_index(['pers_nbr','CYEAR' ]).                 loc[:,['pers_id','Employee_category','org_cd','mau_code','dept_code','capc_util_lvl','anl_sal']], 
                 on=['pers_nbr','CYEAR' ], lsuffix='_pay', how='inner').query('anl_sal>0')


# In[268]:


cost['FTE_Funding_Derived']= cost['wt_pct'] * cost['capc_util_lvl']/10000
#when Academic staff GFFTE is FTE when fund group =GF
cost['GFFTE']= np.where(cost['fund_grp_cd']=='GF', cost['FTE_Funding_Derived'],0)
#when Non Acad, GFFTE needs adjustment
cost['GFFTE']= np.where(cost['Employee_category'] != 'Non Acad', cost['GFFTE'],
                         np.where( (cost['fund']=='MSXC023271') & (cost['org_cd']=='10070386'), cost['FTE_Funding_Derived']*0.5,
                             np.where(cost['fund']=='MSXC023271', cost['FTE_Funding_Derived']*0.7, cost['GFFTE'])))

cost['egrp']= np.where(cost['Employee_category']=='Grad Asst','ga',
                        np.where(cost['Employee_category']== 'Tenure Sys','ts',
                                np.where(cost['Employee_category']=='Fix Fac','or',
                                        np.where(cost['Employee_category']=='Fix Staff','fts',
                                                np.where(cost['Employee_category']=='Cont Staff','cs','fy')))))


# In[269]:


##### generating output for high level FTE################
EmpFTEagg = cost.groupby(['CYEAR', 'pers_id','pers_nbr','mau_code_pay','dept_code_pay','egrp',                           'Employee_category','FTE_Funding_Derived','fund_grp_cd'])['FTE_Funding_Derived','GFFTE'].sum()
EmpFTEagg.columns = ['affte','gffte']
EmpFTEagg['ngfte']= EmpFTEagg['affte']- EmpFTEagg['gffte']
EmpFTEagg= EmpFTEagg.reset_index().melt(id_vars=['CYEAR','pers_id','pers_nbr','mau_code_pay','dept_code_pay','egrp','Employee_category','FTE_Funding_Derived','fund_grp_cd'], var_name='type', value_name='fte')
EmpFTEagg['vargrp']= EmpFTEagg['egrp']+EmpFTEagg['type']
EmpFTEagg = EmpFTEagg.pivot_table(index=['CYEAR','pers_id','pers_nbr','mau_code_pay','dept_code_pay',                                         'Employee_category','FTE_Funding_Derived','fund_grp_cd'], columns='vargrp', fill_value=0 )
EmpFTEagg.columns = EmpFTEagg.columns.droplevel()
EmpFTEagg['onraffte']= EmpFTEagg['csaffte'] + EmpFTEagg['ftsaffte']
EmpFTEagg['onrgffte']= EmpFTEagg['csgffte'] + EmpFTEagg['ftsgffte']
EmpFTEagg['onrngffte']= EmpFTEagg['csngfte'] + EmpFTEagg['ftsngfte']
EmpFTEagg = EmpFTEagg.reset_index()
EmpFTEagg['CUC'] = EmpFTEagg['mau_code_pay'] +  EmpFTEagg['dept_code_pay']
EmpFTEagg['NCUC'] = EmpFTEagg['CUC']+'00'
EmpFTEagg['Current_Assignment']='Y'
EmpFTEagg = EmpFTEagg.drop(['mau_code_pay','dept_code_pay'], axis=1).rename(columns={'fund_grp_cd':'Fund_Group', 'pers_id':'Person_ID','pers_nbr':'Personnel_Number','CYEAR':'CYear'})


# In[ ]:


EmpFTEagg.groupby('CYear')['gaaffte','gagffte', 'tsaffte','tsgffte',                 'oraffte','orgffte','onraffte','onrgffte','csaffte','csgffte','ftsaffte','ftsgffte','fyaffte','fygffte','fyngfte'].sum()


# In[ ]:


#compare with existing ds
pd.read_sql('select CYear , sum([gaaffte]) as gaaffte, sum([gagffte]) as gagffte,sum([tsaffte]) as tsaffte,sum([tsgffte]) as tsgffte ,sum([oraffte]) as oraffte, sum([orgffte]) as orgffte,sum([onraffte]) as onraffte,sum([onrgffte]) as onrgffte, sum([csaffte]) as csaffte ,sum([csgffte]) as csgffte  ,sum([ftsaffte]) as ftsaffte,sum([ftsgffte]) asftsgffte    FROM CUC_Aggregates.dbo.HR_HighLevel_FTE_by_Fund_cuc group by CYear order by CYear ', OPBDB2)


# In[ ]:


pd.read_sql('SELECT CYear,sum([fyaffte]) as fyaffte,sum([fygffte]) as fygffte  ,sum([fyngfte]) as fyngfte   FROM CUC_Aggregates.dbo.HR_nacad_FTE_by_Fund_cuc group by CYear order by CYear', OPBDB2)


# In[ ]:


########################################################
#########following to get NonAcad FTE details################


# In[270]:


NAcadFTE = cost.join(prim.set_index(['pers_id','pers_nbr','CYEAR']).                       loc[:,['emp_cat_cd','emp_cat_cd_1','zpid','ssn','Name','job_ttl','pay_scale_lvl_cd','cul','EEO_1',                             'ethnc_cd','ETHNIC','ETHNIC1','ETHNIC2','ETHNIC3','ETHNIC4','ETHNIC5','GENDER','CITIZEN']],                        on=['pers_id','pers_nbr','CYEAR'] ).query('emp_cat_cd_1=="S"').query('anl_sal>0')


# In[271]:


NAcadFTE['Employee_Group'] = NAcadFTE['emp_cat_cd'].str.slice(1,2)
NAcadFTE['OCCupation_CODE'] = '0'
NAcadFTE['FTE_Total'] = NAcadFTE['FTE_Funding_Derived']

NAcadFTE['FTE_GF'] = np.where(NAcadFTE['fund'].str.startswith('MSG'),NAcadFTE['FTE_Total'],
                             np.where((NAcadFTE['fund']=='MSXC023271') & (NAcadFTE['org_cd'] != '10070386') , NAcadFTE['FTE_Total']*0.7,
                                     np.where((NAcadFTE['fund']=='MSXC023271') & (NAcadFTE['org_cd'] == '10070386'), NAcadFTE['FTE_Total']*0.5,0)))


NAcadFTE['FTE_ERF']= np.where(NAcadFTE['fund'].str.startswith('MSR'), NAcadFTE['FTE_Total'],0)
NAcadFTE['FTE_DES']= np.where( (NAcadFTE['fund'].str.startswith('MSD') ) & (NAcadFTE['fund'].str.startswith('MSDC')== False), NAcadFTE['FTE_Total'],0)

NAcadFTE['FTE_AUX'] = np.where(NAcadFTE['fund'].str.startswith('MSX')== False,0,
                             np.where((NAcadFTE['fund']=='MSXC023271') & (NAcadFTE['org_cd'] != '10070386') , NAcadFTE['FTE_Total']*0.3,
                                     np.where((NAcadFTE['fund']=='MSXC023271') & (NAcadFTE['org_cd'] == '10070386'), NAcadFTE['FTE_Total']*0.5,NAcadFTE['FTE_Total'])))

NAcadFTE['Person_FTE']= NAcadFTE['cul']/100
NAcadFTE['FTE_OTH']= np.where(NAcadFTE['fund'].str.startswith('MSP'),NAcadFTE['FTE_Total'],
                             np.where(NAcadFTE['fund'].str.startswith('MSL'),NAcadFTE['FTE_Total'],0 ))

NAcadFTE['FULLPART']= np.where(NAcadFTE['cul']>=90,'1','2')
NAcadFTE['TIMe_code']=NAcadFTE['FULLPART']+'2'
NAcadFTE['CUC']= NAcadFTE['mau_code_pay']+NAcadFTE['dept_code_pay']
NAcadFTE['NCUC']= NAcadFTE['mau_code_pay']+NAcadFTE['dept_code_pay']+'00'
NAcadFTE['FYear']= (pd.to_numeric( NAcadFTE['CYEAR'])-2000).astype(str) +(pd.to_numeric( NAcadFTE['CYEAR'])-1999).astype(str)
NAcadFTE['Function'] = 4


# In[272]:


NAcadFTE=NAcadFTE.rename(columns={'zpid':'ZPid','pers_id':'Pers_ID','ssn':'EID',                         'Name':'NAME','job_ttl':'TITLE','pay_scale_lvl_cd':'LEVEL_code','EEO_1':'EEO_code',                        'ETHNIC':'ETHNIC_code','ETHNIC1':'ETHNIC_code1','ETHNIC2':'ETHNIC_code2','ETHNI3':'ETHNIC_code3',                         'ETHNIC4':'ETHNIC_code4','ETHNIC5':'ETHNIC_code5','GENDER':'GNDR_flag','CITIZEN':'CTZN_flag',                        'fund':'Account','FUND_TYP':'FUND'}).loc[:,[ 'NCUC' ,'CUC','FYear','Sem','CYEAR','AYear','Employee_Group','OCCupation_CODE','FTE_Total'      ,'FTE_GF','FTE_ERF','FTE_DES','FTE_AUX','FTE_OTH','ZPid','Pers_ID' ,'EID','NAME','TITLE','LEVEL_code' ,'FULLPART'      ,'TIMe_code' ,'EEO_code','ETHNIC_code','ETHNIC_code1','ETHNIC_code2','ETHNIC_code3','ETHNIC_code4','ETHNIC_code5'      ,'GNDR_flag','CTZN_flag','Person_FTE','Account' ,'FUND' ,'Category','TYPE','Type_Source','Function']]


# In[ ]:


#from IPython.display import display

#pd.options.display.max_columns = None
#display(NAcadFTE.query('CYEAR=="2012"').query('Pers_ID=="00018161"'))


# In[ ]:


#compare with existing##
ext = pd.read_sql('SELECT  NCUC, CUC, FYear, Sem , CYEAR , AYear, Employee_Group, OCCupation_CODE      , ZPid, Pers_ID, EID, NAME, TITLE, LEVEL_code, FULLPART, TIMe_code, EEO_code      , ETHNIC_code, ETHNIC_code1, ETHNIC_code2, ETHNIC_code3, ETHNIC_code4, ETHNIC_code5      , GNDR_flag, CTZN_flag , Person_FTE , Account, FUND , Category , TYPE , Type_Source      ,sum( FTE_Total) as FTE_Total, sum(  FTE_GF) as FTE_GF, sum( FTE_ERF) as FTE_ERF ,   sum( FTE_DES) as FTE_DES, sum( FTE_AUX) as FTE_AUX, sum( FTE_OTH) as FTE_OTH  FROM  Non_Aggregated. dbo. NonAcad_FTE  where CYEAR>=2011  group by  NCUC, CUC, FYear, Sem , CYEAR , AYear, Employee_Group, OCCupation_CODE      , ZPid, Pers_ID, EID, NAME, TITLE, LEVEL_code, FULLPART, TIMe_code, EEO_code      , ETHNIC_code, ETHNIC_code1, ETHNIC_code2, ETHNIC_code3, ETHNIC_code4, ETHNIC_code5      , GNDR_flag, CTZN_flag , Person_FTE , Account, FUND , Category , TYPE , Type_Source ', OPBDB2)


# In[ ]:


ext = NAcadFTE.join(ext.set_index(['Pers_ID','CYEAR','CUC','Account']), on=['Pers_ID','CYEAR','CUC','Account'], rsuffix='.y', how='outer' )


# In[ ]:


#due to org_cd =10010117 do not have HR structure under ooi_ooi_org_struct_cd_lvl_rv instead I used 
#OOI_OOI_ORG_STRUCT_CODE_AV
ext[(ext['NCUC'].isnull()) | (ext['NCUC.y'].isnull())][['CUC','NCUC','NCUC.y','CYEAR']].drop_duplicates()


# In[ ]:


#persid of 00023830 with CUC57513 did not in existing because of pos_nbr=999999
ext[(ext['NCUC'].isnull()) | (ext['NCUC.y'].isnull())].query('CUC=="57513"')[['Pers_ID','CUC','NCUC','NCUC.y','CYEAR']]
prim.query('CYEAR=="2013"').query('pers_id=="00023830"').loc[:,['CYEAR','pers_id','pos_nbr']]


# In[ ]:


ext.query('Pers_ID!="00023830"').groupby('CYEAR')['FTE_Total','FTE_Total.y','FTE_GF','FTE_GF.y','FTE_ERF','FTE_ERF.y',                   'FTE_DES','FTE_DES.y','FTE_AUX','FTE_AUX.y','FTE_OTH','FTE_OTH.y'].sum()


# In[ ]:


t=ext.query('Pers_ID!="00023830"').query('(CYEAR != "2011") | (CUC not in ("47117","10117"))')
t[t['FUND'] != t['FUND.y']].loc[:,['Pers_ID','CYEAR','FUND','FUND.y']].head()
t[t['ETHNIC_code'] != t['ETHNIC_code.y'].astype(float)].loc[:,['Pers_ID','CYEAR','ETHNIC_code','ETHNIC_code.y']].head()


# In[ ]:


#what ethc_cd is null default?
# what happed to 2017 racial coding
prim.query('pers_id=="00044135"').query('CYEAR=="2011"')[['pers_id','CYEAR','ethnc_cd','ETHNIC','racial_cat_cd_1']]


# In[ ]:


prim.query('pers_id=="00015129"').query('CYEAR=="2017"')[['pers_id','CYEAR','ethnc_cd','ETHNIC','racial_cat_cd_1']]


# In[ ]:


#problem with 'org_code of 10010117' in 2011
prim.query('dept_code=="117"')[['org_cd','mau_code','dept_code','CYEAR']].drop_duplicates()


# In[ ]:


#Pushing outputs to OPBDB2 Chen_Test#########


# In[273]:


#function to convert to datatype in sql
import sqlalchemy
def sqlcol(dfparam):    

    dtypedict = {}
    for i,j in zip(dfparam.columns, dfparam.dtypes):
        if "object" in str(j):
            dtypedict.update({i: sqlalchemy.types.NVARCHAR(length=255)})
        
        if "datetime64[ns]" in str(j):
            dtypedict.update({i: sqlalchemy.types.DateTime()})

        if "float" in str(j):
            dtypedict.update({i: sqlalchemy.types.Float(precision=1, asdecimal=True)})

        if "int" in str(j):
            dtypedict.update({i: sqlalchemy.types.INT()})

    return dtypedict


# In[333]:


from sqlalchemy import create_engine, event
from urllib.parse import quote_plus


conn =  "Driver={SQL Server};Server=OPBDB2;Database=Chen_Test;Trusted_Connection=yes;"
quoted = quote_plus(conn)
new_con = 'mssql+pyodbc:///?odbc_connect={}'.format(quoted)
engine = create_engine(new_con)

@event.listens_for(engine, 'before_cursor_execute')
def receive_before_cursor_execute(conn, cursor, statement, params, context, executemany):
    print("FUNC call")
    if executemany:
        cursor.fast_executemany = True



#push prim
prim.to_sql('HR_Person_Position_Job', engine, if_exists = 'replace', chunksize = 500,index=False, dtype=sqlcol(prim) )


# In[275]:


#push cost
cost.to_sql('HR_Person_Cost_Fund_Dist', engine, if_exists = 'replace', chunksize = 1000,index=False, dtype=sqlcol(cost) )


# In[276]:


#push Acad
from pandas.io import sql
from sqlalchemy.types import NVARCHAR

conn_str = r'mssql+pymssql://OPBDB2/Chen_Test'
engine = create_engine(conn_str)

Acad.to_sql('Non_Aggregated_Acad', engine, if_exists = 'replace', chunksize = 1000,index=False, dtype=sqlcol(Acad) )


# In[277]:


##########separte EmpFTEaggg as 2 outputs to fit into current structure###############

AcadHL = EmpFTEagg.query('Employee_category != "Non Acad" ').loc[:,['CUC' ,'Fund_Group'  ,'Personnel_Number' ,'Person_ID','CYear','NCUC','FTE_Funding_Derived','Current_Assignment'      ,'Employee_category' ,'gaaffte'  ,'gagffte','tsaffte' ,'tsgffte' ,'oraffte' ,'orgffte'  ,'onraffte'  ,'onrgffte'  ,'csaffte'      ,'csgffte','ftsaffte'  ,'ftsgffte']]

NonAcadHL = EmpFTEagg.query('Employee_category == "Non Acad" ').loc[:,['Personnel_Number' ,'Person_ID' ,'CYear','CUC' ,'Fund_Group','NCUC' ,'FTE_Funding_Derived','Current_Assignment'      ,'Employee_category' ,'fyaffte' ,'fygffte' ,'fyngfte']]


AcadHL.to_sql('HR_HighLevel_FTE_by_Fund_cuc', engine, if_exists = 'replace', chunksize = 1500,index=False, dtype=sqlcol(AcadHL) )


NonAcadHL.to_sql('HR_nacad_FTE_by_Fund_cuc', engine, if_exists = 'replace', chunksize = 1500,index=False, dtype=sqlcol(NonAcadHL) )


# In[ ]:


olist = NAcadFTE.select_dtypes(include='object').columns.tolist()
for i in olist:
    NAcadFTE[i]= NAcadFTE[i].astype('category')


# In[ ]:


NAcadFTE.to_sql('Non_Aggregated_NonAcad_FTE', engine, if_exists = 'replace', chunksize = 200,index=False, dtype=sqlcol(NAcadFTE) )


# In[ ]:


NAcadFTE.groupby('CYEAR')['FTE_Total','FTE_GF','FTE_ERF',                   'FTE_DES','FTE_AUX','FTE_OTH'].sum()


# In[ ]:


NAcadFTE.shape

