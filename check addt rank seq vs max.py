# -*- coding: utf-8 -*-
"""
Created on Tue Feb  5 15:03:02 2019
check differences between coding with max rank across both primary and addtional rank
vs. use primary first and addtional sequentially
@author: chendi4
"""

import numpy as np
import pandas as pd
import connectdb as ct
import re

def adtrank(hrexdt, ooiexdt, dtfilter):
    PRIM =  pd.read_sql("select * from HR_"+hrexdt+".HR_PA_EMP_PRIMARY_ASSIGNMENT_V \
                   where '"+dtfilter+"' between start_date and end_date", ct.EDW)
    JOB = pd.read_sql("select * from HR_"+hrexdt+".HR_OM_JOB_V \
                   where '"+dtfilter+"' between start_date and end_date", ct.EDW)
    ACTION = pd.read_sql("select * from HR_"+hrexdt+".HR_PA_ACTIONS_V \
                   where '"+dtfilter+"' between start_date and end_date and EMP_STATUS_CD in ('1','3') ", ct.EDW)
    ActivePerson = ACTION[['pers_nbr','emp_status_cd','emp_status_nm']].drop_duplicates()
    ActivePerson['Active']='Y'
    
    PRIM= PRIM.drop(['curr_ind', 'src_last_updated_by', 'src_last_update_date', 'created_by',\
                   'create_date', 'last_updated_by', 'last_update_date'], axis=1).\
      join(JOB.set_index('job_cd').loc[:,['job_ttl','eeo_cd','eeo_nm','emp_cat_cd','emp_cat_nm','fac_rnk_cd']], on='job_cd', how='inner').\
      join(ActivePerson.set_index('pers_nbr'), on='pers_nbr')
    
    
    PRIM = PRIM[ PRIM['emp_cat_cd'].str.slice(0,1).isin(['F','S','G'])].query('emp_grp_cd != "5"').query('Active=="Y"')
    PRIM['cutdt']= np.datetime64(pd.to_datetime(dtfilter))
    #since 2013, prim_asgn_end_date datatype changed "datetime64[ns]"
    #pers_id=="00140406" in 2015 frozn have prim_asgn_end_date of 9999-12-31
    if PRIM['prim_asgn_end_date'].dtype == 'object':
        PRIM['prim_asgn_end_date'] = np.where(PRIM['prim_asgn_end_date'].astype(str).str.slice(0,4)=='9999', None, \
                          PRIM['prim_asgn_end_date'].astype(str) )
    PRIM['prim_asgn_end_date']= pd.to_datetime(PRIM['prim_asgn_end_date'])
    PRIM = PRIM[(PRIM['prim_asgn_end_date'] == np.datetime64('1900-01-01')) | (PRIM['prim_asgn_end_date'].isnull()) | ( PRIM['prim_asgn_end_date'] >=  PRIM['cutdt'])]
    #first character of emp_cat_cd
    if PRIM['fac_rnk_cd'].isnull().sum() < len(PRIM):
        PRIM['fac_rnk_cd'] = np.where(PRIM['fac_rnk_cd'].isnull(), np.nan,  PRIM['fac_rnk_cd'].str.slice(3,4).astype(float))
        
    else:
        PRIM['fac_rnk_cd']= np.where(PRIM['job_ttl'].str.contains('professorial',case=False), np.nan,
                                np.where(PRIM['job_ttl'].str.contains('ASSISTANT INSTRUCTOR', case=False),  np.nan,
                                   np.where( PRIM['job_ttl'].str.contains('INSTRUCTOR' , case=False), 1,
                                       np.where(PRIM['job_ttl'].str.contains('ASSISTANT PROFESSOR',case=False),2,
                                           np.where(PRIM['job_ttl'].str.contains('ASSOCIATE PROFESSOR',case=False),3,
                                                   np.where(PRIM['job_ttl'].str.contains('PROF,',case=False),4, 
                                                           np.where(PRIM['job_ttl'].str.upper().str.endswith('PROF'),4,
                                                                   np.where(PRIM['job_ttl'].str.contains('PROFESSOR',case=False),4,
                                                                           np.where(PRIM['job_ttl'].str.contains('PROF-',case=False),4,np.nan)))))))))
        
    ASGN = pd.read_sql("select * from HR_"+hrexdt+".hr_pa_fas_assignment_v \
                   where '"+dtfilter+"' between start_date and end_date", ct.EDW)
    #transpose the columns to rows
    asgncol  =[x for x in ASGN.columns if not re.match(r'prim_asgn_also_rpt_to_\d',x) and not re.match(r'\w+_org_cd_\d+',x) ]
    asgndf = ASGN.filter(items=asgncol).set_index(['pers_nbr','start_date','end_date']).\
            filter(regex= r'\w+_\d')
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
                                  np.where( asgndfL['DerivedRnk'].isnull()==False, asgndfL['DerivedRnk'] , np.nan
                                          ))
    asgndfL= asgndfL[asgndfL['DerivedRnk_BC'].notnull()]
    prim= PRIM[['pers_nbr','fac_rnk_cd']].join(asgndfL.set_index('pers_nbr'), on='pers_nbr', how='inner', rsuffix='.y')
    prim['minseq']= prim.groupby('pers_nbr')['seq'].transform(min)
    prim['higestaddt']=prim.groupby('pers_nbr')['DerivedRnk_BC'].transform(max)
    prim['CYEAR'] = hrexdt[:4]
    prim = prim.query('minseq==seq')
    return prim[['pers_nbr','CYEAR','DerivedRnk_BC','higestaddt','fac_rnk_cd']]

t17= adtrank(hrexdt='20171026', ooiexdt='20171026', dtfilter='01-OCT-17')
t16= adtrank(hrexdt='20161019', ooiexdt='20161019', dtfilter='01-OCT-16')
t15= adtrank(hrexdt='20151027', ooiexdt='20151027', dtfilter='01-OCT-15')
t14= adtrank(hrexdt='20141025', ooiexdt='20141025', dtfilter='01-OCT-14')
t13= adtrank(hrexdt='20131018', ooiexdt='20131018', dtfilter='01-OCT-13')
t12= adtrank(hrexdt='201210', ooiexdt='201210', dtfilter='01-OCT-12')
t11= adtrank(hrexdt='20111025', ooiexdt='201110', dtfilter='01-OCT-11')
t18= adtrank(hrexdt='20181026', ooiexdt='20181026', dtfilter='01-OCT-18')
t= pd.concat([t11,t12,t13,t14,t15,t16,t17,t18])
t['CYEAR'].value_counts()
t['rnkdi']= t[['higestaddt','fac_rnk_cd']].apply(np.nanmax, axis=1)
t['old']=np.where(t['fac_rnk_cd'].notnull(), t['fac_rnk_cd'], t['DerivedRnk_BC'])
t.head()
diff = t.query('DerivedRnk_BC != higestaddt')
diff.shape
a=t.query('old != rnkdi')
a.columns=['pers_nbr','Year','additional_rnk_lowest_seq','highest_addtional_rnk','primary_rnk','rnk_use_DC','rnk_Jaime']
a
