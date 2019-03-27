"""
Created on 03/22/19
By Val requested on 3/21/19 HR_PPS_Data table
under prim, release all filters
@author: chendi4
"""



import numpy as np
import pandas as pd
import connectdb as ct
import re

def primds(hrexdt, ooiexdt, dtfilter):
    PRIM =  pd.read_sql("select * from HR_"+hrexdt+".HR_PA_EMP_PRIMARY_ASSIGNMENT_V \
                   where '"+dtfilter+"' between start_date and end_date", ct.EDW)
    PSV = pd.read_sql("select * from HR_"+hrexdt+".HR_PA_PERSON_SV \
                   where '"+dtfilter+"' between start_date and end_date", ct.EDW)
    POS = pd.read_sql("select * from HR_"+hrexdt+".HR_OM_POSITIONS_V \
                   where '"+dtfilter+"' between start_date and end_date", ct.EDW)
    JOB = pd.read_sql("select * from HR_"+hrexdt+".HR_OM_JOB_V \
                   where '"+dtfilter+"' between start_date and end_date", ct.EDW)
    ACTION = pd.read_sql("select * from HR_"+hrexdt+".HR_PA_ACTIONS_V \
                   where '"+dtfilter+"' between start_date and end_date and EMP_STATUS_CD in ('1','3') ", ct.EDW)

    CX = pd.read_sql("select * from HR_"+hrexdt+".HR_PA_PERNR_CROSSWALK_V ", ct.EDW)
    DSPEC = pd.read_sql("select * from HR_"+hrexdt+".HR_PA_DATE_SPECIFICATIONS_SV \
                   where '"+dtfilter+"' between start_date and end_date", ct.EDW)

    PDETAIL = pd.read_sql("select * from HR_"+hrexdt+".HR_PA_PERSON_DETAIL_SV \
                   where '"+dtfilter+"' between start_date and end_date", ct.EDW)
    RES = pd.read_sql("select * from HR_"+hrexdt+".HR_PA_RESIDENCE_STATUS_SV \
                   where '"+dtfilter+"' between start_date and end_date", ct.EDW)

    BN = pd.read_sql("select * from HR_"+hrexdt+".HR_BN_BNFT_SVC_INFO_V \
                   where '"+dtfilter+"' between start_date and end_date", ct.EDW)
    ED = pd.read_sql("select * from HR_"+hrexdt+".HR_PA_PERSON_EDUCATION_V", ct.EDW)

    BASEPAY = pd.read_sql("select * from HR_"+hrexdt+".HR_PA_BASE_PAY_V \
                   where '"+dtfilter+"' between base_pay_start_date and base_pay_end_date and SRC_STYPE = '0'", ct.EDW)

    FASDETAIL = pd.read_sql("select * from HR_"+hrexdt+".HR_PA_FAS_DETAILS_V \
                   where '"+dtfilter+"' between start_date and end_date", ct.EDW)
    
    Newhire = pd.read_sql("select * from HR_"+hrexdt+".HR_PA_ACTIONS_V \
                   where ACTN_TYP_CD='ZA' and   EMP_STATUS_CD in ('1','3') and "+\
                     "start_date between '01-JULY-"+dtfilter[7:]+"' and '30-SEP-"+dtfilter[7:]+"'"  , ct.EDW)
    
    LTD = pd.read_sql("select distinct pers_nbr, max(start_date) as max_actn_dt from HR_"+hrexdt+".HR_PA_ACTIONS_V \
                   where ACTN_TYP_CD='ZQ' and   EMP_STATUS_CD in ('1') and cust_status_cd in (6,8) group by pers_nbr having "+\
                     "max(start_date) <= '01-OCT-"+dtfilter[7:]+"'" , ct.EDW)
    LTD['LTD']='Y'

    var = [x for x in PDETAIL.columns if re.match(r'racial_cat_cd_\d+',x) or x=='ethnc_cd']
    PDETAIL= PDETAIL.set_index('pers_id').filter(items=var)

    ActivePerson = ACTION[['pers_nbr','emp_status_cd','emp_status_nm']].drop_duplicates()
    ActivePerson['Active']='Y'

    EDPrsn = ED.groupby('pers_id')['cert_cd'].max().reset_index()
    Newhire = Newhire[['pers_nbr']].drop_duplicates()
    Newhire['NewHire']='Y'
#EDPrsn.head()

    return PRIM.drop(['curr_ind', 'src_last_updated_by', 'src_last_update_date', 'created_by',\
                   'create_date', 'last_updated_by', 'last_update_date'], axis=1).\
       join(CX.set_index('pers_nbr').loc[:,['pers_id']], on='pers_nbr', how='inner').\
       join(JOB.set_index('job_cd').loc[:,['job_ttl','eeo_cd','eeo_nm','emp_cat_cd','emp_cat_nm','fac_rnk_cd']], on='job_cd', how='inner').\
        join(PSV.set_index('pers_id').loc[:,['ssn','uuid','fst_nm','mid_nm', 'lst_nm','birth_date', 'gndr_cd', 'gndr_nm','zpid']], on='pers_id').\
        join(PDETAIL, on='pers_id').\
        join(RES.set_index('pers_id').loc[:,['res_status_cd','res_status_nm']] , on='pers_id').\
        join(DSPEC.set_index('pers_nbr').loc[:,['cont_svc_date','time_in_lvl_date']], on='pers_nbr').\
        join(ActivePerson.set_index('pers_nbr'), on='pers_nbr').\
        join(BN.set_index('pers_nbr').loc[:,['acum_fte_svc_mths','vstd_date']], on='pers_nbr').\
        join(EDPrsn.set_index('pers_id'), on='pers_id').\
        join(BASEPAY.set_index('pers_nbr').loc[:,['pay_scale_typ_cd','pay_scale_area_cd','wage_typ_cd_2','pymt_wage_typ_amt_2',\
                                                  'pay_scale_grp_cd','pay_scale_lvl_cd','capc_util_lvl', 'anl_sal','hrly_rate']], on='pers_nbr', how='inner').\
        join(POS.set_index('pos_nbr').loc[:,['pos_ttl']], on='pos_nbr').\
        join(FASDETAIL.set_index('pers_id').loc[:,['status_cd', 'status_nm','tenure_cont_grant_dept','tenure_cont_status_grant_date',\
                                                   'tenure_exmp_agr_date','tenure_cont_sys_entr_date','promo_to_cur_rnk_entr_date']], on='pers_id').\
        join(Newhire.set_index('pers_nbr'), on='pers_nbr').\
        join(LTD.set_index('pers_nbr'), on='pers_nbr')
        
        
##########function get addtional ranks from addtional assignment#########
def adtrank(hrexdt, ooiexdt, dtfilter):
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
                                  np.where( asgndfL['DerivedRnk'].isnull()==False, asgndfL['DerivedRnk'] ,
                                          np.where(asgndfL['fas_asgn_nm'].str.contains('LECTURER'), 0, np.nan)))
    return asgndfL


def ooihr(hrexdt, ooiexdt, dtfilter):
    #get HR org hirachy from ooi_ooi_org_struct_cd_lvl_rv
    OOIV = pd.read_sql('select * from OOI_'+ooiexdt+'.ooi_ooi_org_struct_cd_lvl_rv ', ct.EDW)
    OOIHR = OOIV.query('structure_type=="HR"').loc[:,['org_code','org_full_name', 'org_type', 'mau_code','mau_name','dept_code','dept_name']]
    OOIHR['type']=1
    #get HR org hirachy from OOI_OOI_ORG_STRUCT_CODE_AV
    ORG_V = pd.read_sql('select * from OOI_'+ooiexdt+'.OOI_OOI_ORG_V', ct.EDW)
    STRUCT = pd.read_sql('select * from OOI_'+ooiexdt+'.OOI_OOI_ORG_STRUCT_CODE_AV', ct.EDW)
    STRUCTHR = STRUCT.query('struct_type=="HR"').\
    join(ORG_V.query('org_type=="MAU"').set_index('org_ref_code').loc[:,['org_full_name']], rsuffix='_mau', on='mau').\
    join(ORG_V.query('org_type=="DP"').set_index('org_ref_code').loc[:,['org_full_name']], rsuffix='_dept', on='dept').\
    loc[:,['org_code','org_full_name','org_type','mau','org_full_name_mau','dept','org_full_name_dept']]
    STRUCTHR['type']=2
    #put 2 sources together and use ooi_ooi_org_struct_cd_lvl_rv answer first if mutliple
    STRUCTHR.columns = OOIHR.columns
    HROOI = pd.concat([OOIHR,STRUCTHR  ])
    HROOI['r']=HROOI.groupby('org_code')['type'].transform(min)
    HROOI = HROOI.query('r==type').drop(['type','r'], axis=1)
    return HROOI


##function to do the variable derivation 
def primpop(hrexdt, ooiexdt, dtfilter):
    #call primary function to pull base table containing primary position job pay info 
    prim = primds(hrexdt=hrexdt, ooiexdt=ooiexdt, dtfilter=dtfilter)
    #filter include only academics, support staff and Grad assistant
    #remove non-employee/no-pay
    #remove non-active
    #remove PRIM_ASGN_END_DATE before frozen date
    #prim = prim.query('emp_grp_cd != "5"').query('Active=="Y"')
    prim['cutdt']= np.datetime64(pd.to_datetime(dtfilter))
    #since 2013, prim_asgn_end_date datatype changed "datetime64[ns]"
    #pers_id=="00140406" in 2015 frozn have prim_asgn_end_date of 9999-12-31
    if prim['prim_asgn_end_date'].dtype == 'object':
        prim['prim_asgn_end_date'] = np.where(prim['prim_asgn_end_date'].astype(str).str.slice(0,4)=='9999', None, \
                          prim['prim_asgn_end_date'].astype(str) )
    prim['prim_asgn_end_date']= pd.to_datetime(prim['prim_asgn_end_date'])
    prim['primasgndtfilter'] = np.where( (prim['prim_asgn_end_date'] == np.datetime64('1900-01-01')) | (prim['prim_asgn_end_date'].isnull()) | ( prim['prim_asgn_end_date'] >=  prim['cutdt']),'Y','N')
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
    CUL= prim[['pers_id','pers_nbr','emp_cat_cd','capc_util_lvl','asgn_typ_cd','emp_sgrp_cd','emp_cat_cd_1']].\
        dropna(subset=['pers_id', 'capc_util_lvl','emp_cat_cd'])


    CULag = CUL.groupby(['pers_id','emp_cat_cd_1'])['capc_util_lvl'].sum().reset_index().\
        rename(columns={'capc_util_lvl':'cul'})
        
    prim = prim.join(CULag.set_index(['pers_id','emp_cat_cd_1']), on=['pers_id','emp_cat_cd_1'] )
    
    #add additional ranks, choose the lowest sequence
    arnk = adtrank(hrexdt=hrexdt, ooiexdt=ooiexdt, dtfilter=dtfilter)
    arnk['minseq']= arnk.groupby('pers_nbr')['seq'].transform(min)
    arnk = arnk.query('minseq==seq').loc[:,['pers_nbr','DerivedRnk','DerivedRnk_BC']]
    #DerRank = adtrank(hrexdt=hrexdt, ooiexdt=ooiexdt, dtfilter=dtfilter).groupby('pers_nbr')['DerivedRnk'].max().reset_index()
    #DerRankBC = adtrank(hrexdt=hrexdt, ooiexdt=ooiexdt, dtfilter=dtfilter).groupby('pers_nbr')['DerivedRnk_BC'].max().reset_index()
    #DerRank= DerRank.join(DerRankBC.set_index('pers_nbr'), on='pers_nbr')
    prim = prim.join(arnk.set_index(['pers_nbr']), on=['pers_nbr'] )
    #derived_rank is looking at primary rank first and then take into consideration of addtional rank
    prim['DerivedRnk_BC1']= np.where(prim['DerivedRnk_BC']==0, np.nan,prim['DerivedRnk_BC'] )
    #prim['derived_rank']= prim[['fac_rnk_cd','DerivedRnk']].apply(np.nanmax, axis=1)
    #prim['derived_rank_rev']= prim[['fac_rnk_cd','DerivedRnk_BC1']].apply(np.nanmax, axis=1)
    prim['derived_rank']= np.where(prim['fac_rnk_cd'].notnull(),prim['fac_rnk_cd'], prim['DerivedRnk'])
    prim['derived_rank_rev']= np.where(prim['fac_rnk_cd'].notnull(),prim['fac_rnk_cd'], prim['DerivedRnk_BC1'])
    
    #hard code derived rank for certain job_cd for 2011
    
    if dtfilter.endswith('11'):
        prim['derived_rank'] = np.where(prim['job_cd'].isin(['20001600','20001601']),4,prim['derived_rank'])
        prim['derived_rank_rev'] = np.where(prim['job_cd'].isin(['20001600','20001601']),4,prim['derived_rank_rev'])
        
    #code for Employee_category
    #status_cd='COTR' and have rank continuning staff?
    #BC fixed term faculty restrict on emp_cat_cd in('FMM','FFF','FXE','FHF','FNF') exclude specialist with rank
    prim['Employee_category']= np.where(prim['emp_cat_cd'].str.slice(0,1).isin(['F','S','G'])==False,None, np.where(prim['emp_cat_cd_1']=='G','Grad Asst',
                                       np.where(prim['emp_cat_cd_1']=='S', 'Non Acad',
                                               np.where(prim['status_cd'].isin(['TENR','TPRO']),'Tenure Sys',
                                                       np.where((prim['derived_rank_rev'].notnull()) & (prim['status_cd'].isnull()) & (prim['emp_cat_cd'].isin(['FMM','FFF','FXE','FHF','FFT'])) ,'Fix Fac',
                                                               np.where((prim['derived_rank_rev'].notnull()) & (prim['status_cd'].str.endswith('E')) & (prim['emp_cat_cd'].isin(['FMM','FFF','FXE','FHF','FFT'])), 'Fix Fac',
                                                                       np.where((prim['status_cd'].notnull()) & ( prim['status_cd'].str.endswith('E') == False),'Cont Staff', 'Fix Staff' 
                                                                               #np.where(prim['derived_rank_rev'].isnull(),'Fix Staff', None)
                                                                               )))))))
    
    #rank for academics= RPNAME in Acad
    prim['RANK']= np.where( (prim['status_cd'].isin(['TENR','TPRO']) ) & (prim['derived_rank_rev'].isnull()), 'Professor',
                        np.where(prim['derived_rank_rev']==4, 'Professor',
                                np.where(prim['derived_rank_rev']==3, 'Assoc Professor',
                                        np.where(prim['derived_rank_rev']==2,'Asst Professor',
                                                np.where(prim['derived_rank_rev']==1, 'Instructor', None)))))


    #newhire
    prim['New_Hires'] = np.where( (prim['cont_svc_date']>= np.datetime64(str(int(hrexdt[0:4])-1)+'-10-01') ) & \
                             (prim['cont_svc_date']<= np.datetime64(hrexdt[0:4]+ '-10-01') )  ,1,0 )
    #new hire using action
    prim['New_Hires_Act'] = np.where(prim['NewHire']=='Y',1, 0)
    #age
    prim['CYEAR'] = hrexdt[:4]
    #prim['Age']= (( prim['cutdt']-prim['birth_date']).dt.days)/365
    #Change age based on July date
    prim['Age']=( pd.to_datetime((prim['CYEAR'].astype(int) ).astype(str)+'-07-01' ) -prim['birth_date']).dt.days/365.25
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
                                                        np.where(prim['racial_cat_cd_1']=='R5',5, np.nan))))))
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
    prim['Name']= np.where(prim['fst_nm'].isnull(),prim['lst_nm'].str.strip(),prim['lst_nm'].str.strip()+', '+prim['fst_nm'] ) 
    prim['Lst_Fst_Mid_Name']= np.where( (prim['fst_nm'].isnull() ) & (prim['mid_nm'].isnull()), prim['lst_nm'].str.strip(),
                                       np.where( prim['mid_nm'].isnull(), prim['lst_nm'].str.strip()+', '+prim['fst_nm'],
                                              prim['lst_nm'].str.strip()+', ' +prim['fst_nm'].str.strip()+' ' +prim['mid_nm'].str.strip())) 
    
    
    #add OOI for primary job org
    HROOI= ooihr(hrexdt=hrexdt, ooiexdt=ooiexdt, dtfilter=dtfilter)
    prim = prim.join(HROOI.set_index('org_code').\
                     loc[:,['org_full_name','mau_code','mau_name','dept_code','dept_name']], on='org_cd')
    
    
    #correct for LTD
    prim['LTD']= np.where((prim['emp_status_cd']=='1') & (prim['LTD']=='Y'),'Y','N' )
    #sal
    prim['salaryflag']= np.where(prim['anl_sal']>0,'Y','N')

    return prim

prim18 = primpop(hrexdt='20181026', ooiexdt='20181026', dtfilter='01-OCT-18')
prim17 = primpop(hrexdt='20171026', ooiexdt='20171026', dtfilter='01-OCT-17')
prim11 = primpop(hrexdt='20111025', ooiexdt='201110', dtfilter='01-OCT-11')

prim12 = primpop(hrexdt='201210', ooiexdt='201210', dtfilter='01-OCT-12')
#primds(hrexdt, ooiexdt, dtfilter)
prim13 = primpop(hrexdt='20131018', ooiexdt='20131018', dtfilter='01-OCT-13')

prim14 = primpop(hrexdt='20141025', ooiexdt='20141025', dtfilter='01-OCT-14')

prim15 = primpop(hrexdt='20151027', ooiexdt='20151027', dtfilter='01-OCT-15')

prim16 = primpop(hrexdt='20161019', ooiexdt='20161019', dtfilter='01-OCT-16')

prim18.columns
prim18['hrly_rate'].max()
prim18.groupby(['Active','primasgndtfilter','emp_cat_cd_1']).size()
prim18['derived_rank_rev'].isnull().sum()
prim18[['emp_cat_cd_1','emp_cat_nm']]
(prim18['anl_sal']>0).sum()
prim = pd.concat([prim11,prim12,prim13,prim14,prim15,prim16,prim17, prim18])

def costdist(hrexdt, ooiexdt, dtfilter):
    FM = pd.read_sql("select * from HR_"+hrexdt+".HR_PY_FUND_MASTER_V \
                   where '"+dtfilter+"' between start_date and end_date", ct.EDW)
    CD = pd.read_sql("select * from HR_"+hrexdt+".HR_PA_COST_DISTRIBUTION_V \
                   where '"+dtfilter+"' between start_date and end_date and LBR_DSTR_TBL_TYP='DIST' ", ct.EDW)
    BASEPAY = pd.read_sql("select * from HR_"+hrexdt+".HR_PA_BASE_PAY_V \
                   where '"+dtfilter+"' between base_pay_start_date and base_pay_end_date and SRC_STYPE = '0'", ct.EDW)
    COSTDist = FM.loc[:,['org_cd','acct_pub_nm','fund_grp_cd','fund']].\
            join(CD.set_index('fund').loc[:,['pers_nbr','wt_pct','pymt_wage_typ_amt']], on='fund' , how='inner').\
            join(BASEPAY[['pers_nbr']].set_index('pers_nbr'), on='pers_nbr', how='inner').reset_index().\
            groupby(['pers_nbr','org_cd','fund_grp_cd','fund'])['wt_pct','pymt_wage_typ_amt'].sum()
    COSTDist['sum_pymt_wage_typ_amt']=COSTDist.groupby(['pers_nbr'])['pymt_wage_typ_amt'].transform(sum)   
    HROOI = ooihr(hrexdt=hrexdt, ooiexdt=ooiexdt, dtfilter=dtfilter)
    COSTDist = COSTDist.reset_index().join(HROOI.set_index('org_code').\
                     loc[:,['org_full_name','mau_code','mau_name','dept_code','dept_name']], on='org_cd')
    COSTDist['Weight_Percent']= COSTDist['pymt_wage_typ_amt']/COSTDist['sum_pymt_wage_typ_amt']*100
    COSTDist['FUND_TYP'] = np.where(COSTDist['fund'].str.startswith('MSG'),'1',
                           np.where(COSTDist['fund'].str.startswith('MSR'),'2',
                                   np.where( (COSTDist['fund'].str.startswith('MSD') ) & (COSTDist['fund'].str.startswith('MSDC')== False),'3',
                                           np.where(COSTDist['fund'].str.startswith('MSX'),'4',
                                                   np.where(COSTDist['fund'].str.startswith('MSA'),'9',
                                                           np.where(COSTDist['fund'].str.startswith('MSL'),'5','5'))))))
    COSTDist['CYEAR'] = hrexdt[:4]
    
    return COSTDist
cost18 = costdist(hrexdt='20181026', ooiexdt='20181026', dtfilter='01-OCT-18')
cost11 = costdist(hrexdt='20111025', ooiexdt='201110', dtfilter='01-OCT-11')

cost12 = costdist(hrexdt='201210', ooiexdt='201210', dtfilter='01-OCT-12')

cost13 = costdist(hrexdt='20131018', ooiexdt='20131018', dtfilter='01-OCT-13')
cost14 = costdist(hrexdt='20141025', ooiexdt='20141025', dtfilter='01-OCT-14')

cost15 = costdist(hrexdt='20151027', ooiexdt='20151027', dtfilter='01-OCT-15')

cost16 = costdist(hrexdt='20161019', ooiexdt='20161019', dtfilter='01-OCT-16')
cost17 = costdist(hrexdt='20171026', ooiexdt='20171026', dtfilter='01-OCT-17')

cost= pd.concat([cost11,cost12,cost13,cost14,cost15,cost16,cost17, cost18])
cost = cost.join(prim.set_index(['pers_nbr','CYEAR' ]).\
                 loc[:,['pers_id','Employee_category','org_cd','mau_code','dept_code','capc_util_lvl','anl_sal']], 
                 on=['pers_nbr','CYEAR' ], lsuffix='_pay', how='inner').query('anl_sal>0 ')

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


#additional org
def adtorg(hrexdt, ooiexdt, dtfilter):
    ASGN = pd.read_sql("select a.*,x.pers_id from HR_"+hrexdt+".hr_pa_fas_assignment_v a  inner join HR_"+hrexdt+".HR_PA_PERNR_CROSSWALK_V x \
                   on a.pers_nbr=x.pers_nbr where '"+dtfilter+"' between start_date and end_date ", ct.EDW)
    #transpose the columns to rows
    col  =[x for x in ASGN.columns if  re.match(r'prim_asgn_also_rpt_to_\d',x) or re.match(r'\w+_org_cd_\d+',x) ]
    asgndf = ASGN.set_index(['pers_id', 'pers_nbr']).filter(items=col)
    asgndfL = asgndf.stack().reset_index().rename(columns={'level_2':'variable',0:'org_cd'})
    #ooi = ooihr(hrexdt, ooiexdt, dtfilter)
    #asgndfL = asgndfL.join(ooi.set_index('org_code'), on='org_code')
    return asgndfL.drop('variable',axis=1)


def allorgstr(hrexdt, ooiexdt, dtfilter):
    
    #combine previous data
    primt = prim[ prim['CYEAR']==hrexdt[:4]]
    costdis = cost[cost['CYEAR']==hrexdt[:4]]
    
    primorg = primt[['pers_id','pers_nbr','org_cd','CYEAR']]
    primorg['type']='owner_org'
    tenureorg= primt[['pers_id','pers_nbr','tenure_cont_grant_dept','CYEAR']].rename(columns={'tenure_cont_grant_dept':'org_cd'})
    tenureorg= tenureorg[tenureorg['org_cd'].notnull()]
    tenureorg['type']='tenure_org'
    adorg=adtorg(hrexdt=hrexdt, ooiexdt=ooiexdt, dtfilter=dtfilter).\
    join(primorg.set_index(['pers_id','pers_nbr']), on=['pers_id','pers_nbr'], rsuffix='x' , how='inner').drop('org_cdx',axis=1)
    adorg['type']='also_reports_to_org'

    payorg=costdis[['pers_nbr','org_cd_pay','CYEAR']].\
    join(primorg[['pers_id','pers_nbr','CYEAR']].set_index(['pers_nbr','CYEAR']), on=['pers_nbr','CYEAR'], how='inner')
    payorg= payorg[['pers_id','pers_nbr','org_cd_pay','CYEAR']].rename(columns={'org_cd_pay':'org_cd'})
    payorg['type']='payer_org'
    
    orgall = pd.concat([primorg,tenureorg,payorg,adorg]).drop_duplicates()
    orgall['value']='Y'
    #orgall = orgall.pivot_table(index=['CYEAR','org_cd','pers_id','pers_nbr'],columns='type', values='value',
     #                           aggfunc='first',fill_value='N')
    orgall = orgall.drop('CYEAR',axis=1).set_index(['pers_id','pers_nbr','org_cd','type']).unstack('type')

    orgall.columns = orgall.columns.droplevel()
    orgall= orgall.fillna('N').reset_index()
    paypct=costdis.groupby(['pers_nbr','org_cd_pay','fund','fund_grp_cd'])['wt_pct'].sum().reset_index()
    orgall['payorg']= np.where(orgall['payer_org']=='Y', orgall['org_cd'], None)
    orgall = orgall.join(paypct.set_index(['pers_nbr','org_cd_pay']), on=['pers_nbr','payorg'])
    ooi = ooihr(hrexdt=hrexdt, ooiexdt=ooiexdt, dtfilter=dtfilter)
    orgall = orgall.join(ooi.set_index('org_code'), on='org_cd', how='inner')
    orgall['CYEAR']= hrexdt[:4]
    orgall['structure_type']='HR'
    
    return orgall

dat18 = allorgstr(hrexdt='20181026', ooiexdt='20181026', dtfilter='01-OCT-18')
dat11 = allorgstr(hrexdt='20111025', ooiexdt='201110', dtfilter='01-OCT-11')
dat12 = allorgstr(hrexdt='201210', ooiexdt='201210', dtfilter='01-OCT-12')
dat13 = allorgstr(hrexdt='20131018', ooiexdt='20131018', dtfilter='01-OCT-13')
dat14 = allorgstr(hrexdt='20141025', ooiexdt='20141025', dtfilter='01-OCT-14')
dat15 = allorgstr(hrexdt='20151027', ooiexdt='20151027', dtfilter='01-OCT-15')
dat16 = allorgstr(hrexdt='20161019', ooiexdt='20161019', dtfilter='01-OCT-16')
dat17 = allorgstr(hrexdt='20171026', ooiexdt='20171026', dtfilter='01-OCT-17')
AllOrgdat = pd.concat([dat11,dat12,dat13,dat14,dat15,dat16,dat17,dat18])
#function to convert to datatype in sql
import sqlalchemy
def sqlcol(dfparam):    

    dtypedict = {}
    for i,j in zip(dfparam.columns, dfparam.dtypes):
        if "object" in str(j):
            dtypedict.update({i: sqlalchemy.types.VARCHAR(length=255)})
        
        if "datetime64[ns]" in str(j):
            dtypedict.update({i: sqlalchemy.types.DateTime()})

        if "float" in str(j):
            dtypedict.update({i: sqlalchemy.types.Float(precision=1, asdecimal=True)})

        if "int" in str(j):
            dtypedict.update({i: sqlalchemy.types.INT()})

    return dtypedict

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
prim.to_sql('HR_Person_Position_Job_AllEmployee', engine, if_exists = 'replace', chunksize = 500,index=False, dtype=sqlcol(prim) )
AllOrgdat.to_sql('HR_EMP_ORG_RELA_AllEmployee', engine, if_exists = 'replace', chunksize = 500,index=False, dtype=sqlcol(AllOrgdat) )
