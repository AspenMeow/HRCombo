/*the program is to build longitudional HR froze combo file **/
/***db access***/
%include "H:\SAS\SAS_Log_EDW.sas" ;
LIBNAME OPB  oracle path="MSUEDW" user="&MSUEDW_uid" pw= "&MSUEDW_pwd"
	  	schema = OPB preserve_tab_names = yes connection=sharedread;

option symbolgen;
options mlogic;


/**macro to do the primary assignment person level data pull**/
%macro primds(hrexdt,  dtfilter);
/*assign libref based on extract date*/
 	LIBNAME hr  oracle path="MSUEDW" user="&MSUEDW_uid" pw= "&MSUEDW_pwd"
	  	schema = HR_&hrexdt preserve_tab_names = yes connection=sharedread;
    
	proc sql stimer;
	create table primasgn as 
	select *
	from hr.HR_PA_EMP_PRIMARY_ASSIGNMENT_V
	where &dtfilter between start_date and end_date;

	create table pdetail as 
	select *
	from hr.HR_PA_PERSON_DETAIL_SV 
    where  &dtfilter between start_date and end_date;

	/*no fac_rnk_cd in 2011 need to de-select different fields accordingly*/
	create table job as
	select *
	from hr.HR_OM_JOB_V
	where  &dtfilter between start_date and end_date;

	
	create table activeperson as 
	select distinct pers_nbr,emp_status_cd,emp_status_nm, 'Y' as active
	from hr.HR_PA_ACTIONS_V
	where  &dtfilter between start_date and end_date and  EMP_STATUS_CD in ('1','3');

	/*max education level*/
	create table EDPrsn as 
	select distinct pers_id, max(cert_cd) as cert_cd
	from hr.HR_PA_PERSON_EDUCATION_V
	group by pers_id;

	/*new hire based on Bethan/margart logic hire action between 07/01 to 09/30- this is not used in PPS*/
	create table newhire as 
	select distinct pers_nbr, 'Y' as New_Hires_Act
	from hr.HR_PA_ACTIONS_V
	where ACTN_TYP_CD='ZA' and   EMP_STATUS_CD in ('1','3') and  start_date between "01JUL%substr(&dtfilter,7,4)"D and "30SEP%substr(&dtfilter,7,4)"D ;

	/*LTD leave end date less than or equal to 10/1/Frozen Year*/
	create table LTD as 
	select distinct pers_nbr, max(start_date) as max_actn_dt, 'Y' as LTD
	from hr.HR_PA_ACTIONS_V
    where ACTN_TYP_CD='ZQ' and   EMP_STATUS_CD in ('1') and cust_status_cd in ('6','8') 
     group by pers_nbr 
     having max(start_date) <= "01OCT%substr(&dtfilter,7,4)"D;
    quit;

	/*max active date based on talk with Jaime on 3/25. as long as one has active record after LTD, should reverst LTD*/
	create table maxactivedate as 
	select distinct pers_nbr, max(start_date) as max_active_dt 
	from 	hr.HR_PA_ACTIONS_V 
	where   EMP_STATUS_CD in ('3')
	group by pers_nbr;

	/*get rid of variables*/
	data primasgn;
	set primasgn;
	drop curr_ind src_last_updated_by src_last_update_date created_by create_date last_updated_by last_update_date;
	run;

	data pdetail;
	set pdetail;
	drop curr_ind src_last_updated_by src_last_update_date created_by create_date last_updated_by last_update_date start_date end_date;
	run;


	data job;
	set job;
	drop curr_ind src_last_updated_by src_last_update_date created_by create_date last_updated_by last_update_date start_date end_date;
	run;


	proc sql stimer;
	create table PRIM%substr(&hrexdt,3,2) as 
	select p.*,x.pers_id, 
	j.*,
	psv.ssn,psv.uuid,psv.fst_nm,psv.mid_nm, psv.lst_nm,psv.birth_date, psv.gndr_cd, psv.gndr_nm,psv.zpid,
	pdetail.*,
	res.res_status_cd,res.res_status_nm,
    dspec.cont_svc_date,dspec.time_in_lvl_date,
	activeperson.*,
	bn.acum_fte_svc_mths,bn.vstd_date,
	EDPrsn.cert_cd,
	basepay.pay_scale_typ_cd,basepay.pay_scale_area_cd,basepay.wage_typ_cd_2,basepay.pymt_wage_typ_amt_2,basepay.pay_scale_grp_cd,basepay.pay_scale_lvl_cd,
    basepay.capc_util_lvl, basepay.anl_sal,
	pos.pos_ttl,
	fasdetail.status_cd, fasdetail.status_nm,fasdetail.tenure_cont_grant_dept,fasdetail.tenure_cont_status_grant_date,
    fasdetail.tenure_exmp_agr_date,fasdetail.tenure_cont_sys_entr_date,fasdetail.promo_to_cur_rnk_entr_date, newhire.New_Hires_Act,LTD.max_actn_dt, LTD.LTD,maxactivedate.max_active_dt
	from primasgn p
	inner join hr.HR_PA_PERNR_CROSSWALK_V x
	on p.pers_nbr=x.pers_nbr
	inner join job j
	on p.JOB_CD=j.JOB_CD
	left join 
		(select *
			from hr.HR_PA_PERSON_SV 
          where  &dtfilter between start_date and end_date  ) psv
	on x.pers_id=psv.pers_id
	left join pdetail
	on x.pers_id=pdetail.pers_id 
	left join 
		(select *
			from hr.HR_PA_RESIDENCE_STATUS_SV
          where  &dtfilter between start_date and end_date  ) res
	on x.pers_id=res.pers_id

	left join 
		(select *
			from hr.HR_PA_DATE_SPECIFICATIONS_SV
          where  &dtfilter between start_date and end_date  ) dspec
	on p.pers_nbr=dspec.pers_nbr
	left join activeperson
	on p.pers_nbr= activeperson.pers_nbr
	left join 
		(select *
			from hr.HR_BN_BNFT_SVC_INFO_V
         where  &dtfilter between start_date and end_date    ) bn
	on p.pers_nbr=bn.pers_nbr
	left join EDPrsn
	on x.pers_id=EDPrsn.pers_id
	inner join 
		(select *
			from hr.HR_PA_BASE_PAY_V
         where &dtfilter between base_pay_start_date and base_pay_end_date and SRC_STYPE = '0') basepay
	on p.pers_nbr=basepay.pers_nbr
	left join 
		(select *
			from hr.HR_OM_POSITIONS_V
          where  &dtfilter between start_date and end_date  ) pos
	on p.pos_nbr=pos.pos_nbr
	left join 
		(select *
			from hr.HR_PA_FAS_DETAILS_V
		where  &dtfilter between start_date and end_date ) fasdetail
	on x.pers_id=fasdetail.pers_id
	left join newhire
	on p.pers_nbr=newhire.pers_nbr
	left join LTD
	on p.pers_nbr=LTD.pers_nbr
	left join maxactivedate
	on p.pers_nbr=maxactivedate.pers_nbr;
	quit;

%mend;
/*%primds(hrexdt=20181026,  dtfilter='01OCT2018'D);*/


/**macro to do the find addtional rank from fas assignment**/
%macro adtrank(hrexdt,  dtfilter);
/*assign libref based on extract date*/
 	LIBNAME hr  oracle path="MSUEDW" user="&MSUEDW_uid" pw= "&MSUEDW_pwd"
	  	schema = HR_&hrexdt preserve_tab_names = yes connection=sharedread;
  
	proc sql stimer;
	create table asgn as
	select *
	from hr.HR_PA_FAS_ASSIGNMENT_V
	where  &dtfilter between start_date and end_date;
	quit;

/*ouput variable names from fas asgnment */
proc contents data = asgn noprint out = asgncol (keep = name varnum);
run;


/*check whether fac_rnk_cd exisit*
/*fac_rnk_cd doesn't exist for 2011 cohort, but I want to build the check more robustly based on whether the var exisit, not based on certain year*/
data asgncol;
set asgncol;
if NAME=: 'FAC_RNK_CD' then var=2;
else if NAME=: 'FAS_ASGN_NM' then var=1;
else var=0;
run;

proc sql noprint ;
select max(var) into :exist
from asgncol;
quit;
%put &exist;

/*select additional rank relevent variables*/
data colselect;
set asgncol;
digit = anydigit(NAME);
if digit >0 then do;
	group= substr(NAME,1, digit-2);
	seq= substr(NAME, digit,1);
end;
where NAME not contains 'ORG' and NAME not contains 'PRIM_ASGN_ALSO_RPT_TO';
if NAME ='PERS_NBR' or digit>0;
drop VARNUM digit;
run;

/*use macro variable to extract out variable names*/
proc sql noprint;
 select distinct NAME into: varlist separated by ','
 from colselect;

 select distinct NAME into: varlist1 separated by ' '
 from colselect
 where NAME <> 'PERS_NBR';

 create table asgn1 as 
 select &varlist
 from asgn;
 quit;

 /*transpose table*/
 proc sort data=asgn1 ; by pers_nbr; run;

PROC TRANSPOSE DATA=asgn1 OUT=asgn1 PREFIX=value NAME=Source LABEL=Label;
BY pers_nbr;
VAR &varlist1;
run;
/*merge with colselect structure by pers_nbr, group, seq */
proc sql stimer;
create table asgnt as 
select a.pers_nbr, a.value1 as value, b.seq,b.group
from asgn1 a
inner join colselect b
on a.Label=b.NAME
where a.value1 is not null;
quit;
/*wide transpose to the final structure*/
 proc sort data=asgnt ; by pers_nbr seq; run;
proc transpose data=asgnt out=asgnf ;
    by pers_nbr seq ;
    id group;
    var value;
run;

/*conditiionally processing based on whether the fac_rnk_cd exsit or not */
%if &exist=2 %then %do;
	data asgnf;
		set asgnf;
		/*this is original way without taking into considering the asgn_typ_cd, 
		use the last char of fac_rnk_cd, 4 is highest for professor*/
      	DerivedRnk= substr(fac_rnk_cd,4,1);
		DerivedRnk1= input(DerivedRnk,1.);
		drop DerivedRnk;
		rename DerivedRnk1=DerivedRnk;
	run;
%end;
%else %if &exist=1 %then %do; /*if the fac_rnk_cd doesn't exist which happes in 2011, use the fac_asgn_nm to recode*/
	data asgnf;
		set asgnf;
		if  find(FAS_ASGN_NM,'PROFESSORIAL','i') ge 1 then DerivedRnk=.;
		else if  find(FAS_ASGN_NM,'ASSISTANT INSTRUCTOR','i') ge 1 then DerivedRnk=.;
		else if  find(FAS_ASGN_NM,'INSTRUCTOR','i') ge 1 then DerivedRnk=1;
		else if  find(FAS_ASGN_NM,'ASSISTANT PROFESSOR','i') ge 1 then DerivedRnk=2;
		else if  find(FAS_ASGN_NM,'ASSOCIATE PROFESSOR','i') ge 1 then DerivedRnk=3;
		else if  find(FAS_ASGN_NM,'PROFESSOR','i') ge 1 then DerivedRnk=4;
		else if  find(FAS_ASGN_NM,'PROF','i') ge 1 then DerivedRnk=4;
		else DerivedRnk=.;
	 run;
%end;
/*add the new logic for dervied rank exclude some asgntype cd*/
data asgnf%substr(&hrexdt,3,2);
	set asgnf;
if asgn_typ_cd in ('2','4','8','3','9','A','B') then DerivedRnk_BC=.;
else DerivedRnk_BC=DerivedRnk;
run;

/*get all addtionial orgs from the fas asgn for later val stuff need*/
data orgcolselect;
set asgncol;
where NAME  contains 'ORG' or NAME  contains 'PRIM_ASGN_ALSO_RPT_TO' or NAME ='PERS_NBR';
run;

proc sql noprint;
select NAME into: colvar separated by ' '
from orgcolselect;

select NAME into: colvar1 separated by ' '
from orgcolselect
where NAME <> 'PERS_NBR';

data adorg;
set asgn;
keep &colvar;
run;

/*transpose aditonal org to long */
proc sort data=adorg; by pers_nbr; run;
PROC TRANSPOSE DATA=adorg OUT=adorg PREFIX=org NAME=Source LABEL=Label;
BY pers_nbr;
VAR &colvar1;
run;

data adorg%substr(&hrexdt,3,2);
set adorg;
where org1 ne '';
run;
%mend;
/****macro for HR ORG structure hirachery***/
%macro ooihr(ooiexdt);
	/*assign ooi libref based on extract date*/
 	LIBNAME ooi  oracle path="MSUEDW" user="&MSUEDW_uid" pw= "&MSUEDW_pwd"
	  	schema = OOI_&ooiexdt preserve_tab_names = yes connection=sharedread;
	
  	proc sql stimer;
	/*get HR org hirachy from ooi_ooi_org_struct_cd_lvl_rv*/
	 create table OOIHR as 
	 select org_code,org_full_name, org_type, mau_code,mau_name,dept_code,dept_name,structure_type, 1 as type
	 from ooi.OOI_OOI_ORG_STRUCT_CD_LVL_RV
	 where structure_type='HR';
	 /*get HR org hirachy from OOI_OOI_ORG_STRUCT_CODE_AV to amend some of the org missing structure-broadcasting in this case*/
	 create table STRUCTHR as
	 select org_code,org_full_name,org_type,mau as mau_code,mau_name,dept as dept_code,dept_name,struct_type as structure_type,2 as type
	 from ooi.OOI_OOI_ORG_STRUCT_CODE_AV a
	 left join (select org_ref_code, org_full_name as mau_name from ooi.OOI_OOI_ORG_V where org_type='MAU' ) b 
	 on a.MAU=b.ORG_REF_CODE
	 left join (select org_ref_code, org_full_name as dept_name from ooi.OOI_OOI_ORG_V where org_type='DP' ) c
     on a.DEPT=c.ORG_REF_CODE
	 where a.struct_type='HR';

	 /*union 2 sources above and choose the min type*/
	 create table ooihrall as 
	 select *
	 from OOIHR
	 union
	 select *
	 from STRUCTHR;

	 create table HROOI%substr(&ooiexdt,3,2) as 
	 select a.org_code,a.org_full_name, org_type, mau_code,mau_name,dept_code,dept_name,structure_type
	 from ooihrall a 
	 inner join (
	 	select org_code, min(type) as mintype
	 		from ooihrall
	 	group by org_code) b
	 on a.org_code=b.org_code and a.type=b.mintype;

	quit;
%mend;
/*%ooihr(ooiexdt=20181026);*/


/*deriviation and prim table person job position**/
%macro primpop(hrexdt, ooiexdt, dtfilter);
 /*check if the fac_rnk_cd exist*/ 
/*2011 job table do not have fac_rnk_cd, code fac_rnk_cd based on job_ttl text field*/
proc contents data = prim%substr(&hrexdt,3,2) noprint out = primcol (keep = name varnum);
run;

data primcol;
set primcol;
if NAME=: 'FAC_RNK_CD' then var=2;
else if NAME=: 'JOB_TTL' then var=1;
else var=0;
run;

proc sql noprint ;
select max(var) into :rnkexist
from primcol;
quit;
%put &rnkexist;

  data prim%substr(&hrexdt,3,2);
   set prim%substr(&hrexdt,3,2);
   emp_cat_cd_1 = substr(emp_cat_cd,1,1);
/*filter include only academics, support staff and Grad assistant remove non-employee/no-pay remove non-active
    remove PRIM_ASGN_END_DATE before frozen date*/
/*pers_id=="00140406" in 2015 frozn have prim_asgn_end_date of 9999-12-31*/

   where substr(EMP_CAT_CD,1,1) in ('F','S','G') 
   and emp_grp_cd ne '5'
   and active ='Y'
   /*per talk with Jaime on 3/26/19, add in 9999999 if pos nbr is 99999999 then still use end date*/
   and ( datepart(prim_asgn_end_date)  =  '01JAN1900'D  or datepart(prim_asgn_end_date)>= &dtfilter or pos_nbr =99999999) ;
   run;

/*conditionally code for rank based on whether fac_rnk_cd in the job table or not*/
%if &rnkexist=2 %then %do;
	data prim%substr(&hrexdt,3,2);
	set prim%substr(&hrexdt,3,2);
	fac_rnk_cd= substr(fac_rnk_cd,4,1);
	fac_rnk_cd1= input(fac_rnk_cd,1.);
	drop fac_rnk_cd;
	rename fac_rnk_cd1=fac_rnk_cd;
	run;
%end;
%else %if &rnkexist=1 %then %do;
   data prim%substr(&hrexdt,3,2);
	set prim%substr(&hrexdt,3,2);
	if  find(upcase(job_ttl),'PROFESSORIAL','i') ge 1 then fac_rnk_cd=.;
		else if  find(upcase(job_ttl),'ASSISTANT INSTRUCTOR','i') ge 1 then fac_rnk_cd=.;
		else if  find(upcase(job_ttl),'INSTRUCTOR','i') ge 1 then fac_rnk_cd=1;
		else if  find(upcase(job_ttl),'ASSISTANT PROFESSOR','i') ge 1 then fac_rnk_cd=2;
		else if  find(upcase(job_ttl),'ASSOCIATE PROFESSOR','i') ge 1 then fac_rnk_cd=3;
		else if  find(upcase(job_ttl),'PROFESSOR','i') ge 1 then fac_rnk_cd=4;
		else if  find(upcase(job_ttl),'PROF-','i') ge 1 then fac_rnk_cd=4;
	else fac_rnk_cd=.;
	run;
%end;
/*add total FTE (cul) by first character of emp_cat_cd_1*/
proc sql stimer;
create table CULag as 
select distinct pers_id,emp_cat_cd_1, sum(capc_util_lvl ) as cul 
from prim%substr(&hrexdt,3,2) 
where capc_util_lvl is not null and emp_cat_cd is not null
group by pers_id,emp_cat_cd_1;

create table prim%substr(&hrexdt,3,2) as 
select a.*,b.cul
from prim%substr(&hrexdt,3,2) a 
left join CULag b
on a.pers_id=b.pers_id and a.emp_cat_cd_1=b.emp_cat_cd_1;

/*take the lowest sequential addtional rank*/
create table lrnk as 
select a.*
from asgnf%substr(&hrexdt,3,2) a 
inner join (
select distinct pers_nbr, min(seq) as minseq
from asgnf%substr(&hrexdt,3,2)
group by pers_nbr) b
on a.pers_nbr=b.pers_nbr and a.seq=b.minseq
;

create table prim%substr(&hrexdt,3,2) as 
select a.*,b.DerivedRnk_BC
from prim%substr(&hrexdt,3,2) a 
left join lrnk b
on a.pers_nbr=b.pers_nbr;

data prim%substr(&hrexdt,3,2);
set prim%substr(&hrexdt,3,2);
length Employee_category $20 RANK$20;
/*primary first and then addtional*/
if fac_rnk_cd =. then  derived_rank_rev=DerivedRnk_BC;
else derived_rank_rev=fac_rnk_cd;

/*hard code derived rank for certain job_cd for 2011 base on Jaime SQL program */
 if %substr(&dtfilter,7,4)='2011' then do;
	 if job_cd in ('20001600','20001601') then derived_rank_rev=4;
 end;
 if emp_cat_cd_1='G' then Employee_category='Grad Asst';
 else if emp_cat_cd_1='S' then Employee_category='Non Acad';
 /*need to check those with FFT emp_cat_cd but no status_cd - email exchange on 11/9/2018??*/
 else if status_cd in ('TENR','TPRO') then Employee_category='Tenure Sys';
 /*need to consult with Margaret on whether fix term faculty have to have certain emp_cat_Cd regardless of rank 
 so BC fixed term faculty restrict on emp_cat_cd in('FMM','FFF','FXE','FHF','FNF') this exclude specialist with rank email exchange on 11/9/2018 
 this resolve on 1/15/2019 conversation with Jaime include the emp_cat_cd restriction on fix term but take FNF out */
 else if derived_rank_rev ne . and status_cd='' and emp_cat_cd in ('FMM','FFT','FFF','FXE','FHF') then Employee_category='Fix Fac';
 else if derived_rank_rev ne . and substr(status_cd,length(status_cd),1)='E' and emp_cat_cd in ('FMM','FFT','FFF','FXE','FHF') then Employee_category='Fix Fac';
 else if status_cd ne '' and  substr(status_cd,length(status_cd),1) ne 'E' then Employee_category='Cont Staff';
 /*take out restriction on paid rank to accomodation with with ranks but emp_cat_cd is not those included before and without status cd */
 else Employee_category='Fix Staff';

 /*rank #rank for academics= RPNAME in Acad*/
 if status_cd in ('TENR','TPRO') and derived_rank_rev=. then RANK='Professor';
 else if derived_rank_rev=4 then RANK='Professor';
 else if derived_rank_rev=3 then RANK='Assoc Professor';
 else if derived_rank_rev=2 then RANK='Asst Professor';
 else if derived_rank_rev=1 then RANK='Instructor';

 /*newhire include the PPS new hire*/
 if datepart(cont_svc_date) >= "01OCT%eval( %sysfunc(inputn(%substr(&dtfilter,7,4),4.))-1)"D and 
    datepart(cont_svc_date) <= "01OCT%substr(&dtfilter,7,4)"D then New_Hires=1;
else New_Hires=0;
/*new hire based on action*/
 if New_Hires_Act ='' then New_Hires_Act=0;
 else New_Hires_Act=1;
/*age change age based on 07-01-extraction year per discussion with Jaime on 2/14/19/
 age = ("01JUL%substr(&dtfilter,7,4)"D - datepart(birth_date) )/365.25;
 /*Citizen*/
 if res_status_cd='C' then CITIZEN='1';
 else if res_status_cd='N'  then CITIZEN='3';
 else if res_status_cd in ('S','T','A') then CITIZEN='2';
 else CITIZEN='1';
 /*gender- if missing give to M*/
 if gndr_cd='1' then GENDER='M';
 else if gndr_cd='2' then GENDER='M';
 else GENDER='M';
/*ethnic need to recheck coding for ethnic, records without racial_cat_cd_1 given white*/
 if ethnc_cd='E1' then ETHNIC=3;
 else if racial_cat_cd_1='R1' then ETHNIC=4;
 else if racial_cat_cd_1='R2' then ETHNIC=7;
 else if racial_cat_cd_1='R3' then ETHNIC=1;
 else if racial_cat_cd_1='R4' then ETHNIC=6;
 else if racial_cat_cd_1='R5' then ETHNIC=5;
 else ETHNIC=.;

 /*recode other ethnic*/
ARRAY evar{5} racial_cat_cd_1-racial_cat_cd_5;
ARRAY Ethnicar{5} ETHNIC1-ETHNIC5;
do i=1 to 5;
	if evar{i}='R1' then Ethnicar{i}='4';
	else if evar{i}='R2' then Ethnicar{i}='7';
	else if evar{i}='R3' then Ethnicar{i}='1';
	else if evar{i}='R4' then Ethnicar{i}='6';
	else if evar{i}='R5' then Ethnicar{i}='5';
 end;
 if ethnc_cd ne 'E1' then do;
	ETHNIC1= ETHNIC2;
	ETHNIC2= ETHNIC3;
	ETHNIC4= ETHNIC5;
	ETHNIC5= '';
end;
/*change LTD - only when max active date < LTD date then LTD talk with Jaime on 03/25/2019*/
if max_active_date<max_actn_dt and LTD='Y' then LTD='Y';
else LTD='N';

/*first character of EEO*/
EEO_1 = substr(eeo_cd,1,1);
 /*Name count for missing fst nm*/
if fst_nm ='' then Name = compress( lst_nm );
else Name=compress( lst_nm ||', '||fst_nm);
/*add mid name*/
if fst_nm ='' and mid_nm='' then Lst_Fst_Mid_Name = compress( lst_nm );
else if mid_nm='' then Lst_Fst_Mid_Name=compress( lst_nm ||', '||fst_nm);
else Lst_Fst_Mid_Name= compress( lst_nm ||', '||mid_nm||' ' ||fst_nm);

/*temp oncall*/
if emp_sgrp_cd in ('AC','AW') then TempOnCall='Y';
else TempOnCall='N';
/*extract year*/
 CYEAR = %substr(&dtfilter,7,4);
 run;
/*add OOI for primary job org*/
proc sql stimer;
create table prim%substr(&hrexdt,3,2) as 
select a.*,b.org_full_name,b.mau_code,b.mau_name,b.dept_code,b.dept_name
from prim%substr(&hrexdt,3,2) a
left join HROOI%substr(&ooiexdt,3,2) b
on a.org_cd=b.org_code;  

/*find same person with multiple positions*/
proc sql stimer;
create table mppl as 
select a.*
from prim%substr(&hrexdt,3,2) a 
inner join (
	select pers_id, cyear, count(*) as npos
	from prim%substr(&hrexdt,3,2)
	where emp_cat_cd_1 in ('F','S')
	group by pers_id, cyear
	having count(*)>1) b
on a.pers_id=b.pers_id and a.cyear=b.cyear
where a.emp_cat_cd_1 in ('F','S');
/*get the acd org- proxi those served as tenure org as acdemic org*/
create table acdorg as 
select distinct org_cd, tenure_org_flag
from OPB.HR_PA_CURR_EMP_ORG_RELA_RV
where tenure_org_flag='Y';

create table mppl as 
select a.*, (case when b.tenure_org_flag is null then 0 else 1 end) as acadunit
from mppl a
left join acdorg b
on a.org_cd=b.org_cd;

quit;

data mppl;
set mppl;
if emp_cat_cd_1='F' then Acad=1; else Acad=0;
if TempOnCall='Y' or LTD = 'Y' then LTDTempexlusion=0; else LTDTempexlusion=1;
if anl_sal>0 then paid=1; else paid=0;
if datepart(prim_asgn_end_date)='01JAN1900'D then ContEndDate=1; else ContEndDate=0;
run;

%macro limit(vlist);
%do i=1 %to  9;
	proc sort data = mppl;
  			by pers_id %scan(&vlist, &i)  ;
	run;
	data mppl;
  		set mppl;
  	by   pers_id %scan(&vlist, &i) ;
	%if &i>7 %then %do;
		if first.%scan(&vlist, &i) then output;
	%end;
	%else %do;
		 if last.%scan(&vlist, &i) then output;
	%end;
	run;
%end;
%mend;
/*vlist is the sequence of selecting adding pers_nbr in the end to make sure the loop ends*/
proc sql noprint;
select count(*) into : nrow
from mppl;
quit;
%put &nrow;
%if &nrow>0 %then %do;
	%limit(vlist=Acad LTDTempexlusion paid capc_util_lvl pay_scale_lvl_cd acadunit ContEndDate start_date pers_nbr);
	/*merge with prim*/
	proc sql stimer;
	create table prim%substr(&hrexdt,3,2) as 
	select a.*, p.posnum, (case when a.emp_cat_cd_1 in ('S','F') and p.posnum=1 then 1
				when a.emp_cat_cd_1 in ('S','F') and p.posnum>1 and b.pers_nbr is not null  then 1 else 0 end) as FacStafPosFlag
	from prim%substr(&hrexdt,3,2) a
	inner join (select distinct pers_id, count(*) as posnum from prim%substr(&hrexdt,3,2) group by pers_id) p
	on a.pers_id=p.pers_id
	left join mppl b
	on a.pers_nbr=b.pers_nbr and a.pers_id=b.pers_id;
	quit;
%end;
%else %do;/*if no multiple positions all flag with 1*/
	data prim%substr(&hrexdt,3,2);
	set prim%substr(&hrexdt,3,2);
	posnum=1;
	FacStafPosFlag=1;
%end;

/*get IPEDS flag including only emp_status_cd=1*/
data prim%substr(&hrexdt,3,2);
set prim%substr(&hrexdt,3,2);
if emp_status_cd='1' then IPEDS_Flag='N';
else if TempOnCall='Y' then IPEDS_Flag='N';
else if LTD='Y' then IPEDS_Flag='N';
else if emp_cat_cd_1 in ('S','F') and FacStafPosFlag=1 then IPEDS_Flag='Y';
else IPEDS_Flag='N';
run;

%mend;

/***macro for cost distribution**/
%macro costdist(hrexdt, dtfilter,ooiexdt);
/*assign libref based on extract date*/
 	LIBNAME hr  oracle path="MSUEDW" user="&MSUEDW_uid" pw= "&MSUEDW_pwd"
	  	schema = HR_&hrexdt preserve_tab_names = yes connection=sharedread;
proc sql stimer;
create table CostDist as 
select cd.pers_nbr,fm.org_cd,fm.fund_grp_cd,fm.fund, sum(wt_pct) as wt_pct ,sum(pymt_wage_typ_amt) as pymt_wage_typ_amt
from hr.HR_PY_FUND_MASTER_V fm
inner join hr.HR_PA_COST_DISTRIBUTION_V cd
on fm.fund=cd.fund 
inner join hr.HR_PA_BASE_PAY_V pay
on cd.pers_nbr=pay.pers_nbr
where &dtfilter between fm.start_date and fm.end_date
and &dtfilter between cd.start_date and cd.end_date
and &dtfilter between base_pay_start_date and base_pay_end_date
and cd.LBR_DSTR_TBL_TYP='DIST'
and pay.SRC_STYPE = '0'
group by cd.pers_nbr,fm.org_cd,fm.fund_grp_cd,fm.fund;

create table CostDist as 
select a.*,b.sum_pymt_wage_typ_amt
from CostDist  a
inner join (select distinct pers_nbr, sum(pymt_wage_typ_amt) as sum_pymt_wage_typ_amt from CostDist group by pers_nbr) b
on a.pers_nbr=b.pers_nbr;

/*get pay org structure from ooi*/
create table cost as 
select a.*,i.org_full_name,i.mau_code,i.mau_name,i.dept_code,i.dept_name
from costdist a 
left join HROOI%substr(&ooiexdt,3,2) i
on a.org_cd=i.org_code;
quit;

data cost;
set cost;
if sum_pymt_wage_typ_amt ne . and sum_pymt_wage_typ_amt>0 then Weight_Percent= pymt_wage_typ_amt/sum_pymt_wage_typ_amt*100;
else Weight_Percent=.;
if substr(fund,1,3)='MSG' then FUND_TYP='1';
else if substr(fund,1,3)='MSR' then FUND_TYP='2';
else if substr(fund,1,3)='MSD' and substr(fund,1,4) ne 'MSDC' then FUND_TYP='3';
else if substr(fund,1,3)='MSX' then FUND_TYP='4';
else if substr(fund,1,3)='MSA' then FUND_TYP='9';
else if substr(fund,1,3)='MSL' then FUND_TYP='5';
else FUND_TYP='5';
CYEAR= %substr(&dtfilter,7,4);

/*add additional fields from prim*/
proc sql stimer;
create table cost as 
select b.pers_id,b.Employee_category,b.org_cd,b.mau_code,b.dept_code,b.capc_util_lvl,b.anl_sal,a.DEPT_CODE as DEPT_CODE_pay,
a.DEPT_NAME , a.Fund, a.fund_grp_cd, a.fund_typ,a.MAU_CODE as MAU_CODE_pay, a.MAU_NAME ,
a.ORG_CD as ORG_CD_pay, a.ORG_FULL_NAME , a.pers_nbr, a.Weight_Percent,a.pymt_wage_typ_amt,a.sum_pymt_wage_typ_amt,a.wt_pct,a.cyear
from cost a 
inner join prim%substr(&hrexdt,3,2) b
on a.pers_nbr=b.pers_nbr and a.cyear=b.cyear
where b.anl_sal>0
/*exclude LTD and temp on call*/
and b.LTD ='N' and b.TempOnCall='N';
 
/*LTD and tempon call is already excluded from cost distrib table*/
data cost%substr(&hrexdt,3,2);
set cost;
length egrp $3;
FTE_Funding_Derived =  wt_pct*capc_util_lvl/10000;
/*#when Academic staff GFFTE is FTE when fund group =GF*/
if fund_grp_cd='GF' then GFFTE= FTE_Funding_Derived;
else GFFTE=0;
/* GFFTE needs adjustment*/
if Employee_category ne  'Non Acad' then GFFTE=GFFTE;
else if fund='MSXC023271' and org_cd='10070386' then GFFTE= FTE_Funding_Derived*0.5;
else if fund='MSXC023271' then GFFTE= FTE_Funding_Derived*0.7;
else GFFTE=GFFTE;
/*egrp for easy re-structure data for FTE reporting*/
if trim(Employee_category)='Fix Staff' then egrp='fts';
else if trim(Employee_category)='Grad Asst' then egrp='ga';
else if trim(Employee_category)='Tenure Sys' then egrp='ts';
else if trim(Employee_category)='Fix Fac' then egrp='or';

else if  trim(Employee_category)='Cont Staff' then egrp='cs';
else egrp='fy';
run;

%mend;
/*proc sql;
select distinct Employee_category, egrp
from cost;
quit;*/



/*%costdist(hrexdt=20181026,  dtfilter='01OCT2018'D);*/
/**assemble piecees together**/
/*this is for recoding the cert_cd*/
 data certrecode;
	length cert_cd $ 2 DEGREE $ 2;
  input cert_cd DEGREE;
 cards;
07 S0
08 U0
10 V0
12 L0
15 N0
20 Y0
24 M0
28 H0
32 R0
34 F0
36 W0
38 Z0
44 X0
46 I0
50 Q0
65 J0
66 B0
82 C0
86 A0
92 K0
98 D0
99 P0
  ;
run;

/**assemble piecees together**/
%macro assemble(hrexdt, dtfilter, ooiexdt);
  

 /*call primds macro first*/ 
	%primds(hrexdt=&hrexdt.,  dtfilter=&dtfilter.);
 /*call adrank macro*/
	%adtrank(hrexdt=&hrexdt.,  dtfilter=&dtfilter.);
 /*call ooihr macro*/
	%ooihr(ooiexdt=&ooiexdt.);
 /*call primpop macro*/
 /*prim table generated by this macro should be same as the output in HR_Person_Position_Job*/
	%primpop(hrexdt=&hrexdt.,  dtfilter=&dtfilter. , ooiexdt=&ooiexdt.);
 /*addtional derivation for Acad those all based on Gina SQL for Acad*/
	data Acad%substr(&hrexdt,3,2);
	set prim%substr(&hrexdt,3,2);
	if status_cd in ('TENR','TPRO') then STATC='FW';
	else if status_cd ='' and derived_rank_rev ne .  then STATC='FT';
	else if derived_rank_rev ne . and substr(status_cd, length(status_cd),1)='E' then STATC='FT';
	else if status_cd in ('CLIB','CPLB') then STATC='LW';
	else if emp_cat_cd='FLF' then STATC='LT';
	else if status_cd in ('CEXT','CPEX') then STATC='MW';
	else if emp_cat_cd='FEF' then STATC='MT';
	else if status_cd in ('CNSC','CPNS') then STATC='NW';
	else if emp_cat_cd='FNF' then STATC='NT';
	else if status_cd in ('CPSP','CSPC') then STATC='SW';
	else if emp_cat_cd='FSF' then STATC='ST';
	else if status_cd in ('COTR') then STATC='UW';
	else STATC='UT';
	if cul>=100 then FULLPART='1';
	else FULLPART='2';
	EMPPERC=cul;
	endt = datepart(end_date)- datepart(start_date);
	if endt=. then POSMO=.;
	else if endt/365*12>=12 then POSMO=12;
	else POSMO= floor(endt/365*12);
	HANDICAP='';
	if emp_sgrp_cd in ('AN','AO','AQ') then APPTBAS='AN';
	else if emp_sgrp_cd in ('AP','AR') then APPTBAS='AY';
	else APPTBAS='';
	if cul ne . and cul >0 then SAL_FTE= anl_sal/cul*100;
	else SAL_FTE=.;

	if  find(upcase(job_ttl),'POST','i') ge 1 then RANKPC='XPF0';
	else if substr(emp_cat_cd,1,2) in ('FX','FM') and status_cd not in ('TENR','TPRO') and derived_rank_rev=. then RANKPC='0000';
	else if  derived_rank_rev=4 then RANKPC='AA00';
	/*when tenure status no rank give to professor? that's how I read in Gina's*/
	/*those all hardcode, are those still relvant to the current data with new code or text in ttl?*/
	else if  status_cd  in ('TENR','TPRO') and derived_rank_rev=.  then RANKPC='AA00';
	else if  derived_rank_rev=3 then RANKPC='BB00';
	else if  derived_rank_rev=2 then RANKPC='CC00';
	else if  derived_rank_rev=1 then RANKPC='DD00';
	else if  find(upcase(job_ttl),'ADVISOR','i') ge 1 then RANKPC='SSA0';
	else if  find(upcase(job_ttl),'CURRIC','i') ge 1 then RANKPC='SSC0';
	else if  find(upcase(job_ttl),'- OUTREACH','i') ge 1 then RANKPC='SSP0';
	else if  find(upcase(job_ttl),'- RESEARCH','i') ge 1 then RANKPC='SSR0';
	else if  find(job_ttl,'- Teacher','i') ge 1 then RANKPC='SST0';
	else if STATC in ('MT','MW') then RANKPC='MU00';
	else if emp_cat_cd in ('FLF','FLC') then RANKPC='LLD0';
	else if STATC in ('NT','NW') then RANKPC='NEC0';
	else if  find(job_ttl,'Coach','i') ge 1 then RANKPC='VJ00';
	else if  find(job_ttl,'Athlet','i') ge 1 then RANKPC='VJ00';
	else if  find(job_ttl,'Lectur','i') ge 1 then RANKPC='TL00';
	else if  find(job_ttl,'Intern-','i') ge 1 then RANKPC='Ul00';
	else if  find(job_ttl,'Resident-','i') ge 1 then RANKPC='UR00';
	else if  find(job_ttl,'Scholar-','i') ge 1 then RANKPC='UVSV';
	else if  find(job_ttl,'Assistant Instructor','i') ge 1 then RANKPC='TA00';
	else if  find(job_ttl,'Resident-','i') ge 1 then RANKPC='UR00';
	else if  find(job_ttl,'Research Associate','i') ge 1 then RANKPC='URA0';
	else RANKPC='YZ00';
	if status_cd ='' or substr(status_cd, length(status_cd),1)='E' then TBDATE=start_date;
	if status_cd ='' or substr(status_cd, length(status_cd),1)='E' then TEDATE= prim_asgn_end_date;
	if datepart(vstd_date)>='01JAN9999'D then vstdt=.;
	else vstdt=vstd_date;
	if datepart(vstd_date) <= "01JUL%eval( %sysfunc(inputn(%substr(&dtfilter,7,4),4.))+1)"D and vstdt ne . then Retire_Elig=1;
	else Retire_Elig=0;
	if datepart(prim_asgn_end_date) <= "01JUL%eval( %sysfunc(inputn(%substr(&dtfilter,7,4),4.))+1)"D  and prim_asgn_end_date ne '01JAN1900'D then FixTerm_Exp=1;
	else FixTerm_Exp=0;
	if age<35 then AR1=1; else AR1=0;
	if age>=35 and age<40 then AR2=1; else AR2=0;
	if age>=40 and age<45 then AR3=1; else AR3=0;
	if age>=45 and age<50 then AR4=1; else AR4=0;
	if age>=50 and age<55 then AR5=1; else AR5=0;
	if age>=55 and age<60 then AR6=1; else AR6=0;
	if age>=60 and age<65 then AR7=1; else AR7=0;
	if age>=65 then AR8=1; else AR8=0;
	Total=1;
	if trim(Employee_category)='Tenure Sys' then Tenure_System=1; else Tenure_System=0;
	if trim(Employee_category)='Fix Fac' then Fixed_Term_Faculty=1; else Fixed_Term_Faculty=0;
	if trim(Employee_category)='Cont Staff' then Continuing_Staff=1; else Continuing_Staff=0;
	if trim(Employee_category)='Fix Staff' then Fixed_Term_Staff=1; else Fixed_Term_Staff=0;
	Ranked_Faculty= Tenure_System+ Fixed_Term_Faculty;
	if Ranked_Faculty=1 then Unranked_Faculty=0 ; else Unranked_Faculty=1;
	CUC= compress(mau_code || dept_code);
	NCUC= compress(CUC||'00');
	FYEAR = compress((CYEAR-2000)||(CYEAR-1999));
	AYEAR='';
	TYPE=4;                                                                                                                                    
	where emp_sgrp_cd not in ('AT','AS','B4','AC','A1','AW','AD','AX','B7','B8')
	 and emp_cat_cd_1='F'
	 and (asgn_typ_cd is null or asgn_typ_cd ne '4')
	 and anl_sal>0
	 and LTD ='N'
	 and TempOnCall='N';
	run;

/**this generate Acad table for the year the parmater set on .it is the same output as what in [Chen_Test].[dbo].[Non_Aggregated_Acad**/
proc sql ;
create table Acad%substr(&hrexdt,3,2) as 
select  zpid,pers_id,NCUC,CUC,FYEAR,'' as Sem ,CYEAR, AYEAR,ssn as EID,Name,TYPE,STATC,FULLPART,
           EMPPERC,POSMO,EEO_1 as EEO,ETHNIC,GENDER,HANDICAP,CITIZEN,DEGREE,APPTBAS,anl_sal as SALARY,SAL_FTE,
           acum_fte_svc_mths as SERVMOS,RANKPC ,RANK as RPNAME,birth_date as BDATE,cont_svc_date as CDATE ,vstd_date as VDATE,TBDATE,TEDATE,tenure_cont_sys_entr_date as CEDATE,
          Age,New_Hires,Retire_Elig,FixTerm_Exp,AR1,AR2,AR3,AR4,AR5,AR6,AR7,AR8,
           ETHNIC1,ETHNIC2,ETHNIC3,ETHNIC4,ETHNIC5,Total,Tenure_System,Fixed_Term_Faculty,
           Continuing_Staff,Fixed_Term_Staff,Ranked_Faculty,Unranked_Faculty
from Acad%substr(&hrexdt,3,2) a
left join certrecode b
on a.cert_cd=b.cert_cd;
quit;

/*call cost distrbution function */
%costdist(hrexdt=&hrexdt.,  dtfilter=&dtfilter. , ooiexdt=&ooiexdt. );

/*nonAcad FTE*/
proc sql stimer;
create table NAcadFTE%substr(&hrexdt,3,2) as 
select a.*,b.emp_cat_cd,b.emp_cat_cd_1,b.zpid,b.ssn,b.Name,b.job_ttl,b.pay_scale_lvl_cd,b.cul,b.EEO_1,
                             b.ethnc_cd,ETHNIC,ETHNIC1,ETHNIC2,ETHNIC3,ETHNIC4,ETHNIC5,GENDER,CITIZEN
from cost%substr(&hrexdt,3,2) a
inner join prim%substr(&hrexdt,3,2) b
on a.pers_id=b.pers_id and a.pers_nbr=b.pers_nbr 
where b.emp_cat_cd_1='S' and a.anl_sal>0;
quit;

data NAcadFTE%substr(&hrexdt,3,2);
set NAcadFTE%substr(&hrexdt,3,2);

/*need to check the recoding on Employee_Group for NonAcad FTE, what I used here is Gina's program for 2017. It seems to be different recoding for earlier years??*/
if emp_cat_cd='SZ' then Employee_Group='E'; 
else Employee_Group= substr(emp_cat_cd,2,1);

OCCupation_CODE='0';
FTE_Total= FTE_Funding_Derived;
if substr(fund,1,3)='MSG' then FTE_GF= FTE_Total;
else if fund='MSXC023271' and org_cd ne  '10070386' then FTE_GF= FTE_Total*0.7;
else if fund='MSXC023271' then  FTE_GF= FTE_Total*0.5;
else FTE_GF=0;
if substr(fund,1,3)='MSR' then  FTE_ERF= FTE_Total;
else FTE_ERF=0;
if substr(fund,1,3)='MSD' and substr(fund,1,4) ne 'MSDC' then FTE_DES= FTE_Total;
else FTE_DES=0;
if substr(fund,1,3) ne 'MSX' then FTE_AUX=0;
else if fund='MSXC023271' and org_cd ne  '10070386' then FTE_AUX= FTE_Total*0.3;
else if fund='MSXC023271' then FTE_AUX= FTE_Total*0.5;
else FTE_AUX=FTE_Total;
Person_FTE= cul/100;
if substr(fund,1,3) in ('MSP','MSL') then FTE_OTH=FTE_Total;
else  FTE_OTH=0;
if cul>=90 then FULLPART='1';
else FULLPART='2';
TIMe_code=compress(FULLPART||'2');
CUC = compress(mau_code_pay||dept_code_pay);
NCUC = compress(mau_code_pay||dept_code_pay||'00');
FYEAR = compress((CYEAR-2000)||(CYEAR-1999));
Function=4; 
run;


/**this generate NAcadFTE table for the year the parmater set on .it is the same output as what in [Chen_Test].[dbo].[Non_Aggregated_NonAcad_FTE]**/
proc sql stimer;
create table NAcadFTE%substr(&hrexdt,3,2) as 
select NCUC ,CUC,FYear,'' as Sem,CYEAR,'' as AYear,Employee_Group,OCCupation_CODE,FTE_Total,
      FTE_GF,FTE_ERF,FTE_DES,FTE_AUX,FTE_OTH,ZPid,Pers_ID ,ssn as EID,NAME,job_ttl as TITLE,pay_scale_lvl_cd as LEVEL_code ,FULLPART,
      TIMe_code ,EEO_1 as EEO_code,ETHNIC as ETHNIC_code,ETHNIC1 as ETHNIC_code1,ETHNIC2 as ETHNIC_code2,ETHNIC3 as ETHNIC_code3, ETHNIC4 as ETHNIC_code4,ETHNIC5 as ETHNIC_code5,
      gender as GNDR_flag, citizen as CTZN_flag,Person_FTE,fund as Account ,FUND_TYP as FUND ,'' as Category,'' as TYPE,'' as Type_Source,Function
from NAcadFTE%substr(&hrexdt,3,2);
quit;

/*high level FTE*/
proc sql stimer;
create table EmpFTEagg%substr(&hrexdt,3,2) as 
select CYEAR, pers_id,pers_nbr,mau_code_pay,dept_code_pay,egrp,
        Employee_category,FTE_Funding_Derived,fund_grp_cd, sum(FTE_Funding_Derived) as affte, sum(GFFTE) as gffte, sum(FTE_Funding_Derived)- sum(GFFTE) as ngfte
from cost%substr(&hrexdt,3,2)
group by CYEAR, pers_id,pers_nbr,mau_code_pay,dept_code_pay,egrp,
        Employee_category,FTE_Funding_Derived,fund_grp_cd;
quit;

/*transpose dataset*/
proc sort data=EmpFTEagg%substr(&hrexdt,3,2); by CYEAR pers_id pers_nbr mau_code_pay dept_code_pay egrp
        Employee_category FTE_Funding_Derived fund_grp_cd; run;
PROC TRANSPOSE DATA=EmpFTEagg%substr(&hrexdt,3,2) OUT=EmpFTEaggL PREFIX=fte NAME=Source LABEL=Label;
BY CYEAR pers_id pers_nbr mau_code_pay dept_code_pay egrp
        Employee_category FTE_Funding_Derived fund_grp_cd;
VAR affte gffte ngfte;
run;

data EmpFTEaggL;
set EmpFTEaggL;
vargrp = compress(egrp||Source);

/*tranpose back*/
/*wide transpose to the final structure*/
 proc sort data=EmpFTEaggL ;
by CYEAR pers_id pers_nbr mau_code_pay dept_code_pay  Employee_category FTE_Funding_Derived fund_grp_cd; 
run;
proc transpose data=EmpFTEaggL out=EmpFTEagg ;
    by CYEAR pers_id pers_nbr mau_code_pay dept_code_pay  Employee_category FTE_Funding_Derived fund_grp_cd ;
    id vargrp;
    var FTE1;
run;


data EmpFTEagg%substr(&hrexdt,3,2);
set EmpFTEagg;
onraffte= sum(csaffte, ftsaffte);
onrgffte= sum(csgffte,ftsgffte);
onrngffte= sum(csngfte,ftsngfte);
CUC = compress(mau_code_pay||dept_code_pay);
NCUC= compress(mau_code_pay||dept_code_pay||'00');
Current_Assignment='Y';
drop mau_code_pay dept_code_pay;
run;


proc sql stimer;
/*this generate AcadHL table for the year the parmater set on .it is the same output as what in [Chen_Test].[dbo].[HR_HighLevel_FTE_by_Fund_cuc]*/
create table AcadHL%substr(&hrexdt,3,2) as 
select  CUC  , fund_grp_cd as Fund_Group   , pers_nbr as  Personnel_Number  , pers_id as Person_ID , CYear , NCUC , FTE_Funding_Derived , Current_Assignment ,
Employee_category  , gaaffte   , gagffte , tsaffte  , tsgffte  , oraffte  , orgffte   , onraffte   , onrgffte   , csaffte , csgffte , ftsaffte   , ftsgffte 
from EmpFTEagg%substr(&hrexdt,3,2)
where Employee_category ne 'Non Acad';

/*this generate NonAcadHL table for the year the parmater set on .it is the same output as what in [Chen_Test].[dbo].[HR_nacad_FTE_by_Fund_cuc]*/
create table NonAcadHL%substr(&hrexdt,3,2) as 
select  CUC  , fund_grp_cd as Fund_Group   , pers_nbr as  Personnel_Number  , pers_id as Person_ID , CYear , NCUC , FTE_Funding_Derived , Current_Assignment ,
Employee_category  , fyaffte ,fygffte ,fyngfte
from EmpFTEagg%substr(&hrexdt,3,2)
where Employee_category ='Non Acad';
quit;

/*this for val - get all orgs for position*/
proc sql stimer;
create table primorg as 
select distinct pers_id, pers_nbr, org_cd, cyear ,'Y' as owner_org
from prim%substr(&hrexdt,3,2);

create table tenureorg as
select distinct pers_id,pers_nbr,tenure_cont_grant_dept as org_cd,cyear, 'Y' as tenure_org
from prim%substr(&hrexdt,3,2)
where tenure_cont_grant_dept is not null;

create table payorg as 
select distinct a.pers_id, b.pers_nbr, a.ORG_CD_pay as org_cd, a.cyear,'Y' as payer_org
from cost%substr(&hrexdt,3,2) a
inner join prim%substr(&hrexdt,3,2) b
on a.pers_nbr=b.pers_nbr;

create table adorg as 
select b.pers_id,a.pers_nbr, a.org1 as org_cd, %substr(&dtfilter,7,4) as cyear, 'Y' as also_reports_to_org
from adorg%substr(&hrexdt,3,2) a
inner join prim%substr(&hrexdt,3,2) b
on a.pers_nbr=b.pers_nbr;

create table fundpct as 
select distinct pers_nbr,ORG_CD_pay,fund, sum(wt_pct) as wt_pct
from cost%substr(&hrexdt,3,2)
group by pers_nbr,org_cd,fund;

quit;

data allorgs;
set primorg(drop=owner_org) tenureorg(drop=tenure_org) payorg(drop=payer_org) adorg(drop=also_reports_to_org);
run;

proc sort data=allorgs out=allorgs nodupkey; 
by pers_id pers_nbr org_cd cyear; 
run; 

/*all org is similar for Chen_Test.dbo.HR_EMP_ORG_RELA*/
proc sql stimer;
create table allorg%substr(&hrexdt,3,2) as
select distinct a.*, (case when owner_org is null then 'N' else owner_org end) as owner_org,
(case when tenure_org is null then 'N' else tenure_org end) as tenure_org,
(case when also_reports_to_org is null then 'N' else also_reports_to_org end) as also_reports_to_org,
(case when payer_org is null then 'N' else payer_org end) as payer_org, f.fund, f.wt_pct,
o.*, a.cyear
from allorgs a
left join primorg b
on a.pers_id=b.pers_id and a.pers_nbr=b.pers_nbr and a.org_cd=b.org_cd
left join tenureorg c
on a.pers_id=c.pers_id and a.pers_nbr=c.pers_nbr and a.org_cd=c.org_cd
left join adorg d
on a.pers_id=d.pers_id and a.pers_nbr=d.pers_nbr and a.org_cd=d.org_cd
left join payorg e
on a.pers_id=e.pers_id and a.pers_nbr=e.pers_nbr and a.org_cd=e.org_cd
left join fundpct f
on a.pers_nbr=f.pers_nbr and e.org_cd=f.ORG_CD_pay
inner join HROOI%substr(&ooiexdt,3,2) o
on a.org_cd=o.org_code;
quit;
%mend;

/**all info needs for 2018 call the assemble function, dataset will saved as prim18, acad18, etc*/
%assemble(hrexdt=20181026,  dtfilter='01OCT2018'D,ooiexdt=20181026);
/**2017**/
%assemble(hrexdt=20171026,  dtfilter='01OCT2017'D,ooiexdt=20171026);
/**2016**/
%assemble(hrexdt=20161019,  dtfilter='01OCT2016'D,ooiexdt=20161019);
/*2015*/
%assemble(hrexdt=20151027,  dtfilter='01OCT2015'D,ooiexdt=20151027);
/**2014**/
%assemble(hrexdt=20141025,  dtfilter='01OCT2014'D,ooiexdt=20141025);
/**2013**/
%assemble(hrexdt=20131018,  dtfilter='01OCT2013'D,ooiexdt=20131018);
/**2012**/
%assemble(hrexdt=201210,  dtfilter='01OCT2012'D,ooiexdt=201210);
/**2011**/
%assemble(hrexdt=20111025,  dtfilter='01OCT2011'D,ooiexdt=201110);


/******if want to combine multiple years together run the following**
otherwise, just append each new year to the existing*/
/** for HR_Person_Position_Job*/
data HR_Person_Position_Job;
set prim11 prim12 prim13 prim14 prim15 prim16 prim17 prim18;
run;

/**for HR_Person_Cost_Fund_Dist LTD and TempONcall excluded from here*/
data HRCostFundDist;
set cost11 cost12 cost13 cost14 cost15 cost16 cost17 cost18;
run;

/*for Acad*/
data Acad;
set Acad11 Acad12 Acad13 Acad14 Acad15 Acad16 Acad17 Acad18;
run;
/*for NonAcad_FTE*/
data NonAcadFTE;
set NAcadFTE11 NAcadFTE12 NAcadFTE13 NAcadFTE14 NAcadFTE15 NAcadFTE16 NAcadFTE17 NAcadFTE18;
run;
/*for HR_HighLevel_FTE_by_Fund_cuc*/
data AcadHighLevelFTE;
set AcadHL11 AcadHL12 AcadHL13 AcadHL14 AcadHL15 AcadHL16 AcadHL17 AcadHL18;
run;
/*for HR_nacad_FTE_by_Fund_cuc*/
data NonAcadHighLevelFTE;
set NonAcadHL11 NonAcadHL12 NonAcadHL13 NonAcadHL14 NonAcadHL15 NonAcadHL16 NonAcadHL17 NonAcadHL18;
run;

/*for HR_EMP_ORG_RELA */
data EmpOrGRela;
set allorg11 allorg12 allorg13 allorg14 allorg15 allorg16 allorg17 allorg18;
run;






