---
title: "HR frozen Related ETLS"
output: github_document
---

```{r setup, include=FALSE}
knitr::opts_chunk$set(echo = TRUE)
```

## Summary

The HR PPS consodliation code is to create the 2 longitudinal file from 2011 to 2018 and the 7 outputs .



## Output and data structure
+	[Chen_Test].[dbo].[Non_Aggregated_Acad] is the new [Non_Aggregated].[dbo].[Acad] with same columns for years since 2011
+	[Chen_Test].[dbo].[Non_Aggregated_NonAcad_FTE] is the new  [Non_Aggregated].[dbo].[NonAcad_FTE] with same columns for years since 2011
+	[Chen_Test].[dbo].[HR_HighLevel_FTE_by_Fund_cuc]  is the new [CUC_Aggregates].[dbo].[HR_HighLevel_FTE_by_Fund_cuc]
+	[Chen_Test].[dbo].[HR_nacad_FTE_by_Fund_cuc]  is the new [CUC_Aggregates].[dbo].[HR_nacad_FTE_by_Fund_cuc]
+	[Chen_Test].[dbo].[HR_Person_Position_Job]: this contains all active employee (emp_cat_cd like ‘F%’,’S%’,’G%’), their position, job, rank, basepay, ethnic, gender etc. The primary keys for this table are CYEAR, pers_id, pers_nbr
+	[Chen_Test].[dbo].[HR_Person_Cost_Fund_Dist] this contains the fund distribution, pay org, FTE_Funding_Derived , etc for all active employee (emp_cat_cd like ‘F%’,’S%’,’G%’) with annual salary >0 . The primary keys for this table are pers_nbr, CYEAR, org_cd_pay, fund
+	[Chen_Test].[dbo].[HR_EMP_ORG_RELA] is to create the all org relationship one row by per pers_nbr and org including also report to orgs. This is used to populate the HR_PPS_Data_Post2010_V for Val

