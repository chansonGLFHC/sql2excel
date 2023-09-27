/************************************************************
Purpose: Generate a list of 36 month panel patients that need to be reassigned
    Patient inclusion logic
		patient is member of a 36 month panel
		patient is currently active
		patient is not deceased
		patient does not have a blocked account
		patient has custom field "PCMH Team Member" set to "Unassigned" or "*Unassigned*"

Change Log
Date        user      description
=========================================================
20230906    ehaddock  Initial Creation of script
20230907	ehaddock  Added C3 flag and C3 patients without an Athena Record

*************************************************************/
SELECT CAST(pp.[PATIENTID] AS VARCHAR) AS PatientID,
	pp.[LAST_NAME] AS Last_Name,
	pp.[FIRST_NAME] AS First_Name,
	pp.[SEX] AS Sex,
	pp.[DOB] AS DOB,
	pp.[RESPONSIBLENAME] AS PCP,
	pp.[PCMHTEAM] AS PtPCMHTeam,
	cp.CustomFieldValue AS PCPPCMHTeam,
	usp.BlockAccountFlag AS PCPBlockedAccount,
	pp.[LOC] AS Site,
	pp.[Department] AS Department,
	pp.[PRIMARYINSURANCENAME] AS PrimaryInsurance,
	pp.[LastApptDate] AS LastApptDate,
	pp.[LastLoc] AS LastLoc,
	pp.[LastApptType] AS LastApptType,
	pp.[NextApptDate] AS NextApptDate,
	pp.[NextLoc] AS NextLoc,
	pp.[NextApptType] AS NextApptType,
	pp.[18MonthPanel] AS [18MonthPanel],
	pp.[36MonthPanel] AS [36MonthPanel],
	CASE 
		WHEN aco.patientid IS NOT NULL
			THEN 'Y'
		END AS C3Flag
FROM [MVGLFHC].[dbo].[GLFHCActivePatientPanel] pp
LEFT JOIN Athena_Stg2.AthenaOneData.Provider prv
	ON prv.mdtermdate = '9999-12-31' AND (prv.ProviderLastName + ', ' + prv.ProviderFirstName) = pp.RESPONSIBLENAME
LEFT JOIN Athena_Stg2.AthenaOneData.UserProfile usp
	ON usp.UserName = prv.ProviderUserName AND usp.MDTermDate = '9999-12-31'
LEFT JOIN [Athena_Stg2].[AthenaOneData].[CustomProviderFields] cp
	ON prv.ProviderId = cp.ProviderId AND cp.CustomFieldName = 'PCMH Team Member' AND cp.MDTermDate = '9999-12-31'
LEFT JOIN [BI].[dbo].[ACOC3Member] aco
	ON CAST(pp.patientid AS VARCHAR) = aco.patientid AND aco.termdate >= getdate()
WHERE pp.[36MonthPanel] = 'Y' AND pp.[CurrentlyActive] = 'Y' AND pp.DECEASEDDATE IS NULL AND (usp.BlockAccountFlag = 'Y' OR cp.CustomFieldValue = 'Unassigned' OR pp.RESPONSIBLENAME = '*Unassigned*')

UNION

SELECT aco.[PATIENTID] AS PatientID,
	aco.[Member_Name_Last] AS Last_Name,
	aco.[Member_Name_First] AS First_Name,
	LEFT(aco.[C3ACOGender], 1) AS Sex,
	aco.[Member_Date_of_Birth] AS DOB,
	NULL AS PCP,
	NULL AS PtPCMHTeam,
	NULL AS PCPPCMHTeam,
	NULL AS PCPBlockedAccount,
	SUBSTRING(aco.[C3ACOPidslName], CHARINDEX('(', aco.[C3ACOPidslName]) + 1, CHARINDEX(')', aco.[C3ACOPidslName]) - CHARINDEX('(', aco.[C3ACOPidslName]) - 1) AS Site,
	NULL AS Department,
	'Medicaid-MA - ACO - Community Care Cooperative' AS PrimaryInsurance,
	NULL AS LastApptDate,
	NULL AS LastLoc,
	NULL AS LastApptType,
	NULL AS NextApptDate,
	NULL AS NextLoc,
	NULL AS NextApptType,
	NULL AS [18MonthPanel],
	NULL AS [36MonthPanel],
	CASE 
		WHEN aco.patientid IS NOT NULL
			THEN 'Y'
		END AS C3Flag
FROM [BI].[dbo].[ACOC3Member] aco
WHERE aco.termdate >= getdate() AND aco.PatientId = 'No GLFHC Match'
ORDER BY PCP
