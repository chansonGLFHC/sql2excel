SELECT DISTINCT pat.PatientID,
	pat.FirstName,
	pat.LastName,
	pat.LANGUAGE,
	pat.DOB,
	pat.MobilePhone,
	appt.AppointmentDate,
	appt.AppointmentStartTime,
	dep.DepartmentName,
	Dep.DepartmentGroup,
	appt.SchedulingProvider,
	apty.AppointmentTypeName
FROM Athena_Stg2.AthenaOneData.Patient pat
JOIN Athena_Stg2.AthenaOneData.Appointment appt
	ON appt.PatientId = pat.PatientID
		AND appt.MDTermDate = '9999-12-31'
		AND appt.AppointmentStatus <> 'Cancelled'
		AND appt.AppointmentDeletedDateTime IS NULL
		AND Appt.AppointmentDate = DATEADD(DAY, 7, CONVERT(DATE, GETDATE()))
		AND appt.AppointmentID = ParentAppointmentId
JOIN Athena_Stg2.AthenaOneData.AppointmentType apty
	ON apty.AppointmentTypeId = appt.AppointmentTypeID
		AND apty.MDTermDate = '9999-12-31'
JOIN Athena_Stg2.AthenaOneData.Department dep
	ON dep.DepartmentID = appt.DepartmentID
		AND dep.MDTermDate = '9999-12-31'
WHERE pat.TestPatientYN <> 'Y'
	AND pat.MDTermDate = '9999-12-31'
	AND pat.PatientID <> '1486071'
	AND apty.AppointmentTypeName LIKE '%(TH)%'
ORDER BY DepartmentGroup,
	DepartmentName,
	appt.AppointmentStartTime
