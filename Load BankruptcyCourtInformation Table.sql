INSERT INTO COM_FINVI_OASIS_ACCOUNT_{PP_TenantName}.BANKRUPTCYCOURTINFORMATION
(
	UUID,
	NAME,
	DISTRICT,
	DIVISION,
	ADDRESSLINE1,
	ADDRESSLINE2,
	CITY,
	COUNTY,
	STATEID,
	POSTALCODE,
	PHONE,
	WEBSITE,
	MIGRATION_SOURCE_ID	
)
SELECT 
	SYS_GUID() AS UUID,
	PI.ARENLGLCRTNAMEBKR,
	PI.ARENLGLCRTDISTRICTBKR,
	PI.ARENLGLCRTDIVISIONBKR,
	PI.ARENLGLCRTADR1BKR,
	PI.ARENLGLCRTADR2BKR,
	PI.ARENLGLCRTCITYBKR,
	PI.ARENLGLCRTCOUNTYDS,
	S.ID AS STATEID,  
	PI.ARENLGLCRTZIPBKR,
	PI.ARENLGLCRTPHONEBKR,
	PI.ARENLGLCRTURLBKR,
	PI.CONSUMERID AS MIGRATION_SOURCE_ID
FROM 
	ETL_OASIS_DATA_MIG_{PP_TenantName}.ETL_STG_PARTY_INFO  PI
	INNER JOIN COM_FINVI_OASIS_ACCOUNT_{PP_TenantName}.CUSTOMER CN ON PI.CONSUMERID=CN.MIGRATION_SOURCE_ID
	LEFT OUTER JOIN COM_FINVI_OASIS_STATIC_DATA_{PP_TenantName}.STATE S ON PI.ARENLGLCRTSTBKR=S.CODE
WHERE PI.ARENBNKRPT = 'Y' 