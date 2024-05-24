INSERT INTO COM_FINVI_OASIS_ACCOUNT_{PP_TenantName}.BANKRUPTCYINFORMATION
(
	ID,
	FILEDATE,
	CASENUMBER,
	VERIFICATIONDATE,
	COURTINFORMATIONID, 
	TRUSTEEINFORMATIONID,
	CREATED_ON,
	MODIFIED_ON,
	CREATED_BY,
	MODIFIED_BY,
	MIGRATION_SOURCE_ID
)
SELECT 
	SYS_GUID() AS ID,
	PI.ARENBNKDTE AS FILEDATE,
	PI.ARENBKRCSNUM AS CASENUMBER,
	PI.ARENBKRVERDATEBKR AS VERIFICATIONDATE,
	BCI.COURTINFORMATIONID,
	BTI.TRUSTEEINFORMATIONID,
	sysdate AS CREATED_ON,
	sysdate AS MODIFIED_ON,
    (SELECT ID FROM COM_FINVI_OASIS_ACCOUNT_{PP_TenantName}.user_ WHERE USERNAME = 'etlmigrationuser@finvi.com') AS CREATED_BY,
    (SELECT ID FROM COM_FINVI_OASIS_ACCOUNT_{PP_TenantName}.user_ WHERE USERNAME = 'etlmigrationuser@finvi.com') AS MODIFIED_BY,	
	PI.CONSUMERID AS MIGRATION_SOURCE_ID
FROM 
	ETL_OASIS_DATA_MIG_{PP_TenantName}.ETL_STG_PARTY_INFO PI
	INNER JOIN COM_FINVI_OASIS_ACCOUNT_{PP_TenantName}.CUSTOMER CN ON PI.CONSUMERID = CN.MIGRATION_SOURCE_ID
	LEFT OUTER JOIN COM_FINVI_OASIS_ACCOUNT_{PP_TenantName}.BANKRUPTCYTRUSTEEINFORMATION BTI ON PI.CONSUMERID = BTI.MIGRATION_SOURCE_ID
	LEFT OUTER JOIN COM_FINVI_OASIS_ACCOUNT_{PP_TenantName}.BANKRUPTCYCOURTINFORMATION BCI ON PI.CONSUMERID = BCI.MIGRATION_SOURCE_ID
WHERE PI.ARENBNKRPT = 'Y'	