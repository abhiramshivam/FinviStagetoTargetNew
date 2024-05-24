UPDATE COM_FINVI_OASIS_ACCOUNT_{PP_TenantName}.CUSTOMER cus 
SET cus.DECEASEDINFORMATIONID = 
(
	SELECT dec.ID 
	FROM COM_FINVI_OASIS_ACCOUNT_{PP_TenantName}.DECEASEDINFORMATION dec
	WHERE dec.CUSTOMERID = cus.CUSTOMERID
)
WHERE 
	cus.ISDECEASED = 1
	AND cus.MIGRATION_SOURCE_ID IN (SELECT CONSUMERID FROM ETL_OASIS_DATA_MIG_{PP_TenantName}.ETL_STG_PARTY_INFO par)