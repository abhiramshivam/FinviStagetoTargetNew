INSERT INTO ETL_OASIS_DATA_MIG_{PP_TenantName}.WRK_PYMT_ARRANGEMENT_JSON_DATA(CONSUMERID, PA_JSON, ROWIDENTIFIER)
SELECT consumerId,
	REPLACE(REPLACE(PA_JSON, '"false"', 'false'), '"true"', 'true') AS PA_JSON,
	ROW_NUMBER() OVER(ORDER BY CONSUMERID) AS ROW_NUM
FROM (
SELECT  consumerId,PAYMENTARRANGEMENTID,
	JSON_OBJECT(
	    'oasisTransactionSource' VALUE 'Agent Portal',
	    'consumerId' VALUE LOWER(REGEXP_REPLACE(consumerId,'(.{8})(.{4})(.{4})(.{4})(.{12})', '\1-\2-\3-\4-\5')),
	    'isSettlementArrangement' VALUE CASE WHEN ARPAHSETTLAMT > 0 THEN 'true' ELSE 'false' END,
	    'paymentArrangementStatus' VALUE 'ACTIVE',
	    'gracePeriodDays' VALUE 5,
	    'setBrokenPromise' VALUE  CASE WHEN ARPAHBRKRSN IS NOT NULL THEN 'Y' ELSE 'V' END,
	    'frequency' VALUE CASE ARPAHARRFRQ WHEN 'M' THEN 'monthly' WHEN 'S' THEN 'semi-monthly' WHEN 'B' THEN 'biweekly' WHEN 'W' THEN 'weekly' END,
		'fromMigration' VALUE 'true',
  		'entries' VALUE JSON_ARRAYAGG(JSON_OBJECT(
	        'paymentArrangementPaymentStatus' Value 'ACTIVE',
--  		VALUE CASE WHEN ARPAHSPDTE >= SYSDATE THEN 'ACTIVE' ELSE 'APPROVED' END,
	        'locationKey' VALUE LOCATION_KEY,
	        'paymentToken' VALUE NULL,
	        'currencyCode' VALUE 'USD',
	        'amount' VALUE amount,
	        'arrangedDate' VALUE arrangedDate,
	        'billingFirstName' VALUE 'First Name',
	        'billingLastName' VALUE 'Last Name',
	        'paymentMethod' VALUE 'External',
	        'isExternalPayment' VALUE 'true',
	        'lines' VALUE lines returning clob
	        )RETURNING CLOB ) RETURNING CLOB
	) AS PA_JSON
FROM (
	SELECT
		PS.ARPAHSPAMT AS amount,
		TO_CHAR(PS.ARPAHSPDTE, 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS arrangedDate,
		PS.ARPAHSPDTE,
		CUS.UUID AS consumerId,
		NULL AS LOCATION_KEY,
		NULL AS SETID,
		PA.PAYMENTARRANGEMENTID,
		PA.ARPAHSETTLAMT,
		PA.ARPAHBRKRSN,
		PA.ARPAHARRFRQ,
		JSON_ARRAYAGG(JSON_OBJECT( -- 'amount' VALUE PS.ARPAHSPAMT,
			'oasisAccountHolderId' VALUE LOWER(REGEXP_REPLACE(res.uuid,'(.{8})(.{4})(.{4})(.{4})(.{12})', '\1-\2-\3-\4-\5')) ,
			'currencyCode' VALUE 'USD',
			'oasisAccountId' VALUE LOWER(REGEXP_REPLACE(acc.uuid,'(.{8})(.{4})(.{4})(.{4})(.{12})', '\1-\2-\3-\4-\5'))
		)returning clob) AS lines
	FROM ETL_OASIS_DATA_MIG_{PP_TenantName}.ETL_STG_PYMT_SCHEDULE PS
	INNER JOIN ETL_OASIS_DATA_MIG_{PP_TenantName}.ETL_STG_PYMT_ARRANGMENT PA ON PA.PAYMENTARRANGEMENTID = PS.PAYMENTARRANGEMENTID 
		AND PA.PAYMENTARRANGEMENTID NOT IN 
				(SELECT DISTINCT pa.PAYMENTARRANGEMENTID 
				FROM ETL_OASIS_DATA_MIG_{PP_TenantName}.ETL_STG_PYMT_SCHEDULE PS
				INNER JOIN ETL_OASIS_DATA_MIG_{PP_TenantName}.ETL_STG_PYMT_ARRANGMENT PA ON PA.PAYMENTARRANGEMENTID = PS.PAYMENTARRANGEMENTID
				LEFT JOIN COM_FINVI_OASIS_ACCOUNT_{PP_TenantName}.ACCOUNT acc ON acc.migration_source_id = pa.acCountid
				LEFT JOIN COM_FINVI_OASIS_ACCOUNT_{PP_TenantName}.responsible res ON res.accountnum = acc.accountnum 
				LEFT JOIN COM_FINVI_OASIS_ACCOUNT_{PP_TenantName}.responsibleindex resi ON resi.responsibleid = res.responsibleid 
				LEFT JOIN COM_FINVI_OASIS_ACCOUNT_{PP_TenantName}.customer cus ON cus.customerid = resi.customerinfoid
			GROUP BY pa.PAYMENTARRANGEMENTID
			HAVING count(DISTINCT customerid) > 1 )
	INNER JOIN COM_FINVI_OASIS_ACCOUNT_{PP_TenantName}.ACCOUNT acc ON acc.migration_source_id = pa.acCountid
	INNER JOIN COM_FINVI_OASIS_ACCOUNT_{PP_TenantName}.responsible res ON res.accountnum = acc.accountnum 
	INNER JOIN COM_FINVI_OASIS_ACCOUNT_{PP_TenantName}.responsibleindex resi ON resi.responsibleid = res.responsibleid 
	INNER JOIN COM_FINVI_OASIS_ACCOUNT_{PP_TenantName}.customer cus ON cus.customerid = resi.customerinfoid
	GROUP BY PS.ARPAHSPAMT, PS.ARPAHSPDTE, CUS.UUID, PA.PAYMENTARRANGEMENTID, ARPAHSETTLAMT, PA.ARPAHBRKRSN, PA.ARPAHARRFRQ,PS.ARPAHSPDTE
)
GROUP BY
consumerId,PAYMENTARRANGEMENTID,
CASE WHEN ARPAHSETTLAMT > 0 THEN 'true' ELSE 'false' END,
CASE WHEN ARPAHBRKRSN IS NOT NULL THEN 'Y' ELSE 'V' END,
CASE ARPAHARRFRQ WHEN 'M' THEN 'monthly' WHEN 'S' THEN 'semi-monthly' WHEN 'B' THEN 'biweekly' WHEN 'W' THEN 'weekly' END
)