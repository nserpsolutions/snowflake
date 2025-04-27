/**
 * @NApiVersion 2.1
 * @NModuleScope SameAccount
 *
 */

define(['N/encode', 'N/crypto/certificate', 'N/https', 'N/error'], (encode, certificate, https, error) => {
    const SF_ACCOUNT = {
        ID: 'account-identifier',
        DC: 'data-center'
    };
    const SF_USER = {
        NAME: 'SNOWFLAKE_USER',
        KEY_FINGERPRINT: 'SHA256:keyfingerprint',
        CERT_ID: 'custcertificate_nse_snowflake'
    }

    const createBearerToken = (sfAccountId, sfUserName, sfKeyFingerprint, sfKeyCertId) => {
        let now = Math.floor(Date.now() / 1000);
        let lifetime = 60;

        const header = encode.convert({
            string: JSON.stringify({
                type: 'JWT',
                alg: 'RS256'
            }),
            inputEncoding: encode.Encoding.UTF_8,
            outputEncoding: encode.Encoding.BASE_64_URL_SAFE,
        }).replace(/=+$/, '');

        const payload = encode.convert({
            string: JSON.stringify({
                iss: `${sfAccountId}.${sfUserName}.${sfKeyFingerprint}`,
                sub: `${sfAccountId}.${sfUserName}`,
                iat: now,
                exp: now + lifetime
            }),
            inputEncoding: encode.Encoding.UTF_8,
            outputEncoding: encode.Encoding.BASE_64_URL_SAFE,
        }).replace(/=+$/, '');
        
        const sfSigner = certificate.createSigner({
            certId: sfKeyCertId,
            algorithm: certificate.HashAlg.SHA256,
        });
        sfSigner.update(`${header}.${payload}`);
        
        const sfSignature = sfSigner.sign({
            outputEncoding: encode.Encoding.BASE_64_URL_SAFE,
        }).replace(/=+$/, '');

        return `${header}.${payload}.${sfSignature}`;
    }

    const getQueryPartitions = (query) => {
        let partitionDetails = {};

        let sfApiResponse = https.post({
            body: JSON.stringify({
                'statement': query,
                'warehouse': 'ENGINEERING_WH',
                'role': 'DATA_ANALYST'
            }),
            url: `https://${SF_ACCOUNT.ID}.${SF_ACCOUNT.DC}.snowflakecomputing.com/api/v2/statements`,
            headers: {
                'Content-Type': 'application/json',
                'Accept': 'application/json',
                'X-Snowflake-Authorization-Token-Type': 'KEYPAIR_JWT',
                'Authorization': `Bearer ${createBearerToken(SF_ACCOUNT.ID, SF_USER.NAME, SF_USER.KEY_FINGERPRINT, SF_USER.CERT_ID)}`,
            }
        });

        if (sfApiResponse.code === 200) {
            let responseBody = JSON.parse(sfApiResponse.body);
            partitionDetails.statementHandle = responseBody.statementHandle;
            partitionDetails.partitionInfo = responseBody.resultSetMetaData.partitionInfo;
            partitionDetails.rowTypes = responseBody.resultSetMetaData.rowType;
        } else {
            log.error(sfApiResponse.code, JSON.stringify(sfApiResponse.body));
            throw error.create({
                name: 'NSE_SF_STATEMENT_QUERY_FAILED',
                message: sfApiResponse.body
            });
        }

        return partitionDetails;
    }

    const getPartitionData = (statementHandle, partitionIndex, rowTypes) => {
        let partitionData = [];
        let sfApiResponse = https.get({
            url: `https://${SF_ACCOUNT.ID}.${SF_ACCOUNT.DC}.snowflakecomputing.com/api/v2/statements/${statementHandle}?partition=${partitionIndex}`,
            headers: {
                'Content-Type': 'application/json',
                'Accept': 'application/json',
                'X-Snowflake-Authorization-Token-Type': 'KEYPAIR_JWT',
                'Authorization': `Bearer ${createBearerToken(SF_ACCOUNT.ID, SF_USER.NAME, SF_USER.KEY_FINGERPRINT, SF_USER.CERT_ID)}`,
            }
        });
        
        
        if (sfApiResponse.code === 200) {
            responseBody = JSON.parse(sfApiResponse.body);
            log.debug('responseBody', responseBody);
            log.debug('responseBody.resultSetMetaData', responseBody.resultSetMetaData);

            responseBody.data.forEach(row => {
                let rowData = {};
                rowTypes.forEach((column, index) => {
                    rowData[column.name] = row[index];
                });
                partitionData.push(rowData);
            });
        } else {
            log.error(sfApiResponse.code, JSON.stringify(sfApiResponse.body));
            throw error.create({
                name: 'NSE_SF_PARTITION_QUERY_FAILED',
                message: sfApiResponse.body
            });
        }

        return partitionData;
    }

    const getQueryResults = (query) => {
        let returnData = [];

        let sfApiResponse = https.post({
            body: JSON.stringify({
                'statement': query,
                'warehouse': 'DATON_WAREHOUSE',
                'role': 'DATA_ANALYST'
            }),
            url: `https://${SF_ACCOUNT.ID}.${SF_ACCOUNT.DC}.snowflakecomputing.com/api/v2/statements`,
            headers: {
                'Content-Type': 'application/json',
                'Accept': 'application/json',
                'X-Snowflake-Authorization-Token-Type': 'KEYPAIR_JWT',
                'Authorization': `Bearer ${createBearerToken(SF_ACCOUNT.ID, SF_USER.NAME, SF_USER.KEY_FINGERPRINT, SF_USER.CERT_ID)}`,
            }
        });

        if (sfApiResponse.code === 200) {
            let responseBody = JSON.parse(sfApiResponse.body);
            let statementHandle = responseBody.statementHandle;
            let rowTypes = responseBody.resultSetMetaData.rowType;

            responseBody.data.forEach(row => {
                let rowData = {};
                rowTypes.forEach((column, index) => {
                    rowData[column.name] = row[index];
                });
                returnData.push(rowData);
            });
            
            let partitionLength = responseBody.resultSetMetaData.partitionInfo.length;
            if (partitionLength > 1) {
                for (let p = 1; p < partitionLength; p++) {
                    sfApiResponse = https.get({
                        url: `https://${SF_ACCOUNT.ID}.${SF_ACCOUNT.DC}.snowflakecomputing.com/api/v2/statements/${statementHandle}?partition=${p}`,
                        headers: {
                            'Content-Type': 'application/json',
                            'Accept': 'application/json',
                            'X-Snowflake-Authorization-Token-Type': 'KEYPAIR_JWT',
                            'Authorization': `Bearer ${createBearerToken(SF_ACCOUNT.ID, SF_USER.NAME, SF_USER.KEY_FINGERPRINT, SF_USER.CERT_ID)}`,
                        }
                    });
                    if (sfApiResponse.code === 200) {
                        responseBody = JSON.parse(sfApiResponse.body);
    
                        responseBody.data.forEach(row => {
                            let rowData = {};
                            rowTypes.forEach((column, index) => {
                                rowData[column.name] = row[index];
                            });
                            returnData.push(rowData);
                        });
                    } else {
                        log.error(sfApiResponse.code, JSON.stringify(sfApiResponse.body));
                        throw error.create({
                            name: 'NSE_SF_PARTITION_QUERY_FAILED',
                            message: sfApiResponse.body
                        });;
                    }
                }
            }
        } else {
            log.error(sfApiResponse.code, JSON.stringify(sfApiResponse.body));
            throw error.create({
                name: 'NSE_SF_STATEMENT_QUERY_FAILED',
                message: sfApiResponse.body
            });
        }

        return returnData;
    }

    return {
        getQueryResults,
        getQueryPartitions,
        getPartitionData
    }
});
