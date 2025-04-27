/**
 * @NApiVersion 2.1
 * @NScriptType Suitelet
 * @NModuleScope SameAccount
 * @NAmdConfig /SuiteScripts/nse/snowflake/nse_snowflake_config.json
 *
 */
define(['nseSfLib'], (nseSfLib) => {
    const onRequest = (context) => {
        let dateString = new Date(2024,7,12).toISOString().split('T')[0];
        const SF_QUERY = `SELECT column1, column2 FROM datawarehouse.core.tablename WHERE date_column > TO_DATE('${dateString}', 'YYYY-MM-DD');`;

        context.response.write(JSON.stringify(nseSfLib.getQueryResults(SF_QUERY)));
    }

    return {
        onRequest
    }
});