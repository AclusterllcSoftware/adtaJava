package aclusterllc.adta;

import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.Arrays;

public class DatabaseHelper {
    static Logger logger = LoggerFactory.getLogger(DatabaseHelper.class);
    public static void runMultipleQuery(Connection connection,String query) throws SQLException {
        if(query.length()>0){
            connection.setAutoCommit(false);
            Statement stmt = connection.createStatement();
            stmt.execute(query);
            connection.commit();
            connection.setAutoCommit(true);
            stmt.close();
        }
    }
    public static JSONArray getSelectQueryResults(Connection connection,String query){
        JSONArray resultsJsonArray = new JSONArray();
        try {
            Statement stmt = connection.createStatement();

            ResultSet rs = stmt.executeQuery(query);
            ResultSetMetaData rsMetaData = rs.getMetaData();
            int numColumns = rsMetaData.getColumnCount();
            while (rs.next())
            {
                JSONObject item=new JSONObject();
                for (int i=1; i<=numColumns; i++) {
                    String column_name = rsMetaData.getColumnName(i);
                    item.put(column_name,rs.getString(column_name));
                }
                resultsJsonArray.put(item);
            }
            rs.close();
            stmt.close();
        }
        catch (Exception e) {
            logger.error(CommonHelper.getStackTraceString(e));
        }
        return resultsJsonArray;
    }
    public static int runUpdateQuery(Connection connection,String query) throws SQLException {
        int num_row=0;
        if(query.length()>0){
            connection.setAutoCommit(false);
            Statement stmt = connection.createStatement();
            num_row = stmt.executeUpdate(query);
            connection.commit();
            connection.setAutoCommit(true);
            stmt.close();
        }
        return num_row;
    }
    public static JSONObject getSelectQueryResults(Connection connection,String query,String[] keyColumns){
        JSONObject resultJsonObject = new JSONObject();
        try {
            Statement stmt = connection.createStatement();
            ResultSet rs = stmt.executeQuery(query);
            ResultSetMetaData rsMetaData = rs.getMetaData();
            int numColumns = rsMetaData.getColumnCount();
            while (rs.next())
            {
                JSONObject item=new JSONObject();
                for (int i=1; i<=numColumns; i++) {
                    String column_name = rsMetaData.getColumnName(i);
                    item.put(column_name,rs.getString(column_name));
                }
                String key="";
                for(int i=0;i<keyColumns.length;i++)
                {
                    if(i==0){
                        key=rs.getString(keyColumns[i]);
                    }
                    else{
                        key+=("_"+rs.getString(keyColumns[i]));
                    }
                }
                resultJsonObject.put(key,item);
            }
            rs.close();
            stmt.close();
        }
        catch (Exception e) {
            logger.error(CommonHelper.getStackTraceString(e));
        }
        return resultJsonObject;
    }
    public static JSONArray getActiveAlarms(Connection connection,int machineId) {
        String query = String.format("SELECT *,UNIX_TIMESTAMP(date_active) AS date_active_timestamp FROM active_alarms WHERE machine_id=%d ORDER BY id DESC", machineId);
        return  getSelectQueryResults(connection,query);
    }
    public static JSONObject getAlarmsHistory(Connection connection,int machineId,JSONObject params){
        JSONObject resultJsonObject = new JSONObject();
//        String query = "SELECT *,UNIX_TIMESTAMP(date_active) AS date_active_timestamp,UNIX_TIMESTAMP(date_inactive) AS date_inactive_timestamp FROM active_alarms_history";
//        String totalQuery = "SELECT COUNT(id) as totalRecords FROM active_alarms_history";
        String query = "SELECT ah.*,UNIX_TIMESTAMP(ah.date_active) AS date_active_timestamp,UNIX_TIMESTAMP(ah.date_inactive) AS date_inactive_timestamp";
        query+=",alarms.alarm_class,alarms.location,alarms.description,alarms.variable_name ";
        query+="FROM active_alarms_history as ah";
        query+=" INNER JOIN alarms ON alarms.machine_id=ah.machine_id AND alarms.alarm_id=ah.alarm_id AND alarms.alarm_type=ah.alarm_type ";
        String totalQuery = "SELECT COUNT(ah.id) as totalRecords FROM active_alarms_history as ah INNER JOIN alarms ON alarms.machine_id=ah.machine_id AND alarms.alarm_id=ah.alarm_id AND alarms.alarm_type=ah.alarm_type ";

        query+=String.format(" WHERE ah.machine_id=%d",machineId);
        totalQuery+=String.format(" WHERE ah.machine_id=%d",machineId);
        if(params.has("to_timestamp")){
            query+=String.format(" AND UNIX_TIMESTAMP(date_active)<=%d",params.getInt("to_timestamp"));
            totalQuery+=String.format(" AND UNIX_TIMESTAMP(date_active)<=%d",params.getInt("to_timestamp"));
        }
        if(params.has("from_timestamp")){
            query+=String.format(" AND UNIX_TIMESTAMP(date_active)>=%d",params.getInt("from_timestamp"));
            totalQuery+=String.format(" AND UNIX_TIMESTAMP(date_active)>=%d",params.getInt("from_timestamp"));
        }
        String search1="";
        String search2="";
        if(params.has("search1")){
            search1=params.getString("search1");

        }
        if(params.has("search2")){
            search2=params.getString("search2");

        }
        if((search1.length()>0) && (search2.length()>0)){
            query+=String.format(" AND (location LIKE '%%%s%%' or description LIKE '%%%s%%' or location LIKE '%%%s%%' or description LIKE '%%%s%%' )",search1,search1,search2,search2);
            totalQuery+=String.format(" AND (location LIKE '%%%s%%' or description LIKE '%%%s%%' or location LIKE '%%%s%%' or description LIKE '%%%s%%' )",search1,search1,search2,search2);
        }
        else if(search1.length()>0){
            query+=String.format(" AND (location LIKE '%%%s%%' or description LIKE '%%%s%%')",search1,search1);
            totalQuery+=String.format(" AND (location LIKE '%%%s%%' or description LIKE '%%%s%%')",search1,search1);
        }
        else if(search2.length()>0){
            query+=String.format(" AND (location LIKE '%%%s%%' or description LIKE '%%%s%%')",search2,search2);
            totalQuery+=String.format(" AND (location LIKE '%%%s%%' or description LIKE '%%%s%%')",search2,search2);
        }
        query+=" ORDER BY id DESC";
        if(params.has("per_page")){
            int per_page=params.getInt("per_page");
            if(per_page>0){
                int page=0;
                if(params.has("page")){
                    page=params.getInt("page");
                }
                if(page>0) {
                    query += String.format(" LIMIT %d OFFSET %d", per_page, (page - 1) * per_page);
                }
                else{
                    query+=String.format(" LIMIT %d",per_page);
                }
            }
        }
        query+=";";
        totalQuery+=";";
        JSONArray totalQueryResult=getSelectQueryResults(connection,totalQuery);
        resultJsonObject.put("params", params);
        resultJsonObject.put("totalRecords", totalQueryResult.getJSONObject(0).getInt("totalRecords"));
        resultJsonObject.put("records", getSelectQueryResults(connection,query));
        return resultJsonObject;

    }
    public static JSONObject getDeviceStates(Connection connection,int machineId){
        String query = String.format("SELECT * FROM device_states WHERE machine_id=%d", machineId);
        JSONObject queryResult=getSelectQueryResults(connection,query,new String[] { "machine_id", "device_id"});
        //for main plc manually set status
        JSONObject plc= (JSONObject) queryResult.get(machineId+"_2");
        plc.put("state",ServerConstants.plcConnectStatus.get(machineId));
        queryResult.put(machineId+"_2",plc);
        return queryResult;
    }
    public static JSONObject getInputStates(Connection connection,int machineId){
        String query = String.format("SELECT * FROM input_states WHERE machine_id=%d", machineId);
        return getSelectQueryResults(connection,query,new String[] { "machine_id", "input_id"});
    }
    public static int getMachineMode(Connection connection,int machineId){
        String query = String.format("SELECT machine_mode FROM machines WHERE machine_id=%d", machineId);
        JSONArray modeResult=getSelectQueryResults(connection,query);
        if(modeResult.length()>0){
            return modeResult.getJSONObject(0).getInt("machine_mode");
        }
        return 0;
    }
    public static JSONObject getProductsHistory(Connection connection,int machineId,JSONObject params){
        JSONObject resultJsonObject = new JSONObject();
        String query = "SELECT *,UNIX_TIMESTAMP(created_at) AS created_at_timestamp FROM product_history";
        String totalQuery = "SELECT COUNT(id) as totalRecords FROM product_history";

        query+=String.format(" WHERE machine_id=%d",machineId);
        totalQuery+=String.format(" WHERE machine_id=%d",machineId);
        if(params.has("to_timestamp")){
            query+=String.format(" AND UNIX_TIMESTAMP(created_at)<=%d",params.getInt("to_timestamp"));
            totalQuery+=String.format(" AND UNIX_TIMESTAMP(created_at)<=%d",params.getInt("to_timestamp"));
        }
        if(params.has("from_timestamp")){
            query+=String.format(" AND UNIX_TIMESTAMP(created_at)>=%d",params.getInt("from_timestamp"));
            totalQuery+=String.format(" AND UNIX_TIMESTAMP(created_at)>=%d",params.getInt("from_timestamp"));
        }
        if(params.has("reason")){
            int reason=params.getInt("reason");
            if(reason>-1){
                query+=String.format(" AND reason=%d",params.getInt("reason"));
                totalQuery+=String.format(" AND reason=%d",params.getInt("reason"));
            }
        }
        if(params.has("search_barcode")){
            String search_barcode=params.getString("search_barcode");
            if(search_barcode.length()>0){

                query+=String.format(" AND barcode1_string LIKE \"%%%s%%\"",search_barcode);
                totalQuery+=String.format(" AND barcode1_string LIKE \"%%%s%%\"",search_barcode);
            }
        }

        query+=" ORDER BY id DESC";
        if(params.has("per_page")){
            int per_page=params.getInt("per_page");
            if(per_page>0){
                int page=0;
                if(params.has("page")){
                    page=params.getInt("page");
                }
                if(page>0) {
                    query += String.format(" LIMIT %d OFFSET %d", per_page, (page - 1) * per_page);
                }
                else{
                    query+=String.format(" LIMIT %d",per_page);
                }
            }
        }
        query+=";";
        totalQuery+=";";
        JSONArray totalQueryResult=getSelectQueryResults(connection,totalQuery);
        resultJsonObject.put("params", params);
        resultJsonObject.put("totalRecords", totalQueryResult.getJSONObject(0).getInt("totalRecords"));
        resultJsonObject.put("records", getSelectQueryResults(connection,query));
        return resultJsonObject;

    }

    public static JSONObject getStatisticsData(Connection connection,int machineId,String table,JSONObject params){
        JSONObject resultJsonObject = new JSONObject();
        String query = "SELECT *,UNIX_TIMESTAMP(created_at) AS created_at_timestamp FROM ";
        query+=table;
        query+=String.format(" WHERE machine_id=%d",machineId);
        if(params.has("to_timestamp")){
            query+=String.format(" AND UNIX_TIMESTAMP(created_at)<=%d",params.getInt("to_timestamp"));
        }
        if(params.has("from_timestamp")){
            query+=String.format(" AND UNIX_TIMESTAMP(created_at)>=%d",params.getInt("from_timestamp"));
        }
        if(Arrays.asList("statistics_bins","statistics_bins","statistics_bins").contains(table)){
            if(params.has("bin_id")){
                query+=String.format(" AND bin_id=%d", params.getInt("bin_id"));
            }
        }
        query+=" ORDER BY id DESC";
        if(params.has("per_page")){
            int per_page=params.getInt("per_page");
            if(per_page>0) {
                int page=0;
                if (params.has("page")) {
                    page = params.getInt("page");
                }
                if (page > 0) {
                    query += String.format(" LIMIT %d OFFSET %d", per_page, (page - 1) * per_page);
                } else {
                    query += String.format(" LIMIT %d", per_page);
                }
            }
        }
        query+=";";
        resultJsonObject.put("params", params);
        resultJsonObject.put("records", getSelectQueryResults(connection,query));
        return resultJsonObject;
    }

}
