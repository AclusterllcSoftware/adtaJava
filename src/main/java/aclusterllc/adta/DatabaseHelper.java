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
