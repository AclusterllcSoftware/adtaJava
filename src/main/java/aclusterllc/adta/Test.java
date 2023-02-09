package aclusterllc.adta;

import org.apache.logging.log4j.core.config.Configurator;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
//import java.sql.Connection;
//import java.sql.ResultSet;
//import java.sql.Statement;
//import java.sql.ResultSetMetaData;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.ByteArrayInputStream;
import  java.sql.*;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;


public class Test {
    public static void main(String[] args){
        CustomMap<Long, Long> mailIdtoSQLId = new CustomMap<>();
        mailIdtoSQLId.put(1L, 2L);
        if(mailIdtoSQLId.getKey(1L)!=null){
            System.out.println("exits");
        }
        else
        System.out.println(mailIdtoSQLId.getKey(2L));
    }
    public static void main_5(String[] args){
        //String xmlMessage="<PB id=\"1549\"><heartbeat>3147453399217159</heartbeat></PB>";
        String xmlMessage="<PB id=\"1552\"><sort_piece bin_secondary=\"0\" bin_tertiary=\"0\" piece_id=\"7422591757961142219\" bin=\"1\" group=\"resolved\" open=\"0\" count=\"1\">Routing</sort_piece></PB>";
        try {
            String responseXml="";
            DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
            DocumentBuilder db = dbf.newDocumentBuilder();
            ByteArrayInputStream bis = new ByteArrayInputStream(xmlMessage.getBytes());
            Document doc = db.parse(bis);
            Node pbNode = doc.getDocumentElement();
            if(pbNode.getNodeName().equals("PB")){
                //pbNode.getAttributes().getNamedItem("id");
                Node idNode=pbNode.getAttributes().getNamedItem("id");
                if(idNode!=null){
                    String id=idNode.getTextContent();
                    Node messageNode = pbNode.getFirstChild();
                    if(messageNode.getNodeName().equals("sort_piece")){
                        responseXml = "<PB id=\""+id+"\"><sort_piece>OK</sort_piece></PB>";
                    }

                }
                else{
                    //logger.error("[XML_ERROR] PD id Invalid");
                    System.out.println("[XML_ERROR] PD id Invalid");
                }


            }
            else{
                //logger.error("[XML_ERROR] Message Has no PB");
            }
        } catch (Exception ex) {
            System.out.println(ex.toString());
        }
    }
    public static void insertQuery(){
//        dbConn = DataSource.getConnection();
//        dbConn.setAutoCommit(false);
//        Statement stmt = dbConn.createStatement();
//        int done = stmt.execute(sql,Statement.RETURN_GENERATED_KEYS);
//        ResultSet rs = stmt.getGeneratedKeys();
//        if (rs.next()){
//            System.out.println(rs.getInt(1));
//        }
//
//        //System.err.println(sql + "\n" + done+ "\n");
//        dbConn.commit();
//        dbConn.setAutoCommit(true);
//        stmt.close();
//        dbConn.close(); // connection close
//        logger.info(sql);

//        dbConn = DataSource.getConnection();
//        dbConn.setAutoCommit(false);
//        Statement stmt = dbConn.createStatement();
//        int done = stmt.executeUpdate(sql,Statement.RETURN_GENERATED_KEYS);
//        System.out.println(done);
//        ResultSet rs = stmt.getGeneratedKeys();
//        if (rs.next()){
//            System.out.println(rs.getInt(1));
//        }
//
//        //System.err.println(sql + "\n" + done+ "\n");
//        dbConn.commit();
//        dbConn.setAutoCommit(true);
//        stmt.close();
//        dbConn.close(); // connection close
//        logger.info(sql);


    }
    public static void main_4(String[] args){
        byte[]data={1,2,3,4,5,6,7};
        System.out.println(data.length);
        data= Arrays.copyOfRange(data, 7, data.length);
        System.out.println(data.length);

//        data= Arrays.copyOfRange(data, 3, data.length);
//        System.out.println(data.length);


//        JSONObject client= new JSONObject();
//        byte[]data={1,2,3,4,5,6,7};
//        client.put("buffer",data);
//        client.put("buffer1","shaiful");
//        ConcurrentHashMap<String, JSONObject> ingramClientList= new ConcurrentHashMap<>();
//        ingramClientList.put("first",client);
//        ingramClientList.put("first1",client);
//
//
//        System.out.println(ingramClientList.size());
//        System.out.println(data.length);
//        data=new byte[0];
//        System.out.println(data.length);
//        ConcurrentHashMap<SocketChannel, JSONObject> ingramClientList= new ConcurrentHashMap<>();
//        JSONObject client= new JSONObject();
//        client.put("buffer",new byte[0]);
//        client.put("SocketChannel","shaiful");
//        ingramClientList.put("first",client);
//        System.out.println(ingramClientList.get("first"));
//
//        byte[]data={1,2,3,4,5,6,7};
//        client.put("buffer",data);
//        byte[] buffer =(byte[]) client.get("buffer");
//        System.out.println(buffer.length);

//        byte[] ingramDataBuffer=new byte[0];
//        for(int i=0;i<5;i++){
//            byte[]data={1,2,3,4,5,6,7};
//            if(ingramDataBuffer.length>0){
//                System.out.println("Merging data "+i);
//                ByteArrayOutputStream outputStream = new ByteArrayOutputStream( );
//                try {
//                    outputStream.write( ingramDataBuffer );
//                    outputStream.write( data );
//                    data=outputStream.toByteArray( );
//                } catch (IOException e) {
//                    System.out.println("Merging exception");
//                }
//
//
////                byte[] newData = new byte[ingramDataBuffer.length + data.length];
////                System.arraycopy(ingramDataBuffer, 0, newData, 0, ingramDataBuffer.length);
////                System.arraycopy(data, 0, newData, ingramDataBuffer.length, data.length);
////                data=newData;
//            }
//            System.out.println(i+" data len: "+data.length);
//            ingramDataBuffer= Arrays.copyOfRange(data, i, data.length);
//            System.out.println("Buffer length"+ ingramDataBuffer.length);
//        }


    }
    public static void main_3(String[] args){
        ConcurrentHashMap<String, String> ingramClientList = new ConcurrentHashMap<>();//Currently has no use. may be for future use

        ingramClientList.put("Index-1","Shaiful-1" );
        ingramClientList.put("Index-2","Shaiful-2" );
        for (String key : ingramClientList.keySet()) {
            System.out.println(key);
            String client=ingramClientList.remove(key);
            //String client=ingramClientList.get(key);
            System.out.println(client);
        }



//        System.out.println(ingramClientList.size());
//        System.out.println(ingramClientList.entrySet().iterator().next());
//        System.out.println(ingramClientList.size());


    }
    public static void main_2(String[] args){
        //86400000
        //2147483647
        String ingram_purge_time="15:07";
        int Ingram_Delete_Product_Older_Than=1;
        LocalTime currentTime= LocalTime.now();
        LocalTime scheduleTime= LocalTime.parse(ingram_purge_time);
        int difference=(scheduleTime.toSecondOfDay()- currentTime.toSecondOfDay())*1000;
        System.out.println(difference);
        if(difference<0){
            difference=86400000+difference;
        }
        System.out.println(difference);

        //System.out.println(scheduleTime.compareTo(currentTime)+"----"+currentTime.compareTo(scheduleTime));
        //System.out.println(scheduleTime.toSecondOfDay()+"----"+currentTime.toSecondOfDay());

    }
    public static void main_1(String[] args) {
        Configurator.initialize(null, "./resources/log4j2.xml");
        Logger logger = LoggerFactory.getLogger(ServerForIngram.class);
        //DatabaseHandler databaseHandler = new DatabaseHandler();
        try {
            logger.info("Query started");
            Connection dbConn =DataSource.getConnection();
            Statement stmt = dbConn.createStatement();
            String tbl="ingram_product";
            String carton_id="shaiful 5";
            int dest1=5;
            int dest2=6;
            String query = String.format("INSERT INTO %s (`carton_id`,`dest1`,`dest2`,`created_at`) VALUES( '%s', %d, %d,CURRENT_TIMESTAMP()) ON DUPLICATE KEY UPDATE dest1=VALUES(dest1),dest1=VALUES(dest2),created_at=created_at, updated_at=VALUES(created_at)", tbl,carton_id,dest1,dest2);
            System.out.println(query);
            //String deviceQuery = String.format("SELECT * from %s","events");
            stmt.execute(query);
            //ResultSet rs = stmt.executeQuery(query);

//            JSONArray results=convert(rs);
////            System.out.println(convert(rs).getJSONObject(0).getInt("id"));
//            System.out.println(results);


//            rs.close();
            stmt.close();
            dbConn.close(); // connection close
            System.out.println("Query ended");
            logger.info("Query Ended");

        }
        catch (Exception ex){
            ex.printStackTrace();
        }
        System.out.println("ended");

    }
    public static JSONArray convert( ResultSet rs )
            throws SQLException
    {
        JSONArray json = new JSONArray();
        ResultSetMetaData rsmd = rs.getMetaData();

        while(rs.next()) {
            int numColumns = rsmd.getColumnCount();
            JSONObject obj = new JSONObject();

            for (int i=1; i<numColumns+1; i++) {
                String column_name = rsmd.getColumnName(i);

//                if(rsmd.getColumnType(i)==java.sql.Types.ARRAY){
//                    obj.put(column_name, rs.getArray(column_name));
//                }
//                else if(rsmd.getColumnType(i)==java.sql.Types.BIGINT){
//                    obj.put(column_name, rs.getInt(column_name));
//                }
//                else if(rsmd.getColumnType(i)==java.sql.Types.BOOLEAN){
//                    obj.put(column_name, rs.getBoolean(column_name));
//                }
//                else if(rsmd.getColumnType(i)==java.sql.Types.BLOB){
//                    obj.put(column_name, rs.getBlob(column_name));
//                }
//                else if(rsmd.getColumnType(i)==java.sql.Types.DOUBLE){
//                    obj.put(column_name, rs.getDouble(column_name));
//                }
//                else if(rsmd.getColumnType(i)==java.sql.Types.FLOAT){
//                    obj.put(column_name, rs.getFloat(column_name));
//                }
//                else if(rsmd.getColumnType(i)==java.sql.Types.INTEGER){
//                    obj.put(column_name, rs.getInt(column_name));
//                }
//                else if(rsmd.getColumnType(i)==java.sql.Types.NVARCHAR){
//                    obj.put(column_name, rs.getNString(column_name));
//                }
//                else if(rsmd.getColumnType(i)==java.sql.Types.VARCHAR){
//                    obj.put(column_name, rs.getString(column_name));
//                }
//                else if(rsmd.getColumnType(i)==java.sql.Types.TINYINT){
//                    obj.put(column_name, rs.getInt(column_name));
//                }
//                else if(rsmd.getColumnType(i)==java.sql.Types.SMALLINT){
//                    obj.put(column_name, rs.getInt(column_name));
//                }
//                else if(rsmd.getColumnType(i)==java.sql.Types.DATE){
//                    obj.put(column_name, rs.getDate(column_name));
//                }
//                else if(rsmd.getColumnType(i)==java.sql.Types.TIMESTAMP){
//                    obj.put(column_name, rs.getTimestamp(column_name));
//                }
//                else{
//                    obj.put(column_name, rs.getObject(column_name));
//                }
                obj.put(column_name, rs.getObject(column_name));
            }

            json.put(obj);
        }

        return json;
    }
}
