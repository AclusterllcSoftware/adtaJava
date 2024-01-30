package aclusterllc.adta;

import org.apache.logging.log4j.core.net.ssl.SslConfiguration;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.*;

import static java.lang.String.format;


public class MessageHandler {
    private DBCache dbCache = DBCache.getInstance();
    private final DatabaseWrapper dbWrapper;
    Logger logger = LoggerFactory.getLogger(MessageHandler.class);
    Client client;
    public DatabaseHandler dbHandler;
    public MessageHandler(DatabaseHandler databaseHandler,Client client) {
        dbHandler=databaseHandler;
        dbWrapper = new DatabaseWrapper(databaseHandler);
        this.client=client;
    }

    public byte[] joinTwoBytesArray(byte[] a, byte[] b) {
        byte[] returnArray = new byte[a.length + b.length];

        ByteBuffer buff = ByteBuffer.wrap(returnArray);
        buff.put(a);
        buff.put(b);

        return buff.array();
    }

    public int[] decodeHeader(byte[] encodedHeader) {
        int[] returnArray = {0, 0};
        if(encodedHeader.length == 8) {
            byte[] messageIdBytes = Arrays.copyOfRange(encodedHeader, 0, 4);
            byte[] messageLengthBytes = Arrays.copyOfRange(encodedHeader, 4, encodedHeader.length);

            long messageId = bytesToLong(messageIdBytes);
            long messageLength = bytesToLong(messageLengthBytes);

            returnArray[0] = (int) messageId;
            returnArray[1] = (int) messageLength;
        }

        return returnArray;
    }

    public int getMessageLength(int messageId, int sizeTable) {
        int messageLength;
        switch (messageId) {
            case 2:
                messageLength = 172;
                break;
            case 3:
            case 7:
            case 9:
            case 11:
            case 13:
            case 15:
                messageLength = 15;
                break;
            case 4:
            case 5:
            case 14:
            case 123:
                messageLength = 20;
                break;
            case 6:
            case 8:
            case 10:
            case 12:
                messageLength = 16 + (sizeTable * 4);
                break;
            case 20:
                messageLength = 33;
                break;
            case 21:
                /*It has to be decided later*/
                messageLength = 333;
                break;
            case 22:
                messageLength = 23;
                break;
            case 16:
            case 30:
            case 101:
            case 102:
            case 103:
            case 105:
            case 106:
            case 107:
            case 108:
            case 112:
            case 114:
            case 116:
            case 130:
                messageLength = 8;
                break;
            case 111:
                messageLength = 11;
                break;
            case 120:
                messageLength = 9;
                break;
            case 124:
                messageLength = 16;
                break;
            case 125:
                messageLength = 10;
                break;
            default:
                messageLength = 13;
                break;
        }

        return messageLength;
    }

    public byte[] longToBytes(long x, int byteLength) {
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.putLong(x);
        byte[] wordArray = buffer.array();
        byte[] requiredByteArray = new byte[byteLength];
        int wordArrayLength = wordArray.length;
        int ignoredLength = wordArrayLength - byteLength;
        for (int i=0; i < wordArrayLength; i++) {
           if(i > (ignoredLength - 1)) {
               requiredByteArray[i-ignoredLength] = wordArray[i];
           }
        }

        return requiredByteArray;
    }

    public long bytesToLong(byte[] bytes) {
        byte[] fillArray = new byte[Long.BYTES - bytes.length];
        byte[] longArray = joinTwoBytesArray(fillArray, bytes);
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.put(longArray);
        buffer.flip();//need flip
        return buffer.getLong();
    }

    public byte[] encodeHeader(int messageId, int sizeTable) {
        int messageLength = getMessageLength(messageId, sizeTable);
        byte[] messageIdBytes = longToBytes(messageId, 4);
        byte[] messageLengthBytes = longToBytes(messageLength, 4);

        return joinTwoBytesArray(messageIdBytes, messageLengthBytes);
    }

    public byte[] encodeRequestMessage(int messageId) {

        return encodeHeader(messageId, 0);
    }

    public byte[] encodeRequestMessage(int messageId, int mode, int id) {
        int messageLength = getMessageLength(messageId, 0);

        byte[] headerBytes = encodeHeader(messageId, 0);
        int headerLength = headerBytes.length;
        int bodyLength = messageLength - headerLength;
        byte[] bodyBytes = new byte[bodyLength];

        if(messageId == 120) {
            bodyBytes = longToBytes(Integer.toUnsignedLong(mode), 1);
        } else if(messageId == 123) {
            byte[] deviceBytes = longToBytes(Integer.toUnsignedLong(mode), 4);
            byte[] operationBytes = longToBytes(Integer.toUnsignedLong(id), 4);
            byte[] futureBytes = longToBytes(0, 4);

            byte[] devOpBytes = joinTwoBytesArray(deviceBytes, operationBytes);
            bodyBytes = joinTwoBytesArray(devOpBytes, futureBytes);
        }  else if(messageId == 127) {
            byte[] inductBytes = longToBytes(Integer.toUnsignedLong(mode), 4);
            byte[] modeBytes = longToBytes(Integer.toUnsignedLong(id), 4);
            bodyBytes = joinTwoBytesArray(inductBytes, modeBytes);
        } else if(messageId == 111) {
            byte[] idBytes = longToBytes(Integer.toUnsignedLong(id), 2);
            byte[] modeBytes = longToBytes(Integer.toUnsignedLong(mode), 1);

            bodyBytes = joinTwoBytesArray(idBytes, modeBytes);
        } else if(messageId == 125) {
            bodyBytes = longToBytes(Integer.toUnsignedLong(mode), 2);
        }

        return joinTwoBytesArray(headerBytes, bodyBytes);
    }
    //  this function is for preprocess
    //  params.put("object",this.client);
    //  params.put("bodyBytes",bodyBytes);
    //  params.put("messageId",messageId);
    // params.put("messageLength",messageLength);
    public void processMessage(JSONObject params) {
        try {
            //System.out.println(params);
            int messageId = (int) params.get("messageId");
            int messageLength = (int) params.get("messageLength");
            byte[] bodyBytes = null;
            if (params.has("bodyBytes")) {
                bodyBytes = (byte[]) params.get("bodyBytes");
            }
            String notificationStr =ServerConstants.MESSAGE_IDS.get(messageId)+ " [" + messageId + "]" + "[M:" + this.client.machineId + "]";
            if(bodyBytes != null) {
                byte[] timestampBytes = Arrays.copyOfRange(bodyBytes, 0, 4);
                long timestampLong = bytesToLong(timestampBytes);
                byte[] dataBytes = Arrays.copyOfRange(bodyBytes, 4, bodyBytes.length);
                if(messageId == 1) {
                    byte[] currentStateBytes = Arrays.copyOfRange(dataBytes, 0, 1);
                    int currentState = (int) bytesToLong(currentStateBytes);
                    byte[] currentModeBytes = Arrays.copyOfRange(dataBytes, 1, dataBytes.length);
                    int currentMode = (int) bytesToLong(currentModeBytes);
                    notificationStr+=" State =" + ServerConstants.SYSTEM_STATES.get(currentState);
                    notificationStr+=",Mode =" + ServerConstants.SYSTEM_MODES.get(currentMode);
                    dbWrapper.updateMachineStateMode(currentState, currentMode, this.client.machineId);
                }
                else if(messageId == 45) {
                    byte[] eventIdBytes = Arrays.copyOfRange(dataBytes, 0, 4);
                    int eventId = (int) bytesToLong(eventIdBytes);
                    String eventName = DBCache.getEventData(this.client.machineId, eventId);
                    if(Integer.parseInt(ServerConstants.configuration.get("threesixty_enable"))==1){
                        String eventXML = "<event lane=\""+ this.client.machineId +"\" type=\""+ eventName.replaceAll(" ", "_").toLowerCase() +"\">"+ eventName  + "</event>";
                        this.client.threeSixtyClient.sendMessage(2, eventXML, 0, this.client.machineId);
                    }
                }
                else if(messageId == 48) {
                    byte[] inductIdBytes = Arrays.copyOfRange(dataBytes, 0, 4);
                    int inductId = (int) bytesToLong(inductIdBytes);
                    dbWrapper.processPieceInducted(inductId, this.client.machineId);
                }
                else if(ServerConstants.INPUTS_ERRORS_JAMS_DEVICES.contains(messageId)) {
                    int[] intBitSeq = bitSequenceTranslator(dataBytes, 4);
                    if(messageId == 2 || messageId == 14) {
                        dbWrapper.processInputsDevicesStates(intBitSeq, messageId, this.client.machineId);
                    }
                    else if(messageId == 4 || messageId == 5) {
                        if(Integer.parseInt(ServerConstants.configuration.get("threesixty_enable"))==1){
                            int alarm_type=0;//4=error;
                            int security=1;//4=error
                            if(messageId==5){
                                alarm_type=1;
                                security=2;
                            }
                            Map<String, String> activeAlarms = new HashMap<>();
                            try {
                                Connection dbConn2 = DataSource.getConnection();
                                String alarmsQuery = String.format("SELECT combo_id FROM active_alarms WHERE machine_id=%d ORDER BY id DESC", this.client.machineId);
                                Statement stmt = dbConn2.createStatement();
                                ResultSet rs = stmt.executeQuery(alarmsQuery);
                                while (rs.next())
                                {
                                    activeAlarms.put(rs.getString("combo_id"),rs.getString("combo_id"));
                                }
                                rs.close();
                                stmt.close();
                                dbConn2.close();
                            }
                            catch (SQLException e) {
                                throw new RuntimeException(e);
                            }
                            for(int i=0;i<intBitSeq.length;i++){
                                //if id exists
                                String comboId = String.format("%d%d%d", this.client.machineId, i+1, alarm_type);
                                Map<String, String> singleAlarmData = dbCache.getAlarmData(comboId);
                                if(singleAlarmData.size()>0){
                                    if(intBitSeq[i]==1){
                                        if(activeAlarms.get(comboId)==null){
                                            //<PB id=”1234567890”><exception id=”227” severity=”2” location=”Printer 1”>Out of Ink</exception></PB> lane=\"" + machineId +
                                            String exceptionXML = "<exception  id=\"" + singleAlarmData.get("alarm_id") + "\" severity=\"" + security + "\" location=\"" + singleAlarmData.get("location")+"\">" + singleAlarmData.get("description") + "</exception>";
                                            this.client.threeSixtyClient.sendXmlMessage(exceptionXML);
                                        }
                                    }
                                    //if 0
                                    else{
                                        if(activeAlarms.get(comboId)!=null){
                                            //<exception id=”227” />
                                            String exceptionXML = "<exception  id=\"" + singleAlarmData.get("alarm_id")+ "\" severity=\"" + security + "\" />" ;
                                            this.client.threeSixtyClient.sendXmlMessage(exceptionXML);
                                        }
                                    }
                                }
                            }
                        }
                        dbWrapper.processAlarms(intBitSeq, messageId, this.client.machineId);
                    }
                }
                else if(ServerConstants.MESSAGES_WITH_SIZE_TABLE.contains(messageId)) {
                    byte[] actualDataBytes = Arrays.copyOfRange(dataBytes, 4, dataBytes.length);

                    int[] intBitSeq = bitSequenceTranslator(actualDataBytes, 4);
                    int[] intByteSeq=new int[actualDataBytes.length];
                    for(int i=0;i<intByteSeq.length;i++){
                        intByteSeq[i]=actualDataBytes[i];
                    }
                    //for 42 and 46 it should be byte not bit seq
                    if(messageId == 42) {
                        dbWrapper.processInputsDevicesStates(intByteSeq, messageId, this.client.machineId);
                    } else if(messageId == 46) {
                        dbWrapper.processInputsDevicesStates(intByteSeq, messageId, this.client.machineId);
                    } else {
                        dbWrapper.processBins(intBitSeq, messageId, this.client.machineId);
                    }
                }
                else if(ServerConstants.SINGLE_STATUS_CHANGE_MESSAGES.contains(messageId)) {
                    if(dataBytes.length == 3) {
                        byte[] idBytes = Arrays.copyOfRange(dataBytes, 0, 2);
                        byte[] stateByte = Arrays.copyOfRange(dataBytes, 2, 3);

                        int idLong = (int) bytesToLong(idBytes);
                        int stateValue = (int) bytesToLong(stateByte);

                        if(messageId == 3 || messageId == 15 || messageId == 43 || messageId == 47) {
                            dbWrapper.processSingleInputDeviceState(idLong, stateValue, messageId, this.client.machineId);
                        } else {
                            dbWrapper.processSingleBinState(idLong, stateValue, messageId, this.client.machineId);
                        }
                    } else {
                        logger.error("Error in single status change message. Message ID = " + messageId + " Machine ID = " + this.client.machineId);
                    }
                }
                else if(messageId==53){
                    int[] bitSeq = bitSequenceTranslator(dataBytes, 4);
                    String query = "INSERT INTO io_output_states (`machine_id`, `output_id`, `state`,`created_at`) VALUES ";
                    List<String> insertList = new ArrayList<>();
                    for(int i=0;i<bitSeq.length;i++){
                        String insertString = format("(%d, %d, %d,CURRENT_TIMESTAMP())", this.client.machineId, i+1, bitSeq[i]);
                        insertList.add(insertString);
                    }
                    query+=(String.join(", ", insertList)+" ON DUPLICATE KEY UPDATE state=VALUES(state),created_at=VALUES(created_at)");
                    dbHandler.append(query);
                }


                else{
                    notificationStr="";//do not show in notification area
                }
            }
            else{
                if(messageId == 16) {
                    //this.client.notifyListeners("Server",notificationStr);
                }
                else if(messageId == 58) {
                    //put in queue
                }
                else if(messageId == 30) {
                    this.client.pingPongCounter=0;
                    this.client.notifyListeners("Server", "ping-rec");
                    this.client.logger.info("Ping received from Machine-" +this.client.machineId);
                    notificationStr="";//no need to send
                }
            }
            List<Integer> dbWrapperPostMessages=Arrays.asList(11,12,13,20,21,22,44,49,50,51,52,54,55,56,57,58);
            if(dbWrapperPostMessages.contains(messageId))
            {
                dbHandler.append(params);
            }
            if(notificationStr.length()>0){
                this.client.notifyListeners("Server",notificationStr);
            }
        }
        catch (Exception finalEx) {
            logger.error(CommonHelper.getStackTraceString(finalEx));
        }
    }
    // Char -> Decimal -> Hex
    public static String convertStringToHex(String str) {

        StringBuilder hex = new StringBuilder();

        // loop chars one by one
        for (char temp : str.toCharArray()) {

            // convert char to int, for char `a` decimal 97
            // convert int to hex, for decimal 97 hex 61
            hex.append(Integer.toHexString(temp));
        }

        return hex.toString();
    }

    public int[] bitSequenceTranslator(byte[] dataBytes, int byteHop) {
        int dataBytesLength = dataBytes.length;

        StringBuilder bitSeq = new StringBuilder();

        for(int i = dataBytesLength; i>0; i-=byteHop) {

            int bytesPartTo = i-byteHop;
            if(bytesPartTo < 0) {
                bytesPartTo = 0;
            }

            byte[] dataBytesPart = Arrays.copyOfRange(dataBytes, bytesPartTo, i);
            long bitSeqLong = bytesToLong(dataBytesPart);

            bitSeq.append(longToBinaryString(bitSeqLong));
        }

        String[] strBitSeqArr = bitSeq.toString().split("");

        int bitSeqLength = bitSeq.length();

        int[] intBitSeqArray = new int[bitSeqLength];

        //LSB is bit 0
        for (int i = 0; i < bitSeqLength; i++) {
            intBitSeqArray[bitSeqLength-i-1] = Integer.parseInt(strBitSeqArr[i]);
        }

        return intBitSeqArray;
    }

    public String longToBinaryString(long x) {
        String str = Long.toBinaryString(x);


        int startBlank = 32 - str.length();

        if(startBlank > 0) {
            str = String.join("", Collections.nCopies(startBlank, "0")) + str;
        }

        return str;
    }

    public byte[] getSystemStateMessage() {
        byte[] messageHeader = encodeHeader(1, 0);
        int currentState = 3;
        byte[] currentStateByte = longToBytes(currentState, 1);

        return joinTwoBytesArray(messageHeader, currentStateByte);
    }
    public void sendSingleBinStatusTo360(int binId){
        try {
            Connection dbConn = DataSource.getConnection();
            Statement stmt = dbConn.createStatement();
            String query = "SELECT event_type FROM bin_states WHERE bin_id=" + binId + " AND machine_id=" + this.client.machineId;
            ResultSet rs = stmt.executeQuery(query);
            int removed = 0;
            int value = 0;
            while (rs.next()) {
                int event_type = rs.getInt("event_type");
                if (event_type == 4) {
                    removed = 1;
                }
                else if (event_type == 5) {
                    value = 100;
                }
            }
            rs.close();
            stmt.close();
            dbConn.close(); // connection close
            String xmlMessage = "<bin_status status_id=\"" + binId + "\" removed=\""+removed+"\">" + value + "</bin_status>";
            this.client.threeSixtyClient.sendXmlMessage(xmlMessage);

        }
        catch (Exception ex) {
            logger.error(CommonHelper.getStackTraceString(ex));
        }
    }
    public void sendAllBinStatusTo360(){
        Map<Integer, JSONObject> binStatesData = new HashMap<>();
        try {
            Connection dbConn = DataSource.getConnection();
            Statement stmt = dbConn.createStatement();
            String query = "SELECT bin_id,event_type FROM bin_states WHERE  machine_id=" + this.client.machineId;
            ResultSet rs = stmt.executeQuery(query);
            while (rs.next()) {
                int bin_id = rs.getInt("bin_id");
                int event_type = rs.getInt("event_type");
                JSONObject obj=new JSONObject();
                if (!binStatesData.containsKey(bin_id)) {
                    obj.put("removed",0);
                    obj.put("value",0);
                }
                else{
                    obj=binStatesData.get(bin_id);
                }
                if (event_type == 4) {
                    obj.put("removed",1);
                }
                else if (event_type == 5) {
                    obj.put("value",100);

                }
                binStatesData.put(bin_id, obj);
            }
            rs.close();
            stmt.close();
            dbConn.close();
            for (Map.Entry<Integer,JSONObject> entry : binStatesData.entrySet()) {
                JSONObject obj=entry.getValue();
                String xmlMessage = "<bin_status status_id=\""+entry.getKey()+"\" removed=\""+obj.get("removed")+"\">"+obj.get("value")+"</bin_status>";
                this.client.threeSixtyClient.sendXmlMessage(xmlMessage);
            }
        }
        catch (Exception ex) {
            logger.error(CommonHelper.getStackTraceString(ex));
        }
    }

    //TODO new version by shaiful
//    params.put("object",this.client);
//    params.put("bodyBytes",bodyBytes);
//    params.put("messageId",messageId);
    public void handleMessage(JSONObject params){
        int messageId= (int) params.get("messageId");
        byte[] bodyBytes = null;
        if(params.has("bodyBytes")){
            bodyBytes= (byte[]) params.get("bodyBytes");
        }
        try {
            Connection connection = DataSource.getConnection();
            String notificationStr = ServerConstants.MESSAGE_IDS.get(messageId) + " [" + messageId + "]" + "[M:" + this.client.machineId + "]";
            switch (messageId) {
                case 11:
                case 13: {
                    if (Integer.parseInt(ServerConstants.configuration.get("threesixty_enable")) == 1) {

                        byte[] dataBytes = Arrays.copyOfRange(bodyBytes, 4, bodyBytes.length);
                        byte[] idBytes = Arrays.copyOfRange(dataBytes, 0, 2);
                        byte[] stateByte = Arrays.copyOfRange(dataBytes, 2, 3);

                        int binId = (int) bytesToLong(idBytes);
                        int stateValue = (int) bytesToLong(stateByte);
                        sendSingleBinStatusTo360(binId);
                    }
                    notificationStr="";//Already showing once before
                    break;
                }
                case 12: {
                    if (Integer.parseInt(ServerConstants.configuration.get("threesixty_enable")) == 1) {
                        sendAllBinStatusTo360();
                    }
                    notificationStr="";//Already showing once before
                    break;
                }
                case 20: {
                    byte[] dataBytes = Arrays.copyOfRange(bodyBytes, 4, bodyBytes.length);
                    if (dataBytes.length == 21) {
                        try {

                            byte[] mailIdBytes = Arrays.copyOfRange(dataBytes, 0, 4);
                            byte[] lengthBytes = Arrays.copyOfRange(dataBytes, 4, 8);
                            byte[] widthBytes = Arrays.copyOfRange(dataBytes, 8, 12);
                            byte[] heightBytes = Arrays.copyOfRange(dataBytes, 12, 16);
                            byte[] weightBytes = Arrays.copyOfRange(dataBytes, 16, 20);
                            byte[] rejectCodeByte = Arrays.copyOfRange(dataBytes, 20, 21);

                            long mailId = bytesToLong(mailIdBytes);
                            long length = bytesToLong(lengthBytes);
                            long width = bytesToLong(widthBytes);
                            long height = bytesToLong(heightBytes);
                            long weight = bytesToLong(weightBytes);
                            int reject_code = (int) bytesToLong(rejectCodeByte);
                            String queryCheckProduct = format("SELECT * FROM products WHERE machine_id=%d AND mail_id=%d;", this.client.machineId, mailId);
                            JSONArray queryCheckProductResult = DatabaseHelper.getSelectQueryResults(connection, queryCheckProduct);
                            if (queryCheckProductResult.length() > 0) {
                                JSONObject productInfo = queryCheckProductResult.getJSONObject(0);

                                String query = format("UPDATE products SET length=%d, width=%d, height=%d, weight=%d, reject_code=%d, dimension_at=NOW() WHERE id=%d;",
                                        length, width, height, weight, reject_code, productInfo.getLong("id"));

                                DatabaseHelper.runMultipleQuery(connection, query);
                                logger.info("[PRODUCT][20] Product Updated. MailId=" + mailId);
                            }
                            else {
                                logger.error("[PRODUCT][20] Product not found found. MailId=" + mailId);
                            }
                        }
                        catch (Exception ex) {
                            logger.error("[PRODUCT][20] " + CommonHelper.getStackTraceString(ex));
                        }
                    }
                    else {
                        logger.error("[PRODUCT][20] Data Message length not 21. Length: " + dataBytes.length);
                    }
                    break;
                }
                case 21: {
                    try {
                        byte[] dataBytes = Arrays.copyOfRange(bodyBytes, 4, bodyBytes.length);
                        byte[] mailIdBytes = Arrays.copyOfRange(dataBytes, 0, 4);
                        long mailId = bytesToLong(mailIdBytes);

                        byte[] numberOfResultsBytes = Arrays.copyOfRange(dataBytes, 4, 6);
                        int number_of_results = (int) bytesToLong(numberOfResultsBytes);

                        String queryBarcode = "";
                        int bytePos = 6;
                        JSONObject barCodeInfo = new JSONObject();
                        for (int i = 1; (i < 4) && (i <= number_of_results); i++) {
                            barCodeInfo.put("barcode" + i + "_type", dataBytes[bytePos]);
                            queryBarcode += format("`barcode%s_type`='%s',", i, dataBytes[bytePos]);
                            bytePos++;
                            int barcodeLength = (int) CommonHelper.bytesToLong(Arrays.copyOfRange(dataBytes, bytePos, bytePos + 2));
                            bytePos += 2;
                            String barcode = new String(Arrays.copyOfRange(dataBytes, bytePos, bytePos + barcodeLength), StandardCharsets.UTF_8);
                            //barcode = barcode.replaceAll("\\P{Print}", "");
                            barCodeInfo.put("barcode" + i + "_string", barcode);
                            queryBarcode += format("`barcode%s_string`='%s',", i, barcode);
                            bytePos += barcodeLength;
                        }

                        int valid_read = 1, no_read = 0, multiple_read = 0, no_code = 0;//if number_of_results=1
                        if (number_of_results == 1) {
                            String barcode1_string = barCodeInfo.getString("barcode1_string");
                            switch (barcode1_string) {
                                case "??????????":
                                    no_read = 1;
                                    valid_read = 0;
                                    break;
                                case "9999999999":
                                    multiple_read = 1;
                                    valid_read = 0;
                                    break;
                                case "0000000000":
                                    no_code = 1;
                                    valid_read = 0;
                                    break;
                            }
                        }
                        else {
                            valid_read = 0;
                            if (number_of_results == 0) {
                                no_code = 1;
                            } else {
                                multiple_read = 1;
                            }
                        }
                        JSONObject productInfo = new JSONObject();
                        String query = "";
                        String queryCreateNew = "";
                        String queryCheckProduct = format("SELECT * FROM products WHERE machine_id=%d AND mail_id=%d;", this.client.machineId, mailId);
                        JSONArray queryCheckProductResult = DatabaseHelper.getSelectQueryResults(connection, queryCheckProduct);

                        if (queryCheckProductResult.length() > 0) {
                            productInfo = queryCheckProductResult.getJSONObject(0);
                            query += format("UPDATE products SET %s`number_of_results`='%s', `barcode_at`=now()  WHERE `id`=%d;", queryBarcode, number_of_results, productInfo.getLong("id"));
                        }
                        else {
                            productInfo.put("mail_id", mailId);
                            productInfo.put("machine_id", this.client.machineId);
                            queryCreateNew += format("INSERT INTO products SET %s`number_of_results`='%s',`machine_id`='%s',`mail_id`='%s', `barcode_at`=now();"
                                    , queryBarcode, number_of_results, this.client.machineId, mailId);
                            logger.warn("[PRODUCT][21] Product not found found. Creating New. MailId=" + mailId);
                        }
                        query += format("UPDATE statistics SET total_read=total_read+1, no_read=no_read+%d, no_code=no_code+%d, multiple_read=multiple_read+%d, valid=valid+%d WHERE machine_id=%d ORDER BY id DESC LIMIT 1;", no_read, no_code, multiple_read, valid_read, this.client.machineId);
                        query += format("UPDATE statistics_minutely SET total_read=total_read+1, no_read=no_read+%d, no_code=no_code+%d, multiple_read=multiple_read+%d, valid=valid+%d WHERE machine_id=%d ORDER BY id DESC LIMIT 1;", no_read, no_code, multiple_read, valid_read, this.client.machineId);
                        query += format("UPDATE statistics_hourly SET total_read=total_read+1, no_read=no_read+%d, no_code=no_code+%d, multiple_read=multiple_read+%d, valid=valid+%d WHERE machine_id=%d ORDER BY id DESC LIMIT 1;", no_read, no_code, multiple_read, valid_read, this.client.machineId);
                        query += format("UPDATE statistics_counter SET total_read=total_read+1, no_read=no_read+%d, no_code=no_code+%d, multiple_read=multiple_read+%d, valid=valid+%d WHERE machine_id=%d ORDER BY id DESC LIMIT 1;", no_read, no_code, multiple_read, valid_read, this.client.machineId);
                        connection.setAutoCommit(false);
                        Statement stmt = connection.createStatement();
                        if (queryCreateNew.length() > 0) {
                            stmt.executeUpdate(queryCreateNew, Statement.RETURN_GENERATED_KEYS);
                            ResultSet rs = stmt.getGeneratedKeys();
                            if (rs.next()) {
                                productInfo.put("id", rs.getLong(1));
                            }
                            rs.close();
                        }
                        stmt.execute(query);
                        connection.commit();
                        connection.setAutoCommit(true);
                        stmt.close();
                        logger.info("[PRODUCT][21] Product Updated. MailId=" + mailId);
                    }
                    catch (Exception ex) {
                        logger.error("[PRODUCT][21] " + CommonHelper.getStackTraceString(ex));
                    }
                    break;
                }
                case 22: {
                    try {
                        byte[] dataBytes = Arrays.copyOfRange(bodyBytes, 4, bodyBytes.length);
                        long mail_id = CommonHelper.bytesToLong(Arrays.copyOfRange(dataBytes, 0, 4));
                        int destination = (int) CommonHelper.bytesToLong(Arrays.copyOfRange(dataBytes, 4, 6));
                        int destination_alternate = (int) CommonHelper.bytesToLong(Arrays.copyOfRange(dataBytes, 6, 8));
                        int destination_final = (int) CommonHelper.bytesToLong(Arrays.copyOfRange(dataBytes, 8, 10));
                        int reason = dataBytes[10];

                        JSONObject productInfo = new JSONObject();
                        String query = "";
                        String queryCheckProduct = format("SELECT * FROM products WHERE machine_id=%d AND mail_id=%d;", this.client.machineId, mail_id);
                        JSONArray queryCheckProductResult = DatabaseHelper.getSelectQueryResults(connection, queryCheckProduct);

                        if (queryCheckProductResult.length() > 0) {
                            productInfo = queryCheckProductResult.getJSONObject(0);
                        }
                        else {
                            query = format("INSERT INTO products (`machine_id`, `mail_id`) VALUES (%d, %d);", this.client.machineId, mail_id);
                            query += format("UPDATE %s SET total_read=total_read+1,no_code=no_code+1 WHERE machine_id=%d ORDER BY id DESC LIMIT 1;", "statistics", this.client.machineId);
                            query += format("UPDATE %s SET total_read=total_read+1,no_code=no_code+1 WHERE machine_id=%d ORDER BY id DESC LIMIT 1;", "statistics_hourly", this.client.machineId);
                            query += format("UPDATE %s SET total_read=total_read+1,no_code=no_code+1 WHERE machine_id=%d ORDER BY id DESC LIMIT 1;", "statistics_minutely", this.client.machineId);
                            query += format("UPDATE %s SET total_read=total_read+1,no_code=no_code+1 WHERE machine_id=%d ORDER BY id DESC LIMIT 1;", "statistics_counter", this.client.machineId);
                            logger.warn("[PRODUCT][22] Product not found found. Creating New and Updating Statistics. MailId=" + mail_id);

                            DatabaseHelper.runMultipleQuery(connection, query);
                            JSONArray queryCheckProductResultNew = DatabaseHelper.getSelectQueryResults(connection, queryCheckProduct);

                            if (queryCheckProductResultNew.length() > 0) {
                                productInfo = queryCheckProductResultNew.getJSONObject(0);
                            }
                            else {
                                logger.error("[PRODUCT][22] Failed to create new Product.MailId=" + mail_id);
                                break;
                            }
                        }

                        productInfo.put("destination", destination);
                        productInfo.put("alternate_destination", destination_alternate);
                        productInfo.put("final_destination", destination_final);
                        productInfo.put("reason", reason);
                        String valueFromProductsQuery = "";
                        for (String key : productInfo.keySet()) {
                            valueFromProductsQuery += format("`%s`='%s',", key.equals("id") ? "product_id" : key, productInfo.get(key));
                        }
                        query = format("INSERT INTO product_history SET %s `confirmed_at`=now();", valueFromProductsQuery);
                        query += format("DELETE FROM products WHERE id=%d;", productInfo.getLong("id"));

                        //process short codes
                        JSONObject destBin = null;
                        JSONObject destFinalBin = null;
                        for (int i = 0; i < DBCache.binsInfo.names().length(); i++) {
                            JSONObject bin = (JSONObject) DBCache.binsInfo.get(DBCache.binsInfo.names().getString(i));
                            if (bin.getInt("sort_manager_id") == destination) {
                                destBin = bin;
                            }
                            if (bin.getInt("sort_manager_id") == destination_final) {
                                destFinalBin = bin;
                            }
                        }
                        if (destBin != null && destFinalBin != null) {
                            List<Integer> possibleReasons = new ArrayList<>(Arrays.asList(0, 1, 3, 4, 5, 6, 7, 8, 9, 10, 12, 14, 16, 17, 18, 21));
                            if (possibleReasons.contains(reason)) {
                                String scColumn = "sc" + reason;
                                String recircUpdate = "";
                                String rejectUpdate = "";

                                //statistics
                                if (destFinalBin.getInt("recirc_bin") == 1) {
                                    recircUpdate = " ,recirc=recirc+1";
                                } else if (destFinalBin.getInt("reject_bin") == 1) {
                                    rejectUpdate = " ,reject=reject+1";
                                }

                                query += format("UPDATE %s SET %s=%s+1%s%s WHERE machine_id=%d ORDER BY id DESC LIMIT 1;", "statistics", scColumn, scColumn, recircUpdate, rejectUpdate, this.client.machineId);
                                query += format("UPDATE %s SET %s=%s+1%s%s WHERE machine_id=%d ORDER BY id DESC LIMIT 1;", "statistics_counter", scColumn, scColumn, recircUpdate, rejectUpdate, this.client.machineId);
                                query += format("UPDATE %s SET %s=%s+1%s%s WHERE machine_id=%d ORDER BY id DESC LIMIT 1;", "statistics_hourly", scColumn, scColumn, recircUpdate, rejectUpdate, this.client.machineId);
                                query += format("UPDATE %s SET %s=%s+1%s%s WHERE machine_id=%d ORDER BY id DESC LIMIT 1;", "statistics_minutely", scColumn, scColumn, recircUpdate, rejectUpdate, this.client.machineId);

                                //bin statistics
                                //update short code for all condition destFinalBin
                                {
                                    query += format("UPDATE %s SET %s=%s+1 WHERE machine_id=%d AND bin_id=%d ORDER BY id DESC LIMIT 1;", "statistics_bins", scColumn, scColumn, this.client.machineId, destFinalBin.getInt("bin_id"));
                                    query += format("UPDATE %s SET %s=%s+1 WHERE machine_id=%d AND bin_id=%d ORDER BY id DESC LIMIT 1;", "statistics_bins_counter", scColumn, scColumn, this.client.machineId, destFinalBin.getInt("bin_id"));
                                    query += format("UPDATE %s SET %s=%s+1 WHERE machine_id=%d AND bin_id=%d ORDER BY id DESC LIMIT 1;", "statistics_bins_hourly", scColumn, scColumn, this.client.machineId, destFinalBin.getInt("bin_id"));
                                }
                                if ((destBin.getInt("reject_bin") != 1) && (destBin != destFinalBin)) {
                                    query += format("UPDATE %s SET %s=%s+1%s%s WHERE machine_id=%d AND bin_id=%d ORDER BY id DESC LIMIT 1;", "statistics_bins", scColumn, scColumn, recircUpdate, rejectUpdate, this.client.machineId, destBin.getInt("bin_id"));
                                    query += format("UPDATE %s SET %s=%s+1%s%s WHERE machine_id=%d AND bin_id=%d ORDER BY id DESC LIMIT 1;", "statistics_bins_counter", scColumn, scColumn, recircUpdate, rejectUpdate, this.client.machineId, destBin.getInt("bin_id"));
                                    query += format("UPDATE %s SET %s=%s+1%s%s WHERE machine_id=%d AND bin_id=%d ORDER BY id DESC LIMIT 1;", "statistics_bins_hourly", scColumn, scColumn, recircUpdate, rejectUpdate, this.client.machineId, destBin.getInt("bin_id"));
                                }

                            }
                        }
                        //sc code finished
                        DatabaseHelper.runMultipleQuery(connection, query);
                        logger.info("[PRODUCT][22] Product Updated. MailId=" + mail_id);
                    }
                    catch (Exception ex) {
                        logger.error("[PRODUCT][22] " + CommonHelper.getStackTraceString(ex));
                    }
                    break;
                }
                case 44: {
                    byte[] dataBytes = Arrays.copyOfRange(bodyBytes, 4, bodyBytes.length);
                    byte[] mailIdBytes = Arrays.copyOfRange(dataBytes, 0, 4);
                    byte[] sensorIdBytes = Arrays.copyOfRange(dataBytes, 4, 8);
                    byte[] sensorStatusBytes = Arrays.copyOfRange(dataBytes, 8, 9);
                    long mailId = bytesToLong(mailIdBytes);
                    int sensorId = (int) bytesToLong(sensorIdBytes);
                    String sensorName = DBCache.getSensorData(this.client.machineId, sensorId);
                    int sensorStatus = (int) bytesToLong(sensorStatusBytes);
                    logger.info("[PRODUCT][44] sensorId= " + sensorId + ". sensorStatus=" + sensorStatus + ". MailId=" + mailId);
                    if ((sensorId == 1) && (sensorStatus == 1)) {
                        try {
                            String query = "";
                            String queryOldProduct = format("SELECT * FROM products WHERE machine_id=%d AND mail_id=%d;", this.client.machineId, mailId);
                            JSONArray previousProductInfo = DatabaseHelper.getSelectQueryResults(connection, queryOldProduct);
                            if (previousProductInfo.length() > 0) {
                                long oldProductId = previousProductInfo.getJSONObject(0).getLong("id");
                                logger.info("[PRODUCT][44] Duplicate Product found. MailId=" + mailId + " productId=" + oldProductId);
                                query += format("INSERT INTO overwritten_products SELECT * FROM products WHERE id=%d;", oldProductId);
                                query += format("DELETE FROM products WHERE id=%d;", oldProductId);
                            }

                            connection.setAutoCommit(false);
                            Statement stmt = connection.createStatement();
                            if (query.length() > 0) {
                                stmt.execute(query);
                            }
                            query = format("INSERT INTO products (`machine_id`, `mail_id`) VALUES (%d, %d);", this.client.machineId, mailId);
                            stmt.executeUpdate(query, Statement.RETURN_GENERATED_KEYS);
                            ResultSet rs = stmt.getGeneratedKeys();
                            if (rs.next()) {
                                logger.info("[PRODUCT][44] Inserted New Product MailId=" + mailId + " ProductId:" + rs.getLong(1));
                            }
                            connection.commit();
                            connection.setAutoCommit(true);
                            rs.close();
                            stmt.close();
                        } catch (Exception ex) {
                            logger.error("[PRODUCT][44] " + CommonHelper.getStackTraceString(ex));
                        }
                    }
                    break;
                }
                case 49: {
                    int motorCount = (int) bytesToLong(Arrays.copyOfRange(bodyBytes, 4, 8));//4,5,6,7
                    for (int i = 0; i < motorCount; i++) {
                        DBCache.motorsCurrentSpeed.put(this.client.machineId + "_" + (i + 1), (int) bytesToLong(Arrays.copyOfRange(bodyBytes, 8 + i * 2, 10 + i * 2)));
                    }
                    break;
                }
                case 50: {
                    if (Integer.parseInt(ServerConstants.configuration.get("threesixty_enable")) == 1) {
                        int state = (int) bytesToLong(Arrays.copyOfRange(bodyBytes, 4, 8));//5,6,7,8
                        int location = (int) bytesToLong(Arrays.copyOfRange(bodyBytes, 8, 12));
                        String locationName = DBCache.estop_locations.get(this.client.machineId + "" + location);

                        String xmlMessage = "<estop state=\"" + state + "\">" + locationName + "</estop>";
                        this.client.threeSixtyClient.sendXmlMessage(xmlMessage);
                    }
                    break;
                }
                case 51: {
                    if (Integer.parseInt(ServerConstants.configuration.get("threesixty_enable")) == 1) {
                        int reason = (int) bytesToLong(Arrays.copyOfRange(bodyBytes, 4, 8));//5,6,7,8
                        ServerConstants.machineStoppedReason = reason;
                        String reasonText = ServerConstants.machine_stopped_reasons.get(reason);
                        String xmlMessage = "<machine_stopped type=\"" + reasonText + "\" />";
                        this.client.threeSixtyClient.sendXmlMessage(xmlMessage);
                    }
                    break;
                }
                case 52: {
                    if (Integer.parseInt(ServerConstants.configuration.get("threesixty_enable")) == 1) {
                        int speed = (int) bytesToLong(Arrays.copyOfRange(bodyBytes, 4, 8));//5,6,7,8
                        ServerConstants.beltStatusSpeed = speed;
                        String xmlMessage = "<belts_status unit=\"ips\">" + speed + "</belts_status>";
                        this.client.threeSixtyClient.sendXmlMessage(xmlMessage);
                    }
                    break;
                }
                case 54: {
                    byte[] dataBytes = Arrays.copyOfRange(bodyBytes, 4, bodyBytes.length);
                    long paramId = bytesToLong(Arrays.copyOfRange(dataBytes, 0, 4));
                    long value = bytesToLong(Arrays.copyOfRange(dataBytes, 4, 8));


                    String queryCheckParam = String.format("SELECT * FROM parameters WHERE machine_id=%d AND param_id=%d;", this.client.machineId, paramId);

                    JSONArray queryCheckParamResult = DatabaseHelper.getSelectQueryResults(connection, queryCheckParam);
                    if (queryCheckParamResult.length() > 0) {
                        JSONObject paramInfo = queryCheckParamResult.getJSONObject(0);
                        if(paramInfo.getLong("value")!=value){
                            String query = format("UPDATE %s SET value=%d WHERE id=%d;","parameters",value,paramInfo.getLong("id"));
                            DatabaseHelper.runMultipleQuery(connection, query);
                            CommonHelper.logParamsHistory("\""+paramInfo.getString("description")+"\";\""+paramInfo.getLong("value")+"\";\""+value+"\"");
                        }
                    }
                    else {
                        logger.error("[PARAM][54] Parameter not found found. paramId=" + paramId);
                    }


                    //String query = format("UPDATE %s SET value=%d WHERE machine_id=%d AND param_id=%d;","parameters",value,this.client.machineId,paramId);
                    break;
                }
                case 55: {
                    try {
                        JSONArray resultsJsonArray = new JSONArray();
                        Connection dbConn = DataSource.getConnection();
                        Statement stmt = dbConn.createStatement();
                        String query = String.format("SELECT param_id,value FROM parameters WHERE machine_id=%d", this.client.machineId);
                        ResultSet rs = stmt.executeQuery(query);
                        while (rs.next()) {
                            JSONObject row = new JSONObject();
                            row.put("param_id", rs.getInt("param_id"));
                            row.put("value", rs.getInt("value"));
                            resultsJsonArray.put(row);
                        }
                        rs.close();
                        stmt.close();
                        dbConn.close();
                        for (int i = 0; i < resultsJsonArray.length(); i++) {
                            JSONObject row = (JSONObject) resultsJsonArray.get(i);
                            int paramId = row.getInt("param_id");
                            int value = row.getInt("value");
                            byte[] messageBytes = new byte[]{
                                    0, 0, 0, 115, 0, 0, 0, 20, 0, 0, 0, 0,
                                    (byte) (paramId >> 24), (byte) (paramId >> 16), (byte) (paramId >> 8), (byte) (paramId),
                                    (byte) (value >> 24), (byte) (value >> 16), (byte) (value >> 8), (byte) (value)
                            };
                            this.client.sendBytes(messageBytes);
                        }
                    } catch (Exception ex) {
                        logger.error(CommonHelper.getStackTraceString(ex));
                    }
                    break;
                }
                case 56: {
                    int counterCount = (int) bytesToLong(Arrays.copyOfRange(bodyBytes, 4, 8));//4,5,6,7
                    for (int i = 0; i < counterCount; i++) {
                        DBCache.countersCurrentValue.put(this.client.machineId + "_" + (i + 1), (int) bytesToLong(Arrays.copyOfRange(bodyBytes, 8 + i * 4, 12 + i * 4)));
                    }
                    break;
                }
                case 57: {
                    try {
                        byte[] dataBytes = Arrays.copyOfRange(bodyBytes, 4, bodyBytes.length);
                        String query = "UPDATE statistics_oee SET";
                        query += String.format(" current_state= %d,", (int) bytesToLong(Arrays.copyOfRange(dataBytes, 0, 4)));
                        query += String.format(" average_tput= %d,", (int) bytesToLong(Arrays.copyOfRange(dataBytes, 4, 8)));
                        query += String.format(" max_3min_tput= %d,", (int) bytesToLong(Arrays.copyOfRange(dataBytes, 8, 12)));
                        query += String.format(" successful_divert_packages= %d,", (int) bytesToLong(Arrays.copyOfRange(dataBytes, 12, 16)));
                        query += String.format(" packages_inducted= %d,", (int) bytesToLong(Arrays.copyOfRange(dataBytes, 16, 20)));
                        query += String.format(" tot_sec_since_reset= %d,", (int) bytesToLong(Arrays.copyOfRange(dataBytes, 20, 24)));
                        query += String.format(" tot_sec_estop= %d,", (int) bytesToLong(Arrays.copyOfRange(dataBytes, 24, 28)));
                        query += String.format(" tot_sec_fault= %d,", (int) bytesToLong(Arrays.copyOfRange(dataBytes, 28, 32)));
                        query += String.format(" tot_sec_blocked= %d,", (int) bytesToLong(Arrays.copyOfRange(dataBytes, 32, 36)));
                        query += String.format(" tot_sec_idle= %d,", (int) bytesToLong(Arrays.copyOfRange(dataBytes, 36, 40)));
                        query += String.format(" tot_sec_init= %d,", (int) bytesToLong(Arrays.copyOfRange(dataBytes, 40, 44)));
                        query += String.format(" tot_sec_run= %d,", (int) bytesToLong(Arrays.copyOfRange(dataBytes, 44, 48)));
                        query += String.format(" tot_sec_starved= %d,", (int) bytesToLong(Arrays.copyOfRange(dataBytes, 48, 52)));
                        query += String.format(" tot_sec_held= %d,", (int) bytesToLong(Arrays.copyOfRange(dataBytes, 52, 56)));
                        query += String.format(" tot_sec_unconstrained= %d,", (int) bytesToLong(Arrays.copyOfRange(dataBytes, 56, 60)));
                        query += String.format(" last_record= %d,", dataBytes[60]);
                        query += " updated_at=NOW()";
                        query += String.format(" WHERE machine_id=%d ORDER BY id DESC LIMIT 1;", this.client.machineId);

                        DatabaseHelper.runMultipleQuery(connection, query);

                    } catch (Exception ex) {
                        logger.error(CommonHelper.getStackTraceString(ex));
                    }
                    break;
                }
                case 58: {
                    Runtime r = Runtime.getRuntime();
                    try {
                        logger.info("Shutting down after 2 seconds.");
                        r.exec("shutdown -s -t 2");
                    } catch (IOException ex) {
                        logger.error(CommonHelper.getStackTraceString(ex));
                    }
                    break;
                }
                default:
                    System.out.println("Not Handled: " + messageId);
                    break;
            }
            connection.close();
            if(notificationStr.length()>0){
                this.client.notifyListeners("Server",notificationStr);
            }
        }
        catch (SQLException finalEx) {
            logger.error(CommonHelper.getStackTraceString(finalEx));
        }



    }
}
