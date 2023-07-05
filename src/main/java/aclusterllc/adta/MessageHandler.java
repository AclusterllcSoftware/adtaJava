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
    static Connection dbConn = null;
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

    public Map<Integer, String> decodeMessage(byte[] b, int machineId) throws IOException {
        int receivedMessageLength = b.length;

        Map<Integer, String> returnStr = new HashMap<>();

        if(b.length < 8) {
            System.out.println("Wrong message. Buffer Length =" + b.length + " Time=" + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(Calendar.getInstance().getTime()));
            returnStr.put(0, "Wrong message");

        }
        else {
            byte[] bodyBytes = null;
            byte[] headerBytes = Arrays.copyOfRange(b, 0, 8);

            int[] headerParts = decodeHeader(headerBytes);
            int messageId = headerParts[0];
            int messageLength = headerParts[1];
            List<String> returnMsg = new ArrayList<>();

            returnMsg.add(ServerConstants.MESSAGE_IDS.get(messageId));

            if(messageLength > 8) {
                bodyBytes = Arrays.copyOfRange(b, 8, messageLength);
            }

            if(bodyBytes != null) {
                byte[] timestampBytes = Arrays.copyOfRange(bodyBytes, 0, 4);
                long timestampLong = bytesToLong(timestampBytes);

                byte[] dataBytes = Arrays.copyOfRange(bodyBytes, 4, bodyBytes.length);
                if(messageId == 1) {
                    byte[] currentStateBytes = Arrays.copyOfRange(dataBytes, 0, 1);
                    int currentState = (int) bytesToLong(currentStateBytes);
                    byte[] currentModeBytes = Arrays.copyOfRange(dataBytes, 1, dataBytes.length);
                    int currentMode = (int) bytesToLong(currentModeBytes);
                    returnMsg.add("State =" + ServerConstants.SYSTEM_STATES.get(currentState));
                    returnMsg.add("Mode =" + ServerConstants.SYSTEM_MODES.get(currentMode));
                    if (dbWrapper.updateMachineStateMode(currentState, currentMode, machineId)) {
                        /*if(currentState == 2) {
                            String beltStatusXML = "<belts_status unit=\"ips\">31</belts_status>";
                            threeSixtyClient.sendMessage(5, beltStatusXML);
                        }*/
                        returnMsg.add("DB operations done");
                    }
                }
                else if(messageId == 44) {
                    byte[] mailIdBytes = Arrays.copyOfRange(dataBytes, 0, 4);
                    byte[] sensorIdBytes = Arrays.copyOfRange(dataBytes, 4, 8);
                    byte[] sensorStatusBytes = Arrays.copyOfRange(dataBytes, 8, 9);


                    long mailId = bytesToLong(mailIdBytes);
                    int sensorId = (int) bytesToLong(sensorIdBytes);
                    String sensorName = DBCache.getSensorData(machineId, sensorId);
                    int sensorStatus = (int) bytesToLong(sensorStatusBytes);

                    if((sensorId == 1) && (sensorStatus == 1)) {
                        if (dbWrapper.processSensorHits(mailId, machineId)) {
                            DBCache.increaseMySQLProductId(mailId);
                            long productId = DBCache.getMySQLProductId(mailId);
                            if(Integer.parseInt(ServerConstants.configuration.get("threesixty_enable"))==1){
                                String newMailPieceXML = "<new_mailpiece piece_id=\"" + productId + "\" lane=\"" + machineId + "\" />";
                                this.client.threeSixtyClient.sendMessage(4, newMailPieceXML, mailId, machineId);
                            }
                            returnMsg.add("DB operations done");
                        }
                    }

                    long productId = DBCache.getMySQLProductId(mailId);
                    if(Integer.parseInt(ServerConstants.configuration.get("threesixty_enable"))==1){
                        String sensorHitXML = "<sensor_hit piece_id=\"" + productId + "\" lane=\"" + machineId + "\" name=\"" + sensorName + "\">" + sensorStatus + "</sensor_hit >";
                        this.client.threeSixtyClient.sendMessage(3, sensorHitXML, mailId, machineId);
                    }

                    logger.info("Sensor "+ sensorId+" hit=" + sensorStatus + " for ID=" + mailId);
                }
                else if(messageId == 45) {
                    byte[] eventIdBytes = Arrays.copyOfRange(dataBytes, 0, 4);
                    int eventId = (int) bytesToLong(eventIdBytes);
                    String eventName = DBCache.getEventData(machineId, eventId);
                    if(Integer.parseInt(ServerConstants.configuration.get("threesixty_enable"))==1){
                        String eventXML = "<event lane=\""+ machineId +"\" type=\""+ eventName.replaceAll(" ", "_").toLowerCase() +"\">"+ eventName  + "</event>";
                        this.client.threeSixtyClient.sendMessage(2, eventXML, 0, machineId);
                    }
                    /*if (dbWrapper.processEvents(eventId, machineId)) {
                        logger.info("Event "+ eventId +" for machine=" + machineId);
                        returnMsg.add("DB operations done");
                    }*/


                }
                else if(messageId == 48) {
                    byte[] inductIdBytes = Arrays.copyOfRange(dataBytes, 0, 4);
                    int inductId = (int) bytesToLong(inductIdBytes);

                    if (dbWrapper.processPieceInducted(inductId, machineId)) {
                        returnMsg.add("DB operations done");
                    }
                }
                else if(ServerConstants.INPUTS_ERRORS_JAMS_DEVICES.contains(messageId)) {
                    int[] intBitSeq = bitSequenceTranslator(dataBytes, 4);
                    boolean dbOperationDone = false;
                    if(messageId == 2 || messageId == 14) {
                        dbOperationDone = dbWrapper.processInputsDevicesStates(intBitSeq, messageId, machineId);
                        //io_input_states start included by shaiful to insert into io_input_states table
                        if(messageId==2){
                            String query = "INSERT INTO io_input_states (`machine_id`, `input_id`, `state`,`created_at`) VALUES ";
                            List<String> insertList = new ArrayList<>();
                            for(int i=0;i<intBitSeq.length;i++){
                                String insertString = format("(%d, %d, %d,CURRENT_TIMESTAMP())", machineId, i+1, intBitSeq[i]);
                                insertList.add(insertString);
                            }
                            query+=(String.join(", ", insertList)+" ON DUPLICATE KEY UPDATE state=VALUES(state),created_at=VALUES(created_at)");
                            dbHandler.append(query);
                        }
                        //end io_input_states
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
                                dbConn = DataSource.getConnection();
                                String alarmsQuery = String.format("SELECT combo_id FROM active_alarms WHERE machine_id=%d ORDER BY id DESC", machineId);
                                Statement stmt = dbConn.createStatement();
                                ResultSet rs = stmt.executeQuery(alarmsQuery);
                                while (rs.next())
                                {
                                    activeAlarms.put(rs.getString("combo_id"),rs.getString("combo_id"));
                                }
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

                        dbOperationDone = dbWrapper.processAlarms(intBitSeq, messageId, machineId);
                    }
                    if(dbOperationDone) {
                        returnMsg.add("DB operations done");
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
                         if (dbWrapper.processInputsDevicesStates(intByteSeq, messageId, machineId)) {
                            returnMsg.add("DB operations done");
                        }
                    } else if(messageId == 46) {
                         if (dbWrapper.processInputsDevicesStates(intByteSeq, messageId, machineId)) {
                            returnMsg.add("DB operations done");
                        }
                    } else {
                        if (dbWrapper.processBins(intBitSeq, messageId, machineId)) {
                            returnMsg.add("DB operations done");
                        }
                    }
                }
                else if(ServerConstants.SINGLE_STATUS_CHANGE_MESSAGES.contains(messageId)) {
                    if(dataBytes.length == 3) {
                        byte[] idBytes = Arrays.copyOfRange(dataBytes, 0, 2);
                        byte[] stateByte = Arrays.copyOfRange(dataBytes, 2, 3);

                        int idLong = (int) bytesToLong(idBytes);
                        int stateValue = (int) bytesToLong(stateByte);

                        if(messageId == 3 || messageId == 15 || messageId == 43 || messageId == 47) {
                            if(dbWrapper.processSingleInputDeviceState(idLong, stateValue, messageId, machineId)) {
                                returnMsg.add("DB operations done");
                                //io_input_states start included by shaiful to insert into io_input_states table

                                String query =  format("INSERT INTO io_input_states (`machine_id`, `input_id`, `state`,`created_at`) " +
                                        "VALUES (%d, %d, %d,CURRENT_TIMESTAMP()) " +
                                        "ON DUPLICATE KEY UPDATE state=VALUES(state),created_at=VALUES(created_at)",machineId,idLong,stateValue);
                                dbHandler.append(query);
                            }
                        } else {
                            if (dbWrapper.processSingleBinState(idLong, stateValue, messageId, machineId)) {
                                returnMsg.add("DB operations done");
                            }
                        }
                    } else {
                        //System.err.println("Error in single status change message. Message ID = " + messageId);
                        logger.error("Error in single status change message. Message ID = " + messageId + " Machine ID = " + machineId);
                    }
                }
                else if(messageId == 20) {
                    if(dataBytes.length == 21) {
                        //MAIL_ID, Length, Width, Height, Weight, RejectCode
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
                        int rejectCode = (int) bytesToLong(rejectCodeByte);

                        /*if(DBCache.checkExistingProduct(mailId)) {
                            DBCache.increaseMySQLProductId();
                        }*/

                        if (dbWrapper.processDimension(mailId, length, width, height, weight, rejectCode, machineId)) {
                            //DBCache.increaseMySQLProductId();
                            long productId = DBCache.getMySQLProductId(mailId);
                            logger.info("Dimension Processed. ID=" + mailId);
                            if(Integer.parseInt(ServerConstants.configuration.get("threesixty_enable"))==1){

                                String lengthUnit = ServerConstants.configuration.get("threesixty_length_unit");
                                String weightUnit = ServerConstants.configuration.get("threesixty_weight_unit");


                                double lengthForThreeSixty = length;
                                double widthForThreeSixty = width;
                                double heightForThreeSixty = height;

                                if(lengthUnit.equals("in")) {
                                    lengthForThreeSixty = length / 25.4;
                                    widthForThreeSixty = width / 25.4;
                                    heightForThreeSixty = height / 25.4;
                                }

                                DecimalFormat df = new DecimalFormat("#.##");
                                df.setRoundingMode(RoundingMode.UP);

                                double weightForThreeSixty = weight;

                                if(weightUnit.equals("oz")) {
                                    weightForThreeSixty = weight / 28.35;
                                } else if(weightUnit.equals("lb")) {
                                    weightForThreeSixty = weight / 453.6;
                                }

                                String readerXML = "<reader piece_id=\"" + productId + "\" name=\"Length Reader\" lane=\"" + machineId + "\" type=\"LENGTH\" unit=\""+ lengthUnit +"\">" + df.format(lengthForThreeSixty) + "</reader>";
                                this.client.threeSixtyClient.sendMessage(4, readerXML, mailId, machineId);
                                readerXML = "<reader piece_id=\"" + productId + "\" name=\"Width Reader\" lane=\"" + machineId + "\" type=\"WIDTH\" unit=\""+ lengthUnit +"\">" + df.format(widthForThreeSixty) + "</reader>";
                                this.client.threeSixtyClient.sendMessage(4, readerXML, mailId, machineId);
                                readerXML = "<reader piece_id=\"" + productId + "\" name=\"Height Reader\" lane=\"" + machineId + "\" type=\"HEIGHT\" unit=\""+ lengthUnit +"\">" + df.format(heightForThreeSixty) + "</reader>";
                                this.client.threeSixtyClient.sendMessage(4, readerXML, mailId, machineId);
                                readerXML = "<reader piece_id=\"" + productId + "\" name=\"Weight Reader\" lane=\"" + machineId + "\" type=\"WEIGHT\" unit=\""+ weightUnit +"\">" + df.format(weightForThreeSixty) + "</reader>";
                                this.client.threeSixtyClient.sendMessage(4, readerXML, mailId, machineId);
                            }
                            returnMsg.add("DB operations done");
                        }
                    } else {
                        //System.err.println("Error in dimension message.");
                        logger.error("Error in dimension message.");
                    }
                }
                else if(messageId == 21) {
                    byte[] mailIdBytes = Arrays.copyOfRange(dataBytes, 0, 4);
                    long mailId = bytesToLong(mailIdBytes);

                    byte[] numberOfResultsBytes = Arrays.copyOfRange(dataBytes, 4, 6);
                    int numberOfResults = (int) bytesToLong(numberOfResultsBytes);

                    //System.out.println("BARCODE NUM: " + numberOfResults);
                    int barcodeType = 0;

                    //No Question mark only empty
                    String barcodeStringCleaned = "";

                    Map<Integer, Map<String, String>> barcodeTypeWithString = new HashMap<>();
                    Map<Integer, Map<Integer, String>> barcodesFor360 = new HashMap<>();

                    if(numberOfResults > 0) {
                        int startOfCurrentBCBytes = 0;
                        byte[] barcodeFullBytes = Arrays.copyOfRange(dataBytes, 6, dataBytes.length);
                        //System.out.println(barcodeFullBytes);
                        for(int currentBC = 0; currentBC < numberOfResults; currentBC++) {
                            int columnCounter = currentBC + 1;

                            byte[] barcodeTypeByte = Arrays.copyOfRange(dataBytes, 6+startOfCurrentBCBytes, 7+startOfCurrentBCBytes);
                            barcodeType = (int) bytesToLong(barcodeTypeByte);
                            //System.out.println("Type:" + barcodeType);
                            byte[] barcodeLengthBytes = Arrays.copyOfRange(dataBytes, 7+startOfCurrentBCBytes, 9+startOfCurrentBCBytes);
                            int barcodeStringLength = (int) bytesToLong(barcodeLengthBytes);
                            //System.out.println("BC #" + currentBC + " L:" + barcodeStringLength);
                            byte[] barcodeStringBytes = Arrays.copyOfRange(dataBytes, 9+startOfCurrentBCBytes, 9+startOfCurrentBCBytes+barcodeStringLength);

                            String barcodeString = new String(barcodeStringBytes, StandardCharsets.UTF_8);
                            barcodeStringCleaned = barcodeString.replaceAll("\\P{Print}", "");

                            //System.out.println("CODE: " + barcodeStringCleaned);

                            //only limited upto barcode3
                            if(columnCounter < 4) {
                                String barcodeTypeColumnName = "barcode"+ columnCounter + "_type";
                                String barcodeStringColumnName = "barcode"+ columnCounter + "_string";

                                Map<String, String> barcodeAndString = new HashMap<>();
                                barcodeAndString.put(barcodeTypeColumnName, Integer.toString(barcodeType));
                                barcodeAndString.put(barcodeStringColumnName, barcodeStringCleaned);

                                barcodeTypeWithString.put(columnCounter, barcodeAndString);

                                Map<Integer, String> barcodeFor360 = new HashMap<>();
                                barcodeFor360.put(barcodeType, barcodeStringCleaned);

                                barcodesFor360.put(columnCounter, barcodeFor360);
                            }

                            startOfCurrentBCBytes = 3 + startOfCurrentBCBytes + barcodeStringLength;
                        }
                    } else {
                        Map<String, String> barcodeAndString = new HashMap<>();
                        barcodeAndString.put("barcode1_type", Integer.toString(barcodeType));
                        barcodeAndString.put("barcode1_string", barcodeStringCleaned);

                        barcodeTypeWithString.put(1, barcodeAndString);

                        Map<Integer, String> barcodeFor360 = new HashMap<>();
                        barcodeFor360.put(barcodeType, barcodeStringCleaned);

                        barcodesFor360.put(1, barcodeFor360);
                    }

                    if (dbWrapper.processBarcodeResult(mailId, barcodeTypeWithString, numberOfResults, machineId)) {
                        //logger.info("Barcode processed. ID=" + mailId);
                        //System.out.println("mailId:"+mailId+",numberOfResults: "+numberOfResults+",Barcodes: "+barcodeTypeWithString);
                        if(Integer.parseInt(ServerConstants.configuration.get("ingram_enable"))==1) {
                            //Reply message 124 start
                            //messageId == 124 = SortMailpiece; Message length is 16
                            int dest1 = 31;
                            int dest2 = 0;
                            if (numberOfResults > 0) {
                                String barcodes = "'" + barcodeTypeWithString.get(1).get("barcode1_string") + "'";
                                if (numberOfResults > 1) {
                                    barcodes += ",'" + barcodeTypeWithString.get(2).get("barcode2_string") + "'";
                                    if (numberOfResults > 2) {
                                        barcodes += ",'" + barcodeTypeWithString.get(3).get("barcode3_string") + "'";
                                    }
                                }
                                logger.info("Query from  ingram product");
                                try {

                                    Connection dbConn = DataSource.getConnection();
                                    Statement stmt = dbConn.createStatement();
                                    String tbl = "ingram_products";
                                    //String barcode1_string= barcodeTypeWithString.get(1).get("barcode1_string");
                                    String query = String.format("SELECT * FROM %s WHERE carton_id IN(%s);", tbl, barcodes);
                                    logger.info("Barcode Query is" + query);
                                    ResultSet rs = stmt.executeQuery(query);
                                    if (rs.next()) {

                                        dest1 = rs.getInt("dest1");
                                        dest2 = rs.getInt("dest2");
                                        logger.info("Found barcode." + rs.getInt("id") + "-" + rs.getString("carton_id") + "-" + dest1 + "-" + dest2);
                                    } else {
                                        logger.info("Did not Found barcode:" + barcodes);
                                    }
                                    rs.close();
                                    stmt.close();
                                    dbConn.close();
                                } catch (Exception e) {
                                    //e.printStackTrace();
                                    logger.error("Query from  ingram product Exception." + barcodes);
                                    logger.error(e.toString());
                                }
                            }
                            byte[] headerBytesForSend = new byte[]{0, 0, 0, 124, 0, 0, 0, 16};
                            byte[] bodyBytesForSend = new byte[8];
                            //4byte mailId
                            bodyBytesForSend[0] = (byte) (mailId >> 24);
                            bodyBytesForSend[1] = (byte) (mailId >> 16);
                            bodyBytesForSend[2] = (byte) (mailId >> 8);
                            bodyBytesForSend[3] = (byte) (mailId);
                            //2byte dest1
                            bodyBytesForSend[4] = (byte) (dest1 >> 8);//always 0 because less than 256
                            bodyBytesForSend[5] = (byte) (dest1);
                            //2byte dest2
                            bodyBytesForSend[6] = (byte) (dest2 >> 8);//always 0 because less than 256
                            bodyBytesForSend[7] = (byte) (dest2);
                            client.sendBytes(joinTwoBytesArray(headerBytesForSend, bodyBytesForSend));
                            //                        Reply message 124 end
                        }

                        if(barcodesFor360.size() > 0) {
                            for (Map.Entry<Integer, Map<Integer, String>> e : barcodesFor360.entrySet()) {
                                Integer k = e.getKey();
                                Map<Integer, String> v = e.getValue();
                                HashMap<Integer, String> typeAndString = (HashMap<Integer, String>) v;
                                for (Map.Entry<Integer, String> entry : typeAndString.entrySet()) {
                                    Integer bcType = entry.getKey();
                                    String bcString = entry.getValue();
                                    bcString = bcString.replace("'", "");
                                    bcString = bcString.replace("\"", "");
                                    String bcTypeFor360 = ServerConstants.CONVERTED_BARCODES_FOR_360.get(bcType);

                                    long productId = DBCache.getMySQLProductId(mailId);
                                    if(Integer.parseInt(ServerConstants.configuration.get("threesixty_enable"))==1){
                                        String readerXML = "<reader piece_id=\"" + productId + "\" name=\"Barcode Reader\" lane=\"" + machineId + "\" type=\"" + bcTypeFor360 + "\">" + bcString + "</reader>";
                                        this.client.threeSixtyClient.sendMessage(4, readerXML, mailId, machineId);
                                    }
                                }
                            }
                        }
                        returnMsg.add("DB operations done");
                    }
                }
                else if(messageId == 22) {
                    byte[] mailIdBytes = Arrays.copyOfRange(dataBytes, 0, 4);
                    byte[] destinationBytes = Arrays.copyOfRange(dataBytes, 4, 6);
                    byte[] altDestinationBytes = Arrays.copyOfRange(dataBytes, 6, 8);
                    byte[] finalDestinationBytes = Arrays.copyOfRange(dataBytes, 8, 10);
                    byte[] reasonBytes = Arrays.copyOfRange(dataBytes, 10, 11);

                    long mailId = bytesToLong(mailIdBytes);
                    int destination = (int) bytesToLong(destinationBytes);
                    int altDestination = (int) bytesToLong(altDestinationBytes);
                    int finalDestination = (int) bytesToLong(finalDestinationBytes);
                    int reason = (int) bytesToLong(reasonBytes);
                    logger.error("Destination confirmed received. MailId=" + mailId);
                    if (dbWrapper.processConfirmDestination(mailId, destination, altDestination, finalDestination, reason, machineId)) {
                        logger.error("Destination confirmed DB operation done. MailId=" + mailId);
                        long productId = DBCache.getMySQLProductId(mailId);
                        String reasonText = ServerConstants.BIN_REJECT_CODES.get(reason);
                        if(Integer.parseInt(ServerConstants.configuration.get("threesixty_enable"))==1){
                            String pieceStackedXML = "<piece_stacked  piece_id=\"" + productId + "\" bin=\"" + finalDestination + "\" reason=\"" + reasonText + "\">" + finalDestination + "</piece_stacked>";
                            this.client.threeSixtyClient.sendMessage(4, pieceStackedXML, mailId, machineId);
                        }
                        DBCache.removeSQLId(mailId);
                        returnMsg.add("DB operations done");
                    }
                }
                else if(messageId==53){
                    int[] bitSeq = bitSequenceTranslator(dataBytes, 4);
                    String query = "INSERT INTO io_output_states (`machine_id`, `output_id`, `state`,`created_at`) VALUES ";
                    List<String> insertList = new ArrayList<>();
                    for(int i=0;i<bitSeq.length;i++){
                        String insertString = format("(%d, %d, %d,CURRENT_TIMESTAMP())", machineId, i+1, bitSeq[i]);
                        insertList.add(insertString);
                    }
                    query+=(String.join(", ", insertList)+" ON DUPLICATE KEY UPDATE state=VALUES(state),created_at=VALUES(created_at)");
                    dbHandler.append(query);

                }
                else if(messageId==54){
                    long paramId = bytesToLong(Arrays.copyOfRange(dataBytes, 0, 4));
                    long value = bytesToLong(Arrays.copyOfRange(dataBytes, 4, 8));
                    String query = format("UPDATE %s SET value=%d WHERE machine_id=%d AND param_id=%d;","parameters",value,machineId,paramId);
                    dbHandler.append(query);
                }
                else{
                    returnMsg.remove(0);//to remove return message notification from textarea

                }
            }
            else{
                if(messageId == 16) {
                   //System.out.println("Sync Response");
                }
                else if(messageId == 58) {
                    //put in queue
                }
                else if(messageId == 30) {
                   //System.out.println("Ping Response");
                }
            }
            List<Integer> dbWrapperPostMessages=Arrays.asList(11,12,13,49,50,51,52,55,56,57,58);
            if(dbWrapperPostMessages.contains(messageId))
            {
                JSONObject params=new JSONObject();
                params.put("object",this.client);
                if(bodyBytes != null){
                    params.put("bodyBytes",bodyBytes);
                }
                params.put("messageId",messageId);
                dbHandler.append(params);
                //params.put("messageLength",messageLength);//bodyBytes.length
            }

            if(receivedMessageLength > messageLength) {

                byte[] nextMessage = Arrays.copyOfRange(b, messageLength, receivedMessageLength);

                Map<Integer, String> nextMsgValues = decodeMessage(nextMessage, machineId);
                if(nextMsgValues.size() > 0) {
                    for (Map.Entry<Integer, String> entry : nextMsgValues.entrySet()) {
                        Integer k = entry.getKey();
                        String v = entry.getValue();
                        returnStr.put(k, v);
                    }
                }
            }
            if(returnMsg.size()>0){
                returnStr.put(messageId, String.join(", ", returnMsg));
            }
        }

        return returnStr;
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
            logger.error(ex.toString());
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
            logger.error(ex.toString());
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
        switch (messageId){
            case 11:
            case 13:
                if(Integer.parseInt(ServerConstants.configuration.get("threesixty_enable"))==1) {

                    byte[] dataBytes = Arrays.copyOfRange(bodyBytes, 4, bodyBytes.length);
                    byte[] idBytes = Arrays.copyOfRange(dataBytes, 0, 2);
                    byte[] stateByte = Arrays.copyOfRange(dataBytes, 2, 3);

                    int binId = (int) bytesToLong(idBytes);
                    int stateValue = (int) bytesToLong(stateByte);
                    sendSingleBinStatusTo360(binId);
                }
                break;
            case 12:
                if(Integer.parseInt(ServerConstants.configuration.get("threesixty_enable"))==1) {
                    sendAllBinStatusTo360();
                }
                break;
            case 49:
                int motorCount = (int) bytesToLong(Arrays.copyOfRange(bodyBytes, 4, 8));//4,5,6,7
                for(int i=0;i<motorCount;i++){
                    DBCache.motorsCurrentSpeed.put(this.client.machineId+"_"+(i+1),(int) bytesToLong(Arrays.copyOfRange(bodyBytes, 8+i*2, 10+i*2)));
                }
                break;
            case 50:
                if(Integer.parseInt(ServerConstants.configuration.get("threesixty_enable"))==1) {
                    int state = (int) bytesToLong(Arrays.copyOfRange(bodyBytes, 4, 8));//5,6,7,8
                    int location = (int) bytesToLong(Arrays.copyOfRange(bodyBytes, 8, 12));
                    String locationName = DBCache.estop_locations.get(this.client.machineId + "" + location);

                    String xmlMessage = "<estop state=\"" + state + "\">" + locationName + "</estop>";
                    this.client.threeSixtyClient.sendXmlMessage(xmlMessage);
                }

                break;
            case 51:
                if(Integer.parseInt(ServerConstants.configuration.get("threesixty_enable"))==1) {
                    int reason = (int) bytesToLong(Arrays.copyOfRange(bodyBytes, 4, 8));//5,6,7,8
                    ServerConstants.machineStoppedReason = reason;
                    String reasonText = ServerConstants.machine_stopped_reasons.get(reason);
                    String xmlMessage = "<machine_stopped type=\"" + reasonText + "\" />";
                    this.client.threeSixtyClient.sendXmlMessage(xmlMessage);
                }
                break;
            case 52:
                if(Integer.parseInt(ServerConstants.configuration.get("threesixty_enable"))==1) {
                    int speed = (int) bytesToLong(Arrays.copyOfRange(bodyBytes, 4, 8));//5,6,7,8
                    ServerConstants.beltStatusSpeed = speed;
                    String xmlMessage = "<belts_status unit=\"ips\">" + speed + "</belts_status>";
                    this.client.threeSixtyClient.sendXmlMessage(xmlMessage);
                }
                break;
            case 55:
                try {
                    JSONArray resultsJsonArray = new JSONArray();
                    Connection dbConn = DataSource.getConnection();
                    Statement stmt = dbConn.createStatement();
                    String query = String.format("SELECT param_id,value FROM parameters WHERE machine_id=%d", this.client.machineId);
                    ResultSet rs = stmt.executeQuery(query);
                    while (rs.next())
                    {
                        JSONObject row=new JSONObject();
                        row.put("param_id",rs.getInt("param_id"));
                        row.put("value",rs.getInt("value"));
                        resultsJsonArray.put(row);
                    }
                    rs.close();
                    stmt.close();
                    dbConn.close();
                    for(int i=0;i<resultsJsonArray.length();i++){
                        JSONObject row= (JSONObject) resultsJsonArray.get(i);
                        int paramId = row.getInt("param_id");
                        int value = row.getInt("value");
                        byte[] messageBytes= new byte[]{
                                0, 0, 0, 115, 0, 0, 0, 20,0,0,0,0,
                                (byte) (paramId >> 24),(byte) (paramId >> 16),(byte) (paramId >> 8),(byte) (paramId),
                                (byte) (value >> 24),(byte) (value >> 16),(byte) (value >> 8),(byte) (value)
                        };
                        this.client.sendBytes(messageBytes);
                    }
                }
                catch (Exception e) {
                    logger.error(e.toString());
                }
                break;
            case 56:
                int counterCount = (int) bytesToLong(Arrays.copyOfRange(bodyBytes, 4, 8));//4,5,6,7
                for(int i=0;i<counterCount;i++){
                    DBCache.countersCurrentValue.put(this.client.machineId+"_"+(i+1),(int) bytesToLong(Arrays.copyOfRange(bodyBytes, 8+i*4, 12+i*4)));
                }
                break;
            case 57:
                try {
                    byte[] dataBytes = Arrays.copyOfRange(bodyBytes, 4, bodyBytes.length);
                    String query = "UPDATE statistics_oee SET";
                    query+=String.format(" current_state= %d,", (int) bytesToLong(Arrays.copyOfRange(dataBytes, 0, 4)));
                    query+=String.format(" average_tput= %d,", (int) bytesToLong(Arrays.copyOfRange(dataBytes, 4, 8)));
                    query+=String.format(" max_3min_tput= %d,", (int) bytesToLong(Arrays.copyOfRange(dataBytes, 8, 12)));
                    query+=String.format(" successful_divert_packages= %d,", (int) bytesToLong(Arrays.copyOfRange(dataBytes, 12, 16)));
                    query+=String.format(" packages_inducted= %d,", (int) bytesToLong(Arrays.copyOfRange(dataBytes, 16, 20)));
                    query+=String.format(" tot_sec_since_reset= %d,", (int) bytesToLong(Arrays.copyOfRange(dataBytes, 20, 24)));
                    query+=String.format(" tot_sec_estop= %d,", (int) bytesToLong(Arrays.copyOfRange(dataBytes, 24, 28)));
                    query+=String.format(" tot_sec_fault= %d,", (int) bytesToLong(Arrays.copyOfRange(dataBytes, 28, 32)));
                    query+=String.format(" tot_sec_blocked= %d,", (int) bytesToLong(Arrays.copyOfRange(dataBytes, 32, 36)));
                    query+=String.format(" tot_sec_idle= %d,", (int) bytesToLong(Arrays.copyOfRange(dataBytes, 36, 40)));
                    query+=String.format(" tot_sec_init= %d,", (int) bytesToLong(Arrays.copyOfRange(dataBytes, 40, 44)));
                    query+=String.format(" tot_sec_run= %d,", (int) bytesToLong(Arrays.copyOfRange(dataBytes, 44, 48)));
                    query+=String.format(" tot_sec_starved= %d,", (int) bytesToLong(Arrays.copyOfRange(dataBytes, 48, 52)));
                    query+=String.format(" tot_sec_held= %d,", (int) bytesToLong(Arrays.copyOfRange(dataBytes, 52, 56)));
                    query+=String.format(" tot_sec_unconstrained= %d,", (int) bytesToLong(Arrays.copyOfRange(dataBytes, 56, 60)));
                    query+=String.format(" last_record= %d,", dataBytes[60]);
                    query+=" updated_at=NOW()";
                    query+=String.format(" WHERE machine_id=%d ORDER BY id DESC LIMIT 1;", this.client.machineId);
                    Connection connection = DataSource.getConnection();
                    DatabaseHelper.runMultipleQuery(connection,query);
                    connection.close();
                }
                catch (Exception e) {
                    logger.error(e.toString());
                }
                break;
            case 58:
                Runtime r = Runtime.getRuntime();
                try
                {
                    logger.info("Shutting down after 2 seconds.");
                    r.exec("shutdown -s -t 2");
                }
                catch (IOException ex) {
                    logger.error(CommonHelper.getStackTraceString(ex));
                }
                break;
            default:
                System.out.println("Not Handled: "+messageId);
                break;
        }
//        String notificationStr = ServerConstants.MESSAGE_IDS.get(messageId)+" [" + messageId + "]" + "[M:" + this.client.machineId + "]";
//        this.client.notifyListeners("Server", notificationStr);
    }
}
