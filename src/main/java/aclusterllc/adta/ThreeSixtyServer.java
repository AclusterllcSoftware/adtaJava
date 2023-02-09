package aclusterllc.adta;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.json.JSONObject;
import org.w3c.dom.Document;
import org.w3c.dom.Node;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.ByteArrayInputStream;
import java.io.IOException;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;


public class ThreeSixtyServer implements Runnable {

    private Thread worker;


    private final List<ServerListener> serverListeners = new ArrayList<>();
    private final List<ClientListListener> clientListListeners = new ArrayList<>();

    ConcurrentHashMap<SocketChannel, JSONObject> connectedClientList = new ConcurrentHashMap<>();
    public CustomMap<Integer, Client> cmClients  = new CustomMap<>();
    Selector selector;
    ServerSocketChannel serverSocketChannel;
    ByteBuffer buffer = ByteBuffer.allocate(10240000);
    private final ServerDBHandler serverDBHandler = new ServerDBHandler();
    Logger logger = LoggerFactory.getLogger(ThreeSixtyServer.class);
    public ThreeSixtyClient threeSixtyClient;
    public ThreeSixtyServer(ThreeSixtyClient threeSixtyClient) {
        this.threeSixtyClient=threeSixtyClient;
    }
    public void start(){
        worker = new Thread(this);
        try {
            selector = Selector.open();
            serverSocketChannel = ServerSocketChannel.open();
            serverSocketChannel.bind(new InetSocketAddress(ServerConstants.configuration.get("threesixty_server_ip"), Integer.parseInt(ServerConstants.configuration.get("threesixty_server_port"))));
            serverSocketChannel.configureBlocking(false);
            serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT);
            worker.start();
        } catch (IOException e) {
            logger.error(ServerConstants.configuration.get("threesixty_server_ip") + " - " + Integer.parseInt(ServerConstants.configuration.get("threesixty_server_port")));
            logger.error(e.toString());
        }
    }
//    public void interrupt() {
//		running.set(false);
//		notifyListeners("Self", "", "360 Disconnected from Java server");
//        worker.interrupt();
//    }

    public void run() {

        logger.info("360 Server started: "+ServerConstants.configuration.get("threesixty_server_ip") + " - " + Integer.parseInt(ServerConstants.configuration.get("threesixty_server_port")));

        while (true) {
            //System.out.println("Socket running");
            try {
                System.out.println("[360 Server] waiting for event");
                selector.select();
                Iterator<SelectionKey> iterator = selector.selectedKeys().iterator();
                while (iterator.hasNext()) {
                    SelectionKey key = iterator.next();
                    iterator.remove();
                    if(!key.isValid()){
                        continue;
                    }
                    if(key.isAcceptable()){
                        registerConnectedClient(key);
                    }
                    else if (key.isReadable()) {
                        readReceivedData(key);
                    }

                }

            } catch (IOException e) {
                logger.error(e.toString());
                //e.printStackTrace();
            }
        }
    }

    private void registerConnectedClient(SelectionKey key) {
        try {
            SocketChannel connectedClient=serverSocketChannel.accept();
            if(connectedClientList.size()>0){

                logger.error("[MULTIPLE_CLIENT].Old Connection: "+connectedClientList.keySet().iterator().next().getRemoteAddress()+".New Request from: " + connectedClient.getRemoteAddress()+". Closed New Request");
                connectedClient.close();
            }
            else{
                connectedClient.configureBlocking(false);
                connectedClient.register(selector, SelectionKey.OP_READ);

                JSONObject connectedClientInfo=new JSONObject();
                connectedClientInfo.put("ipAddress",connectedClient.getRemoteAddress().toString().split("/")[1]);
                connectedClientInfo.put("buffer",new byte[0]);
                connectedClientList.put(connectedClient,connectedClientInfo);
                logger.info("[CONNECT] Connected with : " + connectedClient.getRemoteAddress());
            }

        } catch (IOException e) {
            logger.error(e.toString());
        }
    }
    public void disConnectAllConnectedClient(){
        for (SocketChannel key : connectedClientList.keySet()) {
            this.disconnectConnectedClient(key);
        }
    }
    public void disconnectConnectedClient(SocketChannel connectedClient) {
        try {
            connectedClient.close();
            JSONObject connectedClientInfo= connectedClientList.remove(connectedClient);
            logger.error("[DISCONNECT] Disconnected Client: " + connectedClientInfo.get("ipAddress"));

        } catch (IOException e) {
            logger.error(e.toString());
        }
    }

    private void readReceivedData(SelectionKey key) {
        SocketChannel connectedClient = (SocketChannel) key.channel();
        buffer.clear();

        int numRead = 0;
        try {
            numRead = connectedClient.read(buffer);
        } catch (IOException e) {
            logger.error(e.toString());
            disconnectConnectedClient(connectedClient);
            return;
        }
        if (numRead == -1) {
            disconnectConnectedClient(connectedClient);
            return;
        }

        byte[] b = new byte[buffer.position()];
        buffer.flip();
        buffer.get(b);
        processReceivedData(b,connectedClient);
    }
    public void processReceivedData(byte []data,SocketChannel connectedClient){
        try {
            String xmlMessage = new String( data, StandardCharsets.UTF_16LE );
            logger.info("[DATA_RECEIVED]["+connectedClient.getRemoteAddress().toString().split("/")[1]+"] " + xmlMessage);
            handleReceivedXMLMessage(xmlMessage,connectedClient);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    public void handleReceivedXMLMessage(String xmlMessage,SocketChannel connectedClient){
        //logger.info("[MESSAGE] FROM 360 :: " + xmlMessage);
        try {
            String responseXml="";
            DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
            DocumentBuilder db = dbf.newDocumentBuilder();
            ByteArrayInputStream bis = new ByteArrayInputStream(xmlMessage.getBytes());
            Document doc = db.parse(bis);
            Node pbNode = doc.getDocumentElement();
            if(pbNode.getNodeName().equals("PB")){
                Node idNode=pbNode.getAttributes().getNamedItem("id");
                if(idNode!=null){
                    String id=idNode.getTextContent();
                    Node messageNode = pbNode.getFirstChild();
                    if(messageNode.getNodeName().equals("sort_piece")){
                        try {
                            long productId= Long.parseLong(messageNode.getAttributes().getNamedItem("piece_id").getTextContent());
                            int mailId=DBCache.getMailIdFromMySQLProductId(productId);
                            if(mailId>0){
                                int dest1=Integer.parseInt(messageNode.getAttributes().getNamedItem("bin").getTextContent());
                                int dest2=Integer.parseInt(messageNode.getAttributes().getNamedItem("bin_secondary").getTextContent());
                                //send message to CMClient
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
                                sendDataToAllCmClients(joinTwoBytesArray(headerBytesForSend, bodyBytesForSend));

                                //send reply to connected client
                                responseXml = "<PB id=\""+id+"\"><sort_piece>OK</sort_piece></PB>";
                                sendDataToConnectedClient(getBytesUTF16LE(responseXml),connectedClient);
                            }
                            else{
                                logger.error("[SortMailpiece] MailId Not Found for product Id: "+productId);
                            }
                        }
                        catch (Exception ex){
                            logger.error(ex.toString());
                        }
                    }
                    else if(messageNode.getNodeName().equals("belts")){
                        try{
                            int beltsControl= Integer.parseInt(messageNode.getTextContent());
                            byte [] messageBytes=new byte[]{0, 0, 0, 125, 0, 0, 0, 10,0,0};
                            if(beltsControl>0){
                                messageBytes[9]=1;
                            }
                            sendDataToAllCmClients(messageBytes);

                            //send reply to connected client
                            responseXml = "<PB id=\""+id+"\"><belts>OK</belts></PB>";
                            sendDataToConnectedClient(getBytesUTF16LE(responseXml),connectedClient);
                        }
                        catch (Exception ex){
                            logger.error(ex.toString());
                        }
                    }
                    else if(messageNode.getNodeName().equals("refresh_state")){
                        try{
                            responseXml = "<PB id=\""+id+"\"><refresh_state>OK</refresh_state></PB>";
                            sendDataToConnectedClient(getBytesUTF16LE(responseXml),connectedClient);

                            String reasonText= ServerConstants.machine_stopped_reasons.get(ServerConstants.machineStoppedReason);
                            xmlMessage = "<machine_stopped type=\""+reasonText+"\" />";
                            this.threeSixtyClient.sendXmlMessage(xmlMessage);

                            xmlMessage = "<belts_status unit=\"ips\">"+ServerConstants.beltStatusSpeed+"</belts_status>";
                            this.threeSixtyClient.sendXmlMessage(xmlMessage);

                            Connection dbConn = DataSource.getConnection();
                            Statement stmt = dbConn.createStatement();
                            String query = "SELECT combo_id FROM active_alarms";
                            ResultSet rs = stmt.executeQuery(query);

                            //exception
                            while (rs.next()){

                                Map<String, String> singleAlarmData = DBCache.getAlarmData(rs.getString("combo_id"));
                                if(singleAlarmData.size()>0){
                                    responseXml = "<exception  id=\"" + singleAlarmData.get("alarm_id") + "\" severity=\"" + (singleAlarmData.get("alarm_type").equals("1")?2:1) + "\" location=\"" + singleAlarmData.get("location")+"\">" + singleAlarmData.get("description") + "</exception>";
                                    this.threeSixtyClient.sendXmlMessage(responseXml);
                                }
                            }
                            //bin_status
                            Map<Integer, JSONObject> binStatesData = new HashMap<>();
                            query = "SELECT bin_id,event_type FROM bin_states WHERE  machine_id= 1";
                            rs = stmt.executeQuery(query);
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
                            for (Map.Entry<Integer,JSONObject> entry : binStatesData.entrySet()) {
                                JSONObject obj=entry.getValue();
                                xmlMessage = "<bin_status status_id=\""+entry.getKey()+"\" removed=\""+obj.get("removed")+"\">"+obj.get("value")+"</bin_status>";
                                this.threeSixtyClient.sendXmlMessage(xmlMessage);
                            }


                            rs.close();
                            stmt.close();
                            dbConn.close(); // connection close

                        }
                        catch (Exception ex){
                            logger.error(ex.toString());
                        }
                    }

                }
                else{
                    logger.error("[XML_ERROR] PB id Invalid");
                }
            }
            else{
                logger.error("[XML_ERROR] Message Has no PB");
            }
        }
        catch (Exception ex) {
            System.out.println(ex.toString());
        }
    }
    public void sendDataToConnectedClient(byte []data,SocketChannel connectedClient){
        try {
            ByteBuffer buf = ByteBuffer.wrap(data);
            connectedClient.write(buf);
        } catch (IOException e) {
            logger.error(e.toString());
        }
    }
    private byte[] getBytesUTF16LE(String str) {
        final int length = str.length();
        final char buffer[] = new char[length];
        str.getChars(0, length, buffer, 0);
        final byte b[] = new byte[length*2];
        for (int j = 0; j < length; j++) {
            b[j*2] = (byte) (buffer[j] & 0xFF);
            b[j*2+1] = (byte) (buffer[j] >> 8);
        }
        return b;
    }
    public void addCmClients(int machineId, Client client) {
        if (!cmClients.containsKey(machineId)) {
            cmClients.put(machineId, client);
            //System.out.println("CM Client added to server");
        }
    }
    public void sendDataToAllCmClients(byte[]data){
        for (Map.Entry<Integer,Client> entry : cmClients.entrySet()) {
            sendDataToCmClient(entry.getValue(),data);
        }
    }
    public void sendDataToCmClient(Client client,byte []data){
        client.sendBytes(data);
    }
    public byte[] joinTwoBytesArray(byte[] a, byte[] b) {
        byte[] returnArray = new byte[a.length + b.length];

        ByteBuffer buff = ByteBuffer.wrap(returnArray);
        buff.put(a);
        buff.put(b);

        return buff.array();
    }
}
