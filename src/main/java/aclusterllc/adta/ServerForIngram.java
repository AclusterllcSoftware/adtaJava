package aclusterllc.adta;


import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;

public class ServerForIngram implements Runnable{
    private Thread worker;
    Logger logger = LoggerFactory.getLogger(ServerForIngram.class);
    Selector selector;
    ByteBuffer buffer = ByteBuffer.allocate(10240000);
    ServerSocketChannel serverSocketChannel;
//    ConcurrentHashMap<String, SocketChannel> ingramClientList = new ConcurrentHashMap<>();//Currently has no use. may be for future use
    ConcurrentHashMap<SocketChannel, JSONObject> ingramClientList = new ConcurrentHashMap<>();//Currently has no use. may be for future use
    private DatabaseHandler databaseHandler;
    public ServerForIngram( DatabaseHandler databaseHandler) {
        this.databaseHandler = databaseHandler;
    }
    public void start(){
        worker = new Thread(this);
        try {
            selector = Selector.open();
            serverSocketChannel = ServerSocketChannel.open();
            //serverSocketChannel.bind(new InetSocketAddress(ServerConstants.configuration.get("ingram_server_ip"), Integer.parseInt(ServerConstants.configuration.get("ingram_server_port"))));
            serverSocketChannel.bind(new InetSocketAddress(Integer.parseInt(ServerConstants.configuration.get("ingram_server_port"))));
            serverSocketChannel.configureBlocking(false);
            serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT);
            worker.start();
        } catch (IOException e) {
            logger.error(ServerConstants.configuration.get("ingram_server_ip") + " - " + Integer.parseInt(ServerConstants.configuration.get("ingram_server_port")));
            logger.error(e.toString());
        }
    }
    public void run(){
        //logger.info("Server started: "+ServerConstants.configuration.get("ingram_server_ip") + " - " + Integer.parseInt(ServerConstants.configuration.get("ingram_server_port")));
        logger.info("Server started: "+Integer.parseInt(ServerConstants.configuration.get("ingram_server_port")));
        while (true) {
            try {
                System.out.println("waiting for event");
                selector.select();
                Iterator<SelectionKey> iterator = selector.selectedKeys().iterator();
                while (iterator.hasNext()) {
                    SelectionKey key = iterator.next();
                    iterator.remove();
                    if(!key.isValid()){
                        continue;
                    }
                    if(key.isAcceptable()){
                            registerIngramClient(key);
                    }
                    else if (key.isReadable()) {
                        readIngramClientMessage(key);
                    }

                }

            } catch (IOException e) {
                logger.error(e.toString());
                //e.printStackTrace();
            }
        }
    }
    public void registerIngramClient(SelectionKey key){
        try {
            SocketChannel ingramClient=serverSocketChannel.accept();
            if(ingramClientList.size()>0){

                logger.error("[MULTIPLE_CLIENT].Old Connection: "+ingramClientList.keySet().iterator().next().getRemoteAddress()+".New Request from: " + ingramClient.getRemoteAddress()+". Closed New Request");
                ingramClient.close();
            }
            else{
                ingramClient.configureBlocking(false);
                ingramClient.register(selector, SelectionKey.OP_READ);
                //ingramClientList.put(ingramClient.getRemoteAddress().toString().split("/")[1], ingramClient);
                JSONObject ingramClientInfo=new JSONObject();
                ingramClientInfo.put("ipAddress",ingramClient.getRemoteAddress().toString().split("/")[1]);
                ingramClientInfo.put("buffer",new byte[0]);
                ingramClientList.put(ingramClient,ingramClientInfo);
                logger.info("Connected with Ingram: " + ingramClient.getRemoteAddress());
                ServerConstants.serverForIngramConnected = 1;
            }

        } catch (IOException e) {
            logger.error(e.toString());
        }
    }
    public void disConnectAllIngramClient(){
        for (SocketChannel key : ingramClientList.keySet()) {
            this.disconnectIngramClient(key);
        }
    }
    public void disconnectIngramClient(SocketChannel ingramClient) {
        try {
            ingramClient.close();
            JSONObject ingramClientInfo= ingramClientList.remove(ingramClient);
            logger.error("Disconnected Ingram: " + ingramClientInfo.get("ipAddress"));
            if(ingramClientList.size()==0){
                ServerConstants.serverForIngramConnected = 0;
            }

        } catch (IOException e) {
            logger.error(e.toString());
        }
    }
    public void readIngramClientMessage(SelectionKey key) {
        SocketChannel ingramClient = (SocketChannel) key.channel();
        buffer.clear();
        int numRead = 0;
        try {
            numRead = ingramClient.read(buffer);
        } catch (IOException e) {
            logger.error(e.toString());
            disconnectIngramClient(ingramClient);
            return;
        }
        if (numRead == -1) {
            // Remote entity shut the socket down cleanly. Do the same from our end and cancel the channel.
            disconnectIngramClient(ingramClient);
            return;
        }
        byte[] b = new byte[buffer.position()];
        buffer.flip();
        buffer.get(b);
        processIngramClientMessage(b,ingramClient);
    }
    public void processIngramClientMessage(byte[] data,SocketChannel ingramClient){
        String tbl="ingram_products";

        logger.info("Data Received. Length: "+data.length+" bytes");
        if(data.length!=30){
            logger.info("[DIFFERENT_LENGTH] Length is not 30.");
        }
        JSONObject ingramClientInfo=ingramClientList.get(ingramClient);
        if(ingramClientInfo==null){
            logger.error("Buffer Not found Error.");
        }
        else{
            byte[] previousData=(byte[]) ingramClientInfo.get("buffer");
            if(previousData.length>0){
                try {
                    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
                    outputStream.write(previousData);
                    outputStream.write(data);
                    data=outputStream.toByteArray();
                    logger.info("Merged with previous "+previousData.length+" bytes. New data length: "+data.length+" bytes");
                } catch (IOException ex) {
                    logger.info("Data Merging Error");
                    logger.info(ex.toString());
                }
            }
            while (data.length>29){
                if(data[0]==2){
                    if(data[29]==3){
                        try {
                        String carton_id=(new String(data,1,20)).replaceAll(" +$", "");
                        int dest1=Integer.parseInt(new String(data,21,4));
                        dest1= Math.min(dest1, 31);
                        int dest2=Integer.parseInt(new String(data,25,4));
                        dest2= Math.min(dest2, 31);
                        String query = String.format("INSERT INTO %s (`carton_id`,`dest1`,`dest2`,`created_at`) VALUES( '%s', %d, %d,CURRENT_TIMESTAMP()) ON DUPLICATE KEY UPDATE dest1=VALUES(dest1),dest2=VALUES(dest2),created_at=created_at, updated_at=VALUES(created_at)", tbl,carton_id,dest1,dest2);
                        databaseHandler.append(query);
                        logger.info("[PRODUCT] carton_id= "+carton_id+",dest1= "+dest1+",dest2= "+dest2);
                        }
                        catch (Exception ex) {
                            logger.info("Data Parsing Error.");
                            logger.info(ex.toString());
                        }
                    }
                    else{
                        logger.error("Found STX(2) But Missing ETX(3).Found "+data[29]+".Skipping 30 bytes");
                    }
                    data= Arrays.copyOfRange(data, 30, data.length);
                }
                else{
                    logger.error("Missing STX(2).Found "+data[0]+".Skipping and Checking next byte.");
                    data= Arrays.copyOfRange(data, 1, data.length);
                }
            }
            ingramClientInfo.put("buffer",data);
            ingramClientList.put(ingramClient,ingramClientInfo);
            if(data.length>0){
                logger.warn("Storing Last Partial data "+data.length +" bytes");
            }

        }
    }
}
