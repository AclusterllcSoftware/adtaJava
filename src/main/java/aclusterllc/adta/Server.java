package aclusterllc.adta;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.json.JSONException;
import org.json.JSONObject;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

public class Server implements Runnable {

    private Thread worker;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicBoolean stopped = new AtomicBoolean(true);

    private final List<ServerListener> serverListeners = new ArrayList<>();
    private final List<ClientListListener> clientListListeners = new ArrayList<>();
    private static ConcurrentHashMap<String, SocketChannel> clientList = new ConcurrentHashMap<>();
    public CustomMap<Integer, Client> cmClients  = new CustomMap<>();
    Selector selector;
    ServerSocketChannel serverSocket;
    ByteBuffer buffer;
    private final ServerDBHandler serverDBHandler = new ServerDBHandler();
    Logger logger = LoggerFactory.getLogger(Server.class);


    public Server() {
    }

    public void start() {
        logger.info("HMI Server Started");
        worker = new Thread(this);
        try {
            selector = Selector.open();
            serverSocket = ServerSocketChannel.open();
            serverSocket.bind(new InetSocketAddress(ServerConstants.configuration.get("hmi_server_ip"), Integer.parseInt(ServerConstants.configuration.get("hmi_server_port"))));
            serverSocket.configureBlocking(false);
            serverSocket.register(selector, SelectionKey.OP_ACCEPT);
        } catch (IOException e) {
            logger.error(ServerConstants.configuration.get("hmi_server_ip") + " - " + Integer.parseInt(ServerConstants.configuration.get("hmi_server_port")));
            logger.error(e.toString());
            //e.printStackTrace();
        }
        buffer = ByteBuffer.allocate(10240000);
        worker.start();
    }

    public void interrupt() {
		running.set(false);
		notifyListeners("Self", "", "Disconnected from server");
        worker.interrupt();
    }

    boolean isRunning() {
        return running.get();
    }

    boolean isStopped() {
        return stopped.get();
    }

	public void run() {

		running.set(true);
        stopped.set(false);

		while (running.get()) {
		    //System.out.println("Socket running");
            try {
                selector.select();
                Set<SelectionKey> selectedKeys = selector.selectedKeys();
                Iterator<SelectionKey> iter = selectedKeys.iterator();
                while (iter.hasNext()) {
                    SelectionKey key = iter.next();

                    if (key.isValid() && key.isAcceptable()) {
                        register(selector, serverSocket);
                    }

                    if (key.isValid() && key.isReadable()) {
                        readMessage(key);
                    }

                    iter.remove();
                }

            } catch (IOException e) {
                logger.error(e.toString());
                //e.printStackTrace();
            }
		}

		stopped.set(true);
	}

	private void register(Selector selector, ServerSocketChannel serverSocket) {

        SocketChannel client = null;
        String clientName;
        try {
            client = serverSocket.accept();

            client.configureBlocking(false);
            client.register(selector, SelectionKey.OP_READ);
            clientName = client.getRemoteAddress().toString().split("/")[1];
            clientList.put(clientName, client);
            notifyClientListListeners(clientName, 1);
            notifyListeners(clientName, "", "Client connected");
            logger.info("HMI connected from " + clientName);
        } catch (IOException e) {
            //e.printStackTrace();
            logger.error(e.toString());
        }
    }

    public void disconnectClient(SocketChannel client) {

        String clientName = null;
        try {
            clientName = client.getRemoteAddress().toString().split("/")[1];
            client.close();
            clientList.remove(clientName);
            notifyClientListListeners(clientName, 0);
            notifyListeners(clientName, "", "Connection terminated");
            logger.error("HMI disconnected from " + clientName);
        } catch (IOException e) {
            //e.printStackTrace();
            logger.error(e.toString());
        }
    }


    private void readMessage(SelectionKey key) {
        SocketChannel client = (SocketChannel) key.channel();
        String clientName = null;
        try {
            clientName = client.getRemoteAddress().toString().split("/")[1];
        } catch (IOException e) {
            logger.error(e.toString());
            //e.printStackTrace();
        }

        buffer.clear();

        int numRead = 0;
        try {
            numRead = client.read(buffer);
        } catch (IOException e) {
            logger.error(e.toString());
            // The remote forcibly closed the connection, cancel
            // the selection key and close the channel.
            disconnectClient(client);

            return;
        }

        if (numRead == -1) {
            // Remote entity shut the socket down cleanly. Do the
            // same from our end and cancel the channel.
            disconnectClient(client);

            return;
        }

        byte[] b = new byte[buffer.position()];
        buffer.flip();
        buffer.get(b);

        String bufferToString = new String( b, StandardCharsets.UTF_8 );
        //System.out.println(bufferToString);
        while (bufferToString.length()>0){
            //System.out.println(bufferToString);
            if(bufferToString.charAt(0)=='{'){
                int firstCurlyCount=0;
                for(int i=0;i<bufferToString.length();i++){
                    if(bufferToString.charAt(i)=='{'){
                        firstCurlyCount++;
                    }
                    else if(bufferToString.charAt(i)=='}'){
                        firstCurlyCount--;
                    }
                    if(firstCurlyCount==0){
                        try {
                            JSONObject jo = new JSONObject(bufferToString.substring(0,i+1));
                            if(jo.get("req") != null) {
                                processClientRequest(clientName, jo);
                            }
                        } catch (JSONException | ParseException e) {
                            logger.error(e.toString());
                            //e.printStackTrace();
                        }
                        bufferToString=bufferToString.substring(i+1);
                        break;
                    }
                }
                if(firstCurlyCount!=0){
                    logger.error("Json end error");
                    break;
                }
            }
            else{
                logger.error("Json start error");
                break;
            }
        }

    }

    public void sendWelcomeMessage(String clientName) {
        if((clientList.size() > 0) && (clientList.get(clientName) != null)) {
            String msg = "{\"test\": \"msg\"}";
            sendMessage(clientName, msg);
        } else {
            System.err.println("Client not found");
        }
    }

    public void processClientRequest(String clientName, JSONObject jsonObject) throws ParseException {
        String req = jsonObject.get("req").toString();
        //System.out.println(req);
        switch (req) {
            case "basic_info": {
                JSONObject basic_info = new JSONObject();
                basic_info.put("alarmsInfo",DBCache.alarmsInfo);
                basic_info.put("binsInfo",DBCache.binsInfo);
                basic_info.put("conveyorsInfo",DBCache.conveyorsInfo);
                basic_info.put("devicesInfo",DBCache.devicesInfo);
                basic_info.put("inputsInfo",DBCache.inputsInfo);
                basic_info.put("scsInfo",DBCache.scsInfo);

                JSONObject response = new JSONObject();
                response.put("type","basic_info");
                response.put("basic_info",basic_info);
                sendMessage(clientName, response.toString());
                break;
            }
            case "send_ip_list": {
                JSONObject response = serverDBHandler.getMachineList();
                sendMessage(clientName, response.toString());

                break;
            }
            case "getStatistics": {
                int machineId = Integer.parseInt(jsonObject.get("machineId").toString());
                long from_timestamp = Long.parseLong(jsonObject.get("from_timestamp").toString());
                long to_timestamp = Long.parseLong(jsonObject.get("to_timestamp").toString());
                JSONObject response=new JSONObject();
                response.put("type","getStatistics");
                response.put("machineId",machineId);
                response.put("statistics",serverDBHandler.getStatistics(machineId,from_timestamp,to_timestamp));
                sendMessage(clientName, response.toString());
                break;
            }
            case "getStatisticsHourly": {
                int machineId = Integer.parseInt(jsonObject.get("machineId").toString());
                long from_timestamp = Long.parseLong(jsonObject.get("from_timestamp").toString());
                long to_timestamp = Long.parseLong(jsonObject.get("to_timestamp").toString());
                JSONObject response=new JSONObject();
                response.put("type","getStatisticsHourly");
                response.put("machineId",machineId);
                response.put("statistics",serverDBHandler.getStatisticsHourly(machineId,from_timestamp,to_timestamp));
                sendMessage(clientName, response.toString());
                break;
            }
            case "getStatisticsCounter": {
                int machineId = Integer.parseInt(jsonObject.get("machineId").toString());
                long from_timestamp = Long.parseLong(jsonObject.get("from_timestamp").toString());
                long to_timestamp = Long.parseLong(jsonObject.get("to_timestamp").toString());
                JSONObject response=new JSONObject();
                response.put("type","getStatisticsCounter");
                response.put("machineId",machineId);
                response.put("statistics",serverDBHandler.getStatisticsCounter(machineId,from_timestamp,to_timestamp));
                sendMessage(clientName, response.toString());
                break;
            }
            case "getStatisticsBins": {
                int machineId = Integer.parseInt(jsonObject.get("machineId").toString());
                long from_timestamp = Long.parseLong(jsonObject.get("from_timestamp").toString());
                long to_timestamp = Long.parseLong(jsonObject.get("to_timestamp").toString());
                JSONObject response=new JSONObject();
                response.put("type","getStatisticsBins");
                response.put("machineId",machineId);
                response.put("statistics",serverDBHandler.getStatisticsBins(machineId,from_timestamp,to_timestamp));
                sendMessage(clientName, response.toString());
                break;
            }
            case "getStatisticsBinsHourly": {
                int machineId = Integer.parseInt(jsonObject.get("machineId").toString());
                long from_timestamp = Long.parseLong(jsonObject.get("from_timestamp").toString());
                long to_timestamp = Long.parseLong(jsonObject.get("to_timestamp").toString());
                JSONObject response=new JSONObject();
                response.put("type","getStatisticsBinsHourly");
                response.put("machineId",machineId);
                response.put("statistics",serverDBHandler.getStatisticsBinsHourly(machineId,from_timestamp,to_timestamp));
                sendMessage(clientName, response.toString());
                break;
            }
            case "getStatisticsBinsCounter": {
                int machineId = Integer.parseInt(jsonObject.get("machineId").toString());
                long from_timestamp = Long.parseLong(jsonObject.get("from_timestamp").toString());
                long to_timestamp = Long.parseLong(jsonObject.get("to_timestamp").toString());
                JSONObject response=new JSONObject();
                response.put("type","getStatisticsBinsCounter");
                response.put("machineId",machineId);
                response.put("statistics",serverDBHandler.getStatisticsBinsCounter(machineId,from_timestamp,to_timestamp));
                sendMessage(clientName, response.toString());
                break;
            }
            case "getGeneralViewData": {
                int machineId = Integer.parseInt(jsonObject.get("machineId").toString());
                //JSONObject params= (JSONObject) jsonObject.get("params");
                JSONObject response=new JSONObject();
                response.put("type","getGeneralViewData");
                response.put("machineId",machineId);
                response.put("binsStates",serverDBHandler.getBinsStates(machineId));
                response.put("inputsStates",serverDBHandler.getInputsStates(machineId));
                response.put("conveyorsStates",serverDBHandler.getConveyorsStates(machineId));
                response.put("activeAlarms",serverDBHandler.getActiveAlarms(machineId));
                sendMessage(clientName, response.toString());
                break;
            }
            case "getGeneralDevicesViewData": {
                int machineId = Integer.parseInt(jsonObject.get("machineId").toString());
                //JSONObject params= (JSONObject) jsonObject.get("params");
                JSONObject response=new JSONObject();
                response.put("type","getGeneralDevicesViewData");
                response.put("machineId",machineId);
                response.put("binsStates",serverDBHandler.getBinsStates(machineId));
                response.put("inputsStates",serverDBHandler.getInputsStates(machineId));//for estops
                response.put("conveyorsStates",serverDBHandler.getConveyorsStates(machineId));
                response.put("devicesStates",serverDBHandler.getDevicesStates(machineId));
                response.put("activeAlarms",serverDBHandler.getActiveAlarms(machineId));
                sendMessage(clientName, response.toString());
                break;
            }
            case "getGeneralMotorsViewData": {
                int machineId = Integer.parseInt(jsonObject.get("machineId").toString());
                //JSONObject params= (JSONObject) jsonObject.get("params");
                JSONObject response=new JSONObject();
                response.put("type","getGeneralMotorsViewData");
                response.put("machineId",machineId);
                response.put("binsStates",serverDBHandler.getBinsStates(machineId));
                response.put("conveyorsStates",serverDBHandler.getConveyorsStates(machineId));
                response.put("activeAlarms",serverDBHandler.getActiveAlarms(machineId));
                sendMessage(clientName, response.toString());
                break;
            }
            case "getAlarmsViewData": {
                int machineId = Integer.parseInt(jsonObject.get("machineId").toString());
                //JSONObject params= (JSONObject) jsonObject.get("params");
                JSONObject response=new JSONObject();
                response.put("type","getAlarmsViewData");
                response.put("machineId",machineId);
                response.put("activeAlarms",serverDBHandler.getActiveAlarms(machineId));
                sendMessage(clientName, response.toString());
                break;
            }
            case "getGeneralBinDetailsViewData": {
                int machineId = Integer.parseInt(jsonObject.get("machineId").toString());
                JSONObject params= (JSONObject) jsonObject.get("params");
                int sort_manager_id = Integer.parseInt(params.get("sort_manager_id").toString());
                JSONObject response=new JSONObject();
                response.put("type","getGeneralBinDetailsViewData");
                response.put("machineId",machineId);
                response.put("sort_manager_id",sort_manager_id);
                response.put("inputsStates",serverDBHandler.getInputsStates(machineId));
                sendMessage(clientName, response.toString());
                break;
            }
            case "sendDeviceCommand": {
                int machineId = Integer.parseInt(jsonObject.get("machineId").toString());
                JSONObject params= (JSONObject) jsonObject.get("params");
                int deviceId = Integer.parseInt(params.get("deviceId").toString());
                int command = Integer.parseInt(params.get("command").toString());
                int parameter1 = Integer.parseInt(params.get("parameter1").toString());
                byte[] messageBytes= new byte[]{
                        0, 0, 0, 123, 0, 0, 0, 20,
                        (byte) (deviceId >> 24),(byte) (deviceId >> 16),(byte) (deviceId >> 8),(byte) (deviceId),
                        (byte) (command >> 24),(byte) (command >> 16),(byte) (command >> 8),(byte) (command),
                        (byte) (parameter1 >> 24),(byte) (parameter1 >> 16),(byte) (parameter1 >> 8),(byte) (parameter1)
                    };
                Client client = cmClients.get(machineId);
                client.sendBytes(messageBytes);
                break;
            }
            //--------------------------
            case "mod_sort": {
                int machineId = Integer.parseInt(jsonObject.get("id").toString());
                int device_type = Integer.parseInt(jsonObject.get("device_type").toString());
                int device_number = Integer.parseInt(jsonObject.get("device_number").toString());
                //System.out.println("Machine:" + machineId + "D:" + device_type + " S:" + device_number);
                JSONObject response = serverDBHandler.getModSort(machineId, device_type, device_number);
                //System.out.println(response);
                sendMessage(clientName, response.toString());
                break;
            }
            case "induct": {
                int machineId = Integer.parseInt(jsonObject.get("id").toString());
                int induct_id = Integer.parseInt(jsonObject.get("induct_number").toString());
                JSONObject response = serverDBHandler.getInduct(machineId, induct_id);
                //System.out.println(response);
                sendMessage(clientName, response.toString());
                break;
            }
            case "alarms_history": {
                int machineId = Integer.parseInt(jsonObject.get("id").toString());
                JSONObject response = serverDBHandler.getAlarmsHistory(machineId, 0, 0);
                sendMessage(clientName, response.toString());
                break;
            }
            case "filtered_alarm_history": {
                int machineId = Integer.parseInt(jsonObject.get("id").toString());
                long startTimestamp = Long.parseLong(jsonObject.get("start").toString());
                long endTimestamp = Long.parseLong(jsonObject.get("end").toString());
                JSONObject response = serverDBHandler.getAlarmsHistory(machineId, startTimestamp, endTimestamp);
                sendMessage(clientName, response.toString());
                break;
            }
            case "alarms_hit_list": {
                int machineId = Integer.parseInt(jsonObject.get("id").toString());
                JSONObject response = serverDBHandler.getAlarmsHitList(machineId, 0, 0);
                sendMessage(clientName, response.toString());
                break;
            }
            case "filtered_alarm_hit_list": {
                int machineId = Integer.parseInt(jsonObject.get("id").toString());
                long startTimestamp = Long.parseLong(jsonObject.get("start").toString());
                long endTimestamp = Long.parseLong(jsonObject.get("end").toString());
                JSONObject response = serverDBHandler.getAlarmsHitList(machineId, startTimestamp, endTimestamp);
                sendMessage(clientName, response.toString());
                break;
            }
            case "status": {
                int machineId = Integer.parseInt(jsonObject.get("id").toString());
                JSONObject response = serverDBHandler.getErrorStatus(machineId);
                sendMessage(clientName, response.toString());
                break;
            }
            case "statistics": {
                int machineId = Integer.parseInt(jsonObject.get("id").toString());
                JSONObject response = serverDBHandler.getStatistics(machineId);
                sendMessage(clientName, response.toString());
                break;
            }
            case "sorted_graphs": {
                int machineId = Integer.parseInt(jsonObject.get("id").toString());
                JSONObject response = serverDBHandler.getSortedGraphs(machineId);
                sendMessage(clientName, response.toString());
                break;
            }
            case "device_status": {
                int machineId = Integer.parseInt(jsonObject.get("id").toString());
                JSONObject response = serverDBHandler.getDeviceStatus(machineId);
                sendMessage(clientName, response.toString());
                break;
            }
            case "package_list": {
                int machineId = Integer.parseInt(jsonObject.get("id").toString());
                JSONObject response = serverDBHandler.getPackageList(machineId);
                sendMessage(clientName, response.toString());
                break;
            }
            case "filtered_package_list": {
                int machineId = Integer.parseInt(jsonObject.get("id").toString());
                long startTimestamp = Long.parseLong(jsonObject.get("start").toString());
                long endTimestamp = Long.parseLong(jsonObject.get("end").toString());
                String sortingCode = jsonObject.get("sc").toString();
                JSONObject response = serverDBHandler.getFilteredPackageList(machineId, startTimestamp, endTimestamp, sortingCode);
                sendMessage(clientName, response.toString());
                break;
            }
            case "filtered_package_to_sort_list": {
                //int machineId = Integer.parseInt(jsonObject.get("id").toString());
                String cartonId = jsonObject.get("cartonId").toString();
                JSONObject response = serverDBHandler.getIngramProducts(cartonId);
                sendMessage(clientName, response.toString());
                break;
            }
            case "settings" : {
                int machineId = Integer.parseInt(jsonObject.get("id").toString());
                JSONObject response = serverDBHandler.getSettings(machineId);
                sendMessage(clientName, response.toString());
                break;
            }
            case "change_mode" : {
                int machineId = Integer.parseInt(jsonObject.get("id").toString());
                int mode = Integer.parseInt(jsonObject.get("mode").toString());

                Client client = cmClients.get(machineId);
                try {
                    client.sendMessage(120, mode, 0);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                break;
            }
            case "change_induct" : {
                int machineId = Integer.parseInt(jsonObject.get("id").toString());
                int mode = Integer.parseInt(jsonObject.get("mode").toString());
                int inductId = 50 + Integer.parseInt(jsonObject.get("induct").toString());

                Client client = cmClients.get(machineId);
                try {
                    client.sendMessage(123, inductId, mode);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                break;
            }
            case "change_bin_mode": {
                int machineId = Integer.parseInt(jsonObject.get("machine").toString());
                int binId = Integer.parseInt(jsonObject.get("bin").toString());
                int mode = Integer.parseInt(jsonObject.get("mode").toString());

                //System.out.println("Change bin mode,  Machine: " + machineId + ", Bin : " + binId + ", Mode: " + mode);
                Client client = cmClients.get(machineId);
                try {
                    client.sendMessage(111, mode, binId);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                break;
            }
            case "device_command" : {
                int machineId = Integer.parseInt(jsonObject.get("id").toString());
                int deviceId = Integer.parseInt(jsonObject.get("device").toString());
                int operationId = Integer.parseInt(jsonObject.get("operation").toString());

                Client client = cmClients.get(machineId);
                try {
                    client.sendMessage(123, deviceId, operationId);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                break;
            }
            case "login_user" : {
                int machineId = Integer.parseInt(jsonObject.get("id").toString());
                String username = jsonObject.get("username").toString();
                String password = jsonObject.get("password").toString();
                JSONObject response = serverDBHandler.loginUser(machineId, username, password);
                //System.out.println(response);
                sendMessage(clientName, response.toString());
                break;
            }
        }
    }

    public void sendMessage(String clientName, String msg) {
        msg = msg + ";#;#;";

        if((clientList.size() > 0) && (clientList.get(clientName) != null)) {
            SocketChannel sc = clientList.get(clientName);
            ByteBuffer buf = ByteBuffer.wrap(msg.getBytes());
            try {
                sc.write(buf);
            } catch (IOException e) {
                //e.printStackTrace();
                logger.error(e.toString());
            }
        } else {
            System.err.println("Client not found");
        }
    }

    public void addCmClients(int machineId, Client client) {
        if (!cmClients.containsKey(machineId)) {
            cmClients.put(machineId, client);
            //System.out.println("CM Client added to server");
        }
    }

    public void addListeners(ServerListener serverListener) {
        serverListeners.add(serverListener);
    }

    public void removeListener(ServerListener serverListener) {
        serverListeners.remove(serverListener);
    }

    public void notifyListeners(String fromName, String toName, String msg) {
        if(serverListeners.size() > 0) {
            serverListeners.forEach((el) -> el.update(fromName, toName, msg));
        }
    }

    public void addClientListListeners(ClientListListener clientListListener) {
        clientListListeners.add(clientListListener);

    }

    public void removeClientListListeners(ClientListListener clientListListener) {
        clientListListeners.remove(clientListListener);
    }

    public void notifyClientListListeners(String socketName, int opt) {
        if(clientListListeners.size() > 0) {
            clientListListeners.forEach((el) -> el.updateClientList(socketName, opt));
        }
    }
}
