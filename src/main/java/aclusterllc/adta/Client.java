package aclusterllc.adta;

import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.lang.Thread.sleep;

public class Client implements Runnable {

	private Thread worker;
    private AtomicBoolean running = new AtomicBoolean(false);
    private AtomicBoolean stopped = new AtomicBoolean(true);
	private AtomicBoolean threeSixtyRunning = new AtomicBoolean(false);
    //private boolean tryToReconnect = true;
	private final long pingDelayMillisFirstPart = 2500;
	private final long pingDelayMillisSecondPart = 2500;
	private final long reconnectDelayMillis = 5000;
	private boolean reconnectThreadStarted = false;

	//private Socket socket;
	private SocketChannel socket;
	InetSocketAddress socketAddr;
	ByteBuffer buffer = ByteBuffer.allocate(10240000);
	Selector selector;

	private String host;
	private int port;
	public int machineId;
	private final List<ClientListener> clientListeners = new ArrayList<>();
	private final MessageHandler messageHandler;// = new MessageHandler();
	private int pingPongCounter = 0;

	Logger logger = LoggerFactory.getLogger(Client.class);
	public ThreeSixtyClient threeSixtyClient;
	public Client(String host_address, int port_number, int machine_id, ThreeSixtyClient mainThreeSixtyClient, DatabaseHandler databaseHandler) {
		host = host_address;
		port = port_number;
		machineId = machine_id;
		threeSixtyClient = mainThreeSixtyClient;
		messageHandler = new MessageHandler(databaseHandler,this);
	}

	public void addListeners(ClientListener clientListener) {
        clientListeners.add(clientListener);
    }

    public void removeListener(ClientListener clientListener) {
        clientListeners.remove(clientListener);
    }

    public void notifyListeners(String socketName, String msg) {
        if(clientListeners.size() > 0) {
            clientListeners.forEach((el) -> el.update(socketName, msg));
        }
    }

	public void start() {
		if(worker == null || !worker.isAlive()) {
			worker = new Thread(this);

			try {
				logger.error("Trying to connect to the Machine-" + machineId);
				//System.out.println("Trying to connect");
				selector = Selector.open();
				socketAddr = new InetSocketAddress(host, port);
				socket = SocketChannel.open(socketAddr);
				socket.configureBlocking(false);
				socket.register(selector, SelectionKey.OP_READ, new StringBuffer());
				//tryToReconnect = false;
				reconnectThreadStarted = false;
				ServerConstants.plcConnectStatus.put(machineId, 1);
				worker.start(); //Need to fix java.lang.IllegalThreadStateException
			} catch (IOException e) {
				//System.out.println("Connection failed");
				logger.error(e.toString());
				if (!reconnectThreadStarted) {
					logger.error("Starting reconnection thread from connection fail");
					//System.out.println("Starting reconnection thread from connection fail");
					reconnectThreadStarted = true;
					ServerConstants.plcConnectStatus.put(machineId, 0);
					startReconnectThread();
				}
			}
		}
    }

	public void interrupt(){
		running.set(false);

		if(!reconnectThreadStarted) {
			reconnectThreadStarted = true;
			ServerConstants.plcConnectStatus.put(machineId, 0);
			startReconnectThread();
		}

		try {
			socket.close();
			logger.error("Disconnected from Machine-" + machineId);
			notifyListeners("Self", "Disconnected from server [M:" + machineId + "]");
		} catch (IOException e) {
			//e.printStackTrace();
			logger.error(e.toString());
		}

        worker.interrupt();
    }

    boolean isRunning() {
        return running.get();
    }

    boolean isStopped() {
        return stopped.get();
    }

	public void run() {
		//String inputLine;
		running.set(true);
        stopped.set(false);
        pingPongCounter = 0;
		logger.info("Connected to Machine-" + machineId);
		sendSyncMessage();

		if(threeSixtyClient.isRunning()) {
			threeSixtyRunning.set(true);
			//sendAuthToStart(1);
		} else {
			threeSixtyRunning.set(false);
			//sendAuthToStart(0);
		}

		startPingThread();
		while (running.get()) {
			try {
                selector.select();
                Set<SelectionKey> selectedKeys = selector.selectedKeys();
                Iterator<SelectionKey> iter = selectedKeys.iterator();
                while (iter.hasNext()) {
                    SelectionKey key = iter.next();
                    if (key.isValid() && key.isReadable()) {
                        readMessage(key);
                    }
					//have to handle
                    iter.remove(); //have to handle java.util.ConcurrentModificationException
                }
            } catch(IOException | CancelledKeyException | ConcurrentModificationException innerIOE) {
				logger.error(innerIOE.toString());
				if(innerIOE.getMessage().indexOf("An existing connection was forcibly closed") != -1 ||
						innerIOE.getMessage().indexOf("ConcurrentModificationException") != -1 ||
						innerIOE.getMessage().indexOf("ClosedChannelException") != -1
				) {
					//log.info("Exception expected, yay.");
				}
			} // end inner catch
		}

		stopped.set(true);
	}


	public void sendBytes(byte[] myByteArray){
		sendBytes(myByteArray, 0, myByteArray.length);
	}

	public void sendBytes(byte[] myByteArray, int start, int len)  {
		if (len < 1){
			logger.error("[SEND_TO_CM] Negative length not allowed");
		}
		else if (start < 0 || start >= myByteArray.length)
		{
			logger.error("[SEND_TO_CM] Out of bounds: " + start);
		}
		else if (!isRunning())
		{
			logger.error("[SEND_TO_CM] Client not connected");
		}
		else {
			ByteBuffer buf = ByteBuffer.wrap(myByteArray);
			try {
				socket.write(buf);
			} catch (IOException e) {
				logger.error("[SEND_TO_CM] "+e.toString());
			}
		}
	}

	public void readMessage(SelectionKey key) throws IOException {
        SocketChannel client = (SocketChannel) key.channel();

        buffer.clear();

        int numRead = 0;
        try {
            numRead = client.read(buffer);
        } catch (IOException e) {
        	logger.error(e.toString());
            // The remote forcibly closed the connection, cancel
            // the selection key and close the channel.
            interrupt();

            return;
        }

        if (numRead == -1) {
            // Remote entity shut the socket down cleanly. Do the
            // same from our end and cancel the channel.
			//logger.error("Disconnected from Machine-" + machineId);
            interrupt();

            return;
        }


		byte[] b = new byte[buffer.position()];
		buffer.flip();
		buffer.get(b);

		Map<Integer, String> decodedMessages = messageHandler.decodeMessage(b, machineId);
		if(decodedMessages.size() > 0) {
			for (Map.Entry<Integer, String> entry : decodedMessages.entrySet()) {

				Integer k = entry.getKey();
				String v = entry.getValue();

				String messageName;
				int messageId;
				String messageIdInString;
				String notificationStr;

				messageId = k;
				messageName = v;
				messageIdInString = Integer.toString(messageId);
				notificationStr = messageName + " [" + messageIdInString + "]" + "[M:" + machineId + "]";

				if (messageId == 30) {
					pingPongCounter=0;
					notifyListeners("Server", "ping-rec");
					logger.info("Ping received from Machine-" + machineId);
				} else {
					notifyListeners("Server", notificationStr);
				}
			}
		}

    }

	public void sendMessage(int messageId) throws IOException {
		byte[] messageBytes = messageHandler.encodeRequestMessage(messageId);
		sendBytes(messageBytes);
	}

	public void sendMessage(int messageId, int mode, int id) throws IOException {
		//System.out.println("Sending message: " + messageId);
		byte[] messageBytes = messageHandler.encodeRequestMessage(messageId, mode, id);
		sendBytes(messageBytes);
	}

	private void sendSyncMessage() {
		try {
			sendMessage(116);
		} catch (IOException e) {
			logger.error(e.toString());
		}
	}

	private void sendAuthToStart(int mode) {
		try {
			sendMessage(125, mode, 0);
		} catch (IOException e) {
			logger.error(e.toString());
		}

		//System.out.println("AUTH TO START IS STOPPED NOW");
	}

	private void startReconnectThread() {
		//send a test signal
		// You may or may not want to stop the thread here
		Thread reconnectThread = new Thread(() -> {
			while(!Client.this.isRunning()) {
				Client.this.start();

				try {
					sleep(Client.this.reconnectDelayMillis);
				} catch (InterruptedException e) {
					logger.error(e.toString());
					//System.out.print("Sleep interrupted in reconnect thread ID = " + Thread.currentThread().getId() + "\n");
				}
			}
		});

		reconnectThread.start();
	}

	private void startPingThread() {

		//send a test signal
		// You may or may not want to stop the thread here
		Thread pingThread = new Thread(() -> {
			boolean threadClosed = false;
			while (Client.this.isRunning()) {
				if(Client.this.pingPongCounter < 3) {
					try {
						Client.this.sendMessage(130);
						if(!(Client.this.threeSixtyRunning.get() & Client.this.threeSixtyClient.isRunning())) {
							if(Client.this.threeSixtyClient.isRunning()) {
								Client.this.threeSixtyRunning.set(true);
								//Client.this.sendAuthToStart(1);
							} else {
								Client.this.threeSixtyRunning.set(false);
								//Client.this.sendAuthToStart(0);
							}

						}

						logger.info("Ping Message sent to Machine-" + machineId);
						Client.this.pingPongCounter++;
					} catch (IOException e) {
						logger.error(e.toString());
					}
				} else {
					if(Client.this.isRunning()) {
						Client.this.interrupt();
						Thread.currentThread().interrupt();
						threadClosed = true;
					}
				}

				try {
					if(!threadClosed) {
						sleep(Client.this.pingDelayMillisFirstPart);
						notifyListeners("Server", "ping-sent");
						sleep(Client.this.pingDelayMillisSecondPart);
					}
				} catch (InterruptedException e) {
					logger.error(e.toString());
				}
			}
		});

		pingThread.start();
	}
	//TODO new version by shaiful
	public void handleMessage(JSONObject params){
		this.messageHandler.handleMessage(params);
	}

}
