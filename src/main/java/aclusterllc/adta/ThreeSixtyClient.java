package aclusterllc.adta;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.lang.Thread.sleep;

public class ThreeSixtyClient implements Runnable {

	private Thread worker;
    private AtomicBoolean running = new AtomicBoolean(false);
    private AtomicBoolean stopped = new AtomicBoolean(true);
    //private boolean tryToReconnect = true;
	private final long pingDelayMillisFirstPart = 1500;
	private final long pingDelayMillisSecondPart = 1500;
	private final long reconnectDelayMillis = 5000;
	private boolean reconnectThreadStarted = false;

	//private Socket socket;
	private SocketChannel socket;
	InetSocketAddress socketAddr;
	ByteBuffer buffer = ByteBuffer.allocate(10240000);
	Selector selector;

	private String host;
	private int port;
	private int machineId;
	private final List<ClientListener> clientListeners = new ArrayList<>();
	//private final MessageHandler messageHandler = new MessageHandler();
	private int pingPongCounter = 0;
	private long currentPBId = 0;

	Logger logger = LoggerFactory.getLogger(ThreeSixtyClient.class);

	public ThreeSixtyClient(String host_address, int port_number) {
		//System.out.print(host_address + " : " + port_number);
		host = host_address;
		port = port_number;
		//machineId = machine_id;
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
				logger.error("Trying to connect to the 360 server");
				//System.out.println("Trying to connect to 360 server");
				selector = Selector.open();
				socketAddr = new InetSocketAddress(host, port);
				socket = SocketChannel.open(socketAddr);
				socket.configureBlocking(false);
				socket.register(selector, SelectionKey.OP_READ, new StringBuffer());
				//tryToReconnect = false;
				reconnectThreadStarted = false;
				ServerConstants.threeSixtyClientConnected = 1;
				worker.start(); //Need to fix java.lang.IllegalThreadStateException
			} catch (IOException e) {
				//System.out.println("Connection to 360 server failed");
				logger.error(e.toString());
				if (!reconnectThreadStarted) {
					logger.error("Starting reconnection thread from connection fail [360 server]");
					//System.out.println("Starting reconnection thread from connection fail [360 server]");
					reconnectThreadStarted = true;
					ServerConstants.threeSixtyClientConnected = 0;
					startReconnectThread();
				}
			}
		}
    }

	public void interrupt(){
		running.set(false);

		if(!reconnectThreadStarted) {
			reconnectThreadStarted = true;
			ServerConstants.threeSixtyClientConnected = 0;
			startReconnectThread();
		}

		try {
			socket.close();
			notifyListeners("Self", "Disconnected from 360 server");
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
		notifyListeners("Self", "Connected to 360 server");
		logger.info("Connected to 360 server");
		running.set(true);
        stopped.set(false);
        pingPongCounter = 0;

		//sendSyncMessage();

		//startPingThread();
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


	private void sendBytes(byte[] myByteArray){
		sendBytes(myByteArray, 0, myByteArray.length);
	}

	public void sendBytes(byte[] myByteArray, int start, int len)  {
		if (len < 1){
			logger.error("[SEND_FROM_360CLIENT] Negative length not allowed");
		}
		else if (start < 0 || start >= myByteArray.length)
		{
			logger.error("[SEND_FROM_360CLIENT] Out of bounds: " + start);
		}
		else if (!isRunning())
		{
			logger.error("[SEND_FROM_360CLIENT] Client not connected");
		}
		else {
			ByteBuffer buf = ByteBuffer.wrap(myByteArray);
			try {
				socket.write(buf);
			} catch (IOException e) {
				logger.error("[SEND_FROM_360CLIENT] "+e.toString());
			}
		}
	}

	private void readMessage(SelectionKey key) {
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
			logger.error("360 server closed socket connection");
			//System.out.println("360 server closed socket connection");
            interrupt();

            return;
        }
		byte[] b = new byte[buffer.position()];
		buffer.flip();
		buffer.get(b);

		String three360s = new String(b, StandardCharsets.UTF_16LE);
		if(Integer.parseInt(ServerConstants.configuration.get("threesixty_log_enabled")) == 1) {
			if (!three360s.contains("heartbeat")) {
				logger.info("FROM 360 :: " + three360s);
			}
		}
    }

	public void sendMessage(int messageId, String xmlMessage, long mailId, int machineId) throws IOException {
		//byte[] messageBytes = messageHandler.encodeRequestMessage(messageId);
		long pb_id = getPBId();
		StringBuilder xmlBuilder;
		String xmlString;
		switch (messageId) {
			case 1:
				long ticks = pb_id  * 10000;
				xmlBuilder = new StringBuilder("<PB id=\"" + pb_id  + "\">");
				xmlBuilder.append("<heartbeat>");
				xmlBuilder.append(ticks);
				xmlBuilder.append("</heartbeat>");
				xmlBuilder.append("</PB>");
				xmlString = xmlBuilder.toString();
				//logger.error(xmlString);
				byte[] heartBeatMessageBytes = getBytesUTF16LE(xmlString);
				//System.out.println("SENDING HEARTBEAT TO 360");
				sendBytes(heartBeatMessageBytes);
				break;
			default:
				//System.out.println("EVENT MESSAGE");

				xmlBuilder = new StringBuilder("<PB id=\"" + pb_id  + "\">");
				xmlBuilder.append(xmlMessage);
				xmlBuilder.append("</PB>");
				xmlString = xmlBuilder.toString();
				if(Integer.parseInt(ServerConstants.configuration.get("threesixty_log_enabled")) == 1) {
					String logMsg = "TO 360 ::" + xmlString;
					if(mailId != 0) {
						logMsg = "MailId=" + mailId + " Lane=" + machineId + " " + logMsg;
					}
					logger.info(logMsg);
				}
				byte[] eventMessageBytes = getBytesUTF16LE(xmlString);
				sendBytes(eventMessageBytes);
				break;
		}
		//sendBytes(messageBytes);
	}
	public void sendXmlMessage(String xmlMessage){
		//messageId=0 //no use
		//mailId=0 //no use
		//mailId=0 //no use
		try{
			sendMessage(0,xmlMessage,0,0);
		}
		catch (Exception ex){
			//logger.error("[SEND_FROM_360CLIENT] "+ex.toString());
		}


	}
	public long getPBId() {
		if(currentPBId>0){
			currentPBId++;
		}
		else{
			Instant instant_now = Instant.now();
			long epm_now = instant_now.toEpochMilli();
			Instant instant_360 = Instant.parse( "2013-01-01T00:00:00.000Z");
			long epm_360 = instant_360.toEpochMilli();
			currentPBId = (epm_now - epm_360);
		}
		return currentPBId;
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



	//Not Need here, just placeholder
	private void sendSyncMessage() {
		try {
			sendMessage(116, "", 0, 0);
		} catch (IOException e) {
			logger.error(e.toString());
		}
	}

	private void startReconnectThread() {
		//send a test signal
		// You may or may not want to stop the thread here
		Thread reconnectThread = new Thread(() -> {
			while(!ThreeSixtyClient.this.isRunning()) {
				ThreeSixtyClient.this.start();

				try {
					sleep(ThreeSixtyClient.this.reconnectDelayMillis);
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
			//boolean threadClosed = false;
			while (ThreeSixtyClient.this.isRunning()) {
				//if(ThreeSixtyClient.this.pingPongCounter < 3) {
					try {
						ThreeSixtyClient.this.sendMessage(1, "", 0, 0);
						//ThreeSixtyClient.this.pingPongCounter++;
					} catch (IOException e) {
						logger.error(e.toString());
						//System.out.print("Ping send failed\n");
					}
				/*} else {
					if(ThreeSixtyClient.this.isRunning()) {
						ThreeSixtyClient.this.interrupt();
						Thread.currentThread().interrupt();
						threadClosed = true;
					}
				}*/

				try {
					//if(!threadClosed) {
						sleep(ThreeSixtyClient.this.pingDelayMillisFirstPart);
						notifyListeners("Server", "ping-sent");
						sleep(ThreeSixtyClient.this.pingDelayMillisSecondPart);
					//}
				} catch (InterruptedException e) {
					logger.error(e.toString());
					//e.printStackTrace();
					//System.out.print("Sleep interrupted in ping thread ID = " + Thread.currentThread().getId() + "\n");
				}
			}
		});

		pingThread.start();
	}

}
