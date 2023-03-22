package aclusterllc.adta;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.swing.*;
import java.io.IOException;

public class MainGui {
    public JTextArea mainTextArea;
    private JTextArea sendTextArea;
    private JButton sendButton;
    private JButton clearButton;
    private JButton connectButton;
    private JButton disconnectButton;
    private JPanel mainPanel;
    private JScrollPane mainScrollPane;
    private JComboBox<ComboItem> specialMessage;
	private JLabel feedLabel;
    public JLabel pingLabel;
    private Client mainClient;
    private boolean alreadyConnected = false;
    Logger logger = LoggerFactory.getLogger(MainGui.class);

    public MainGui() {

        //mainClient = client;

        /*connectButton.addActionListener(actionEvent -> {
            if(mainClient.isStopped()) {
                connectClient();
            } else {
                System.err.println("Client already connected");
            }
        });

        disconnectButton.addActionListener(actionEvent -> {
            if(mainClient.isRunning()){
                try {
                    disconnectClient();
                    alreadyConnected = false;
                } catch (IOException e) {
                    e.printStackTrace();
                }
            } else {
                System.err.println("Client already disconnected");
            }
        });*/
        clearButton.addActionListener(actionEvent -> clearMainTextArea());
        /*sendButton.addActionListener(actionEvent -> {
            sendMessage();
        });*/
    }

    public void connectClient() {
        mainClient.start();
    }

    public void disconnectClient() throws IOException {
        mainClient.interrupt();
    }

    public void clearMainTextArea() {
        mainTextArea.setText("");
    }

    public void sendMessage() {
        Object item = specialMessage.getSelectedItem();
        int selectedMessageId = item != null ? ((ComboItem) item).getValue() : 0;

        System.err.print(selectedMessageId);


        if(selectedMessageId != 0) {
            new Thread(() -> {
                int thread_runner = 0;

                while(thread_runner < 1000) {
                    try {
                        mainClient.sendMessage(selectedMessageId);
                    } catch (IOException e) {
                        //e.printStackTrace();
                        logger.error(e.toString());
                    }

                    try {
                        Thread.sleep(250);
                    } catch (InterruptedException e) {
                        //e.printStackTrace();
                        logger.error(e.toString());
                    }

                    thread_runner++;
                }
            }).start();
            //this.mainTextArea.append("Self: " + str + "\r\n");
        }
        /*if(!str.isEmpty() && mainClient != null) {
            mainClient.send(str);
            sendTextArea.setText("");
        } else {
            System.err.println("Main client is stopped");
        }*/
    }

    public void startGui() {
        /*specialMessage.addItem(new ComboItem("Select type", 0));
        specialMessage.addItem(new ComboItem("Sensors Message", 2));
        specialMessage.addItem(new ComboItem("Errors Message", 4));
        specialMessage.addItem(new ComboItem("Jams Message", 5));*/

        JFrame frame = new JFrame("ADTA 1.0.4");
        frame.setContentPane(this.mainPanel);
        frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        frame.pack();
        frame.setLocationRelativeTo(null);
        frame.setVisible(true);
    }
}