package aclusterllc.adta;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.swing.*;
import java.awt.*;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

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
    private JCheckBox chk_log_plc_msg;
    private Client mainClient;
    private boolean alreadyConnected = false;
    Logger logger = LoggerFactory.getLogger(MainGui.class);

    public MainGui() {
        clearButton.addActionListener(actionEvent -> clearMainTextArea());
        chk_log_plc_msg.addActionListener(actionPerformed-> { ServerConstants.log_plc_messages=chk_log_plc_msg.isSelected();});
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
        }
    }
    public void appendToMainTextArea(String message){
        LocalDateTime now = LocalDateTime.now();
        DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("dd-MM-yyyy HH:mm:ss");
        String displayMessage = String.format("[%s] %s",now.format(dateTimeFormatter),message);
        mainTextArea.append(displayMessage+"\r\n");
    }

    public void startGui() {
        String version_Info="ADTA 1.0.32";
        logger.info("=====================================");
        logger.info(version_Info);
        logger.info("=====================================");
        JFrame frame = new JFrame(version_Info);
        if(Integer.parseInt(ServerConstants.configuration.get("java_server_minimized"))==1){
            frame.setState(Frame.ICONIFIED);
        }
        frame.setContentPane(this.mainPanel);
        frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        frame.pack();
        frame.setLocationRelativeTo(null);
        frame.setVisible(true);
    }
}