package aclusterllc.adta;

import javax.swing.text.BadLocationException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;



public class TextAreaListener implements ClientListener{
    private MainGui textAreaHolder;
    public TextAreaListener(MainGui mainGui) {
        textAreaHolder = mainGui;
    }
    public static final String DOT = "\u26AB";

    @Override
    public void update(String socketName, String msg) {

        if((!msg.contains("ping-rec")) && (!msg.contains("ping-sent"))) {
            LocalDateTime myDateObj = LocalDateTime.now();
            DateTimeFormatter myFormatObj = DateTimeFormatter.ofPattern("dd-MM-yyyy HH:mm:ss");

            String formattedDate = "[" + myDateObj.format(myFormatObj) + "] ";
            String fullMsg = String.format("%s %s :: %s", formattedDate, socketName, msg);


            int SCROLL_BUFFER_SIZE = 199;
            int numLinesToTrunk = this.textAreaHolder.mainTextArea.getLineCount() - SCROLL_BUFFER_SIZE;
            if (numLinesToTrunk > 0) {
                try {
                    int posOfLastLineToTrunk = this.textAreaHolder.mainTextArea.getLineEndOffset(numLinesToTrunk - 1);
                    this.textAreaHolder.mainTextArea.replaceRange("", 0, posOfLastLineToTrunk);
                } catch (BadLocationException ex) {
                    ex.printStackTrace();
                }
            }

            this.textAreaHolder.mainTextArea.append(fullMsg + "\r\n");
        } else {
            if(msg.contains("ping-rec")) {
                this.textAreaHolder.pingLabel.setText(DOT);
            } else if(msg.contains("ping-sent")) {
                this.textAreaHolder.pingLabel.setText("");
            }
        }
    }
}