package aclusterllc.adta;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ServerConstants {
    public static final CustomMap<Integer, String> MESSAGE_IDS = new CustomMap<>();
    public static final CustomMap<Integer, String> BIN_REJECT_CODES  = new CustomMap<>();
    public static final CustomMap<Integer, String> BARCODE_TYPES  = new CustomMap<>();
    public static final CustomMap<Integer, String> CONVERTED_BARCODES_FOR_360  = new CustomMap<>();
    public static final CustomMap<Integer, String> SYSTEM_STATES  = new CustomMap<>();
    public static final CustomMap<Integer, String> SYSTEM_MODES  = new CustomMap<>();
    public static final CustomMap<Integer, String> DB_TABLES = new CustomMap<>();
    public static final CustomMap<Integer, Integer> BIN_OPPOSITE_STATUS = new CustomMap<>();
    public static final List<Integer> INPUTS_ERRORS_JAMS_DEVICES = Arrays.asList(2, 4, 5, 14);
    public static final List<Integer> MESSAGES_WITH_SIZE_TABLE = Arrays.asList(6, 8, 10, 12, 17, 40, 42, 46);
    public static final List<Integer> SINGLE_STATUS_CHANGE_MESSAGES = Arrays.asList(3, 7, 9, 11, 13, 15, 18, 41, 43, 47);
    private static final HydenConfigs hydenConfigs = new HydenConfigs();
    public static Map<String, String> configuration;
    public static final String noReadBarcodeString = "??????????";
    public static final String multipleReadBarcodeString = "9999999999";
    public static final String noCodeBarcodeString = "0000000000";

    //NEW: ConfirmationPeBlocked=6, ConfirmationPeUnblocked=1, BinEnabled=2, BinPartiallyFull=3, BinFull=5, BinDisabled=4, BinMode: Auto=0, Manual=7[Highest]
    public static final int binModeAuto = 0;
    public static final int binModeManual = 8;
    public static final int confirmationPeBlockedState = 7;
    public static final int confirmationPeUnBlockedState = 1;
    public static final int binEnabledState = 2;
    public static final int binPartiallyFullState = 3;
    public static final int binFullState = 5;
    public static final int binDisabledState = 4;
    public static final int binClearState = 9;
    public static final int binTrayMissingState = 6;

    public static Map<Integer, Integer> plcConnectStatus = new CustomMap<>();
    public static int threeSixtyClientConnected = 0;
    public static int serverForIngramConnected = 0;

    public static final Map<Integer, String> machine_stopped_reasons  = new HashMap<>();
    public static int machineStoppedReason=3;
    public static int beltStatusSpeed=0;



    static {
        try {
            //0 for jar/exe, 1 for IDE
            configuration = hydenConfigs.getPropValues(1);
        } catch (IOException e) {
            //e.printStackTrace();
            //logger.error(e.toString());
        }

        plcConnectStatus.put(1, 0);
        MESSAGE_IDS.put(1, "System State Message");
        MESSAGE_IDS.put(2, "Inputs Message");
        MESSAGE_IDS.put(3, "Input Change Message");
        MESSAGE_IDS.put(4, "Errors Message");
        MESSAGE_IDS.put(5, "Jams Message");
        MESSAGE_IDS.put(6, "ConfirmationPEBlocked Message");
        MESSAGE_IDS.put(7, "ConfirmationPEBlocked change Message");
        MESSAGE_IDS.put(8, "BinPartiallyFull Message");
        MESSAGE_IDS.put(9, "BinPartiallyFull change Message");
        MESSAGE_IDS.put(10, "BinFull Message");
        MESSAGE_IDS.put(11, "BinFull change Message");
        MESSAGE_IDS.put(12, "BinDisabled Message");
        MESSAGE_IDS.put(13, "BinDisabled change Message");
        MESSAGE_IDS.put(14, "Devices Connected Message");
        MESSAGE_IDS.put(15, "Device Connected Change Message");
        MESSAGE_IDS.put(16, "Sync Response");
        MESSAGE_IDS.put(17, "TrayMissing Message");
        MESSAGE_IDS.put(18, "TrayMissing change Message");
        MESSAGE_IDS.put(20, "Dimension Message");
        MESSAGE_IDS.put(21, "Barcode Result");
        MESSAGE_IDS.put(22, "ConfirmDestination Message");
        MESSAGE_IDS.put(30, "Ping Response");
        MESSAGE_IDS.put(40, "BinMode Message");
        MESSAGE_IDS.put(41, "BinModechange Message");
        MESSAGE_IDS.put(42, "ConveyorState Message");
        MESSAGE_IDS.put(43, "ConveyorStateChange Message");
        MESSAGE_IDS.put(44, "SensorHit Message");
        MESSAGE_IDS.put(45, "Event Message");
        MESSAGE_IDS.put(46, "InductLineState Message");
        MESSAGE_IDS.put(47, "InductLineStateChange Message");
        MESSAGE_IDS.put(48, "PieceInducted Message");
        //MESSAGE_IDS.put(50, "EStop Message");


        MESSAGE_IDS.put(120, "SetMode Message");
        MESSAGE_IDS.put(101, "Request Inputs State Message");
        MESSAGE_IDS.put(102, "Request Errors Message");
        MESSAGE_IDS.put(103, "Request Jams Message");
        MESSAGE_IDS.put(105, "Request ConfirmationPEBlocked Message");
        MESSAGE_IDS.put(106, "Request BinPartiallyFull Message");
        MESSAGE_IDS.put(107, "Request BinFull Message");
        MESSAGE_IDS.put(108, "Request BinDisabled Message");
        MESSAGE_IDS.put(109, "Request DevicesConnected State Message");
        MESSAGE_IDS.put(110, "Request BinMode Message");
        MESSAGE_IDS.put(111, "SetBinMode Message");
        MESSAGE_IDS.put(112, "Request ConveyorState Message");
        MESSAGE_IDS.put(113, "Request TrayMissing Message");
        MESSAGE_IDS.put(114, "Request InductLineState Message");
        MESSAGE_IDS.put(116, "Sync Request");
        MESSAGE_IDS.put(130, "Ping Request");

        BIN_REJECT_CODES.put(0,"No Reject");
        BIN_REJECT_CODES.put(1,"Mechanical reject");
        BIN_REJECT_CODES.put(2, "Double");
        BIN_REJECT_CODES.put(3, "Out of Skew");
        BIN_REJECT_CODES.put(4, "Not Justify");

        BARCODE_TYPES.put(0, "Unknown");
        BARCODE_TYPES.put(1, "Aztec");
        BARCODE_TYPES.put(2,"Code 93");
        BARCODE_TYPES.put(3,"Code 128");
        BARCODE_TYPES.put(4,"Code 39");
        BARCODE_TYPES.put(5,"EAN/UCC 128");
        BARCODE_TYPES.put(6,"EAN 13");
        BARCODE_TYPES.put(7,"UPCE");
        BARCODE_TYPES.put(8,"PDF 417");
        BARCODE_TYPES.put(9,"Matrix 2of5");
        BARCODE_TYPES.put(10,"Planet");
        BARCODE_TYPES.put(11,"Code EAN8");
        BARCODE_TYPES.put(12,"Address Block IMB");
        BARCODE_TYPES.put(13,"Clear Zone IMB");
        BARCODE_TYPES.put(14,"Codabar");
        BARCODE_TYPES.put(15,"DataMatrix");
        BARCODE_TYPES.put(16,"2of5 Interleaved");
        BARCODE_TYPES.put(17,"Straight 2of5");
        BARCODE_TYPES.put(18,"Address Block Postnet");
        BARCODE_TYPES.put(19,"Clear Zone Postnet");
        BARCODE_TYPES.put(20,"UPCA");
        BARCODE_TYPES.put(21,"Maxi Code");
        BARCODE_TYPES.put(22,"Intelligent Mail");
        BARCODE_TYPES.put(23,"UPU");
        BARCODE_TYPES.put(24,"QR code");


        CONVERTED_BARCODES_FOR_360.put(0,"BARCODE/UNKNOWN");
        CONVERTED_BARCODES_FOR_360.put(1,"BARCODE/AZTEC");
        CONVERTED_BARCODES_FOR_360.put(2,"BARCODE/CODE93");
        CONVERTED_BARCODES_FOR_360.put(3,"BARCODE/CODE128");
        CONVERTED_BARCODES_FOR_360.put(4,"BARCODE/CODE3OF9");
        CONVERTED_BARCODES_FOR_360.put(5,"BARCODE/UCCEAN128");
        CONVERTED_BARCODES_FOR_360.put(6,"BARCODE/EAN13");
        CONVERTED_BARCODES_FOR_360.put(7,"BARCODE/UPC");
        CONVERTED_BARCODES_FOR_360.put(8,"BARCODE/PDF417");
        CONVERTED_BARCODES_FOR_360.put(9,"BARCODE/MATRIX2OF5");
        CONVERTED_BARCODES_FOR_360.put(10,"BARCODE/PLANET");
        CONVERTED_BARCODES_FOR_360.put(11,"BARCODE/EAN8");
        CONVERTED_BARCODES_FOR_360.put(12,"BARCODE/BNB-62");
        CONVERTED_BARCODES_FOR_360.put(13,"BARCODE/IMB");
        CONVERTED_BARCODES_FOR_360.put(14,"BARCODE/CODABAR");
        CONVERTED_BARCODES_FOR_360.put(15,"BARCODE/DATAMATRIX");
        CONVERTED_BARCODES_FOR_360.put(16,"BARCODE/INTERLEAVE2OF5");
        CONVERTED_BARCODES_FOR_360.put(17,"BARCODE/STRAIGHT2OF5");
        CONVERTED_BARCODES_FOR_360.put(18,"BARCODE/POSTNET");
        CONVERTED_BARCODES_FOR_360.put(19,"BARCODE/POSTNET");
        CONVERTED_BARCODES_FOR_360.put(20,"BARCODE/S18E");
        CONVERTED_BARCODES_FOR_360.put(21,"BARCODE/MAXICODE");
        CONVERTED_BARCODES_FOR_360.put(22,"BARCODE/IMB");
        CONVERTED_BARCODES_FOR_360.put(23,"BARCODE/S18D");
        CONVERTED_BARCODES_FOR_360.put(24,"BARCODE/QR_CODE");


        SYSTEM_STATES.put(0, "Not Ready");
        SYSTEM_STATES.put(1, "Ready");
        SYSTEM_STATES.put(2, "Starting");
        SYSTEM_STATES.put(3, "Running");
        SYSTEM_STATES.put(4, "Stopping");

        SYSTEM_MODES.put(0, "Auto");
        SYSTEM_MODES.put(1, "Manual");


        //NEW: ConfirmationPeBlocked=6, ConfirmationPeUnblocked=1, BinEnabled=2, BinPartiallyFull=3, BinFull=5, BinDisabled=4, BinMode: Auto=0, Manual=7[Highest]
        BIN_OPPOSITE_STATUS.put(confirmationPeUnBlockedState, confirmationPeBlockedState);
        BIN_OPPOSITE_STATUS.put(confirmationPeBlockedState, confirmationPeUnBlockedState);

        BIN_OPPOSITE_STATUS.put(binPartiallyFullState, binFullState);
        BIN_OPPOSITE_STATUS.put(binFullState, binPartiallyFullState);

        BIN_OPPOSITE_STATUS.put(binEnabledState, binDisabledState);
        BIN_OPPOSITE_STATUS.put(binDisabledState, binEnabledState);

        BIN_OPPOSITE_STATUS.put(binModeAuto, binModeManual);
        BIN_OPPOSITE_STATUS.put(binModeManual, binModeAuto);

        DB_TABLES.put(2, "input_states");
        DB_TABLES.put(3, "input_states");
        DB_TABLES.put(4, "active_alarms");
        DB_TABLES.put(5, "active_alarms");
        DB_TABLES.put(14, "device_states");
        DB_TABLES.put(15, "device_states");
        DB_TABLES.put(42, "conveyor_states");
        DB_TABLES.put(43, "conveyor_states");
        DB_TABLES.put(44, "sensor_hits");
        DB_TABLES.put(45, "event_states");
        DB_TABLES.put(46, "induct_states");
        DB_TABLES.put(47, "induct_states");

        machine_stopped_reasons.put(0,"Jam");
        machine_stopped_reasons.put(1,"Error");
        machine_stopped_reasons.put(2,"Estop");
        machine_stopped_reasons.put(3,"User Interface");
        machine_stopped_reasons.put(4,"Autho To Start");
    }
}
