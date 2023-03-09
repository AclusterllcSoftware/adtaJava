package aclusterllc.adta;

import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

import static java.lang.String.format;

public class DatabaseWrapper {
    static Connection dbConn = null;
    private DatabaseHandler dbHandler;
    private DBCache dbCache = DBCache.getInstance();
    private Map<Integer, Timestamp> machineStatsTimestamps = new HashMap<>();
    Logger logger = LoggerFactory.getLogger(DatabaseWrapper.class);
    public DatabaseWrapper(DatabaseHandler databaseHandler) {
        dbHandler = databaseHandler;
    }

    public boolean updateMachineStateMode(int state, int mode, int machineId) {
        //int machineId = 1;

        String sql1 = format("UPDATE machines SET `machine_mode`=%d, `machine_state`=%d WHERE `machine_id`=%d LIMIT 1",
                    mode,
                    state,
                    machineId);

        //System.out.println(sql1);

        dbHandler.append(sql1);


        return true;
    }

    public void insertInputsDevices(int[] bitSeq, String tbl, int messageId, int machineId) {
        //int machineId = 1;
        String columnName = "";

        if(messageId == 2)  columnName = "input";
        else if(messageId == 42) columnName = "conveyor";
        else if(messageId == 46) columnName = "induct";
        else columnName = "device";

        String bigInsertQuery = format("INSERT IGNORE INTO %s (`machine_id`, `%s_id`, `%s_state`) VALUES ", tbl, columnName, columnName);

        List<String> insertParts = new ArrayList<>();

        List<Integer> machineInputDevices;

        if(messageId == 2) {
            machineInputDevices = dbCache.getMachineInputs(machineId);
        } else if(messageId == 42) {
            machineInputDevices = dbCache.getMachineConveyors(machineId);
        } else if(messageId == 46) {
            machineInputDevices = dbCache.getMachineInducts(machineId);
        } else {
            machineInputDevices = dbCache.getMachineDevices(machineId);
        }
        if(machineInputDevices!=null){
            for(int i=1; i<=bitSeq.length; i++) {
                if(machineInputDevices.contains(i)) {
                    String singleInsertPart = format("(%d, %d, %d)", machineId, i, bitSeq[i - 1]);
                    insertParts.add(singleInsertPart);
                }
            }

            String insertPartsForQuery = String.join(", ", insertParts);

            bigInsertQuery = bigInsertQuery + insertPartsForQuery;

            dbHandler.append(bigInsertQuery);
        }
        else{
            System.out.println("Database Table is empty.Message Id:"+messageId);
            logger.error("Database Table is empty.Message Id:"+messageId);
        }


    }

    public boolean updateInputsDevices(String tbl, String historyTbl, int messageId, int machineId) {
        //int machineId = 1;

        String bigQuery;
        if(messageId == 2) {
            List<Integer> machineHistoryDisabledInputs = dbCache.getMachineHistoryDisabledInputs(machineId);
            String sql1;
            if(machineHistoryDisabledInputs.size() > 0) {
                String listOfHistoryDisabledInputs = machineHistoryDisabledInputs.stream().map(String::valueOf).collect(Collectors.joining(", "));
                sql1 = format("INSERT INTO %s SELECT * FROM %s WHERE machine_id=%d AND input_id NOT IN (%s);", historyTbl, tbl, machineId, listOfHistoryDisabledInputs);
            } else {
                sql1 = format("INSERT INTO %s SELECT * FROM %s WHERE machine_id=%d;", historyTbl, tbl, machineId);
            }

            String sql2 = format("DELETE FROM %s WHERE machine_id=%d ORDER BY id;", tbl, machineId);

            bigQuery = sql1+sql2;
        } else {
            String sql1 = format("INSERT INTO %s SELECT * FROM %s WHERE machine_id=%d;", historyTbl, tbl, machineId);
            String sql2 = format("DELETE FROM %s WHERE machine_id=%d ORDER BY id;", tbl, machineId);

            bigQuery = sql1+sql2;
        }

        dbHandler.append(bigQuery);

        return true;
    }

    public boolean processInputsDevicesStates(int[] bitSeq, int messageId, int machineId) {
        String tblName = ServerConstants.DB_TABLES.get(messageId);
        String historyTblName = tblName + "_history";

        if(updateInputsDevices(tblName, historyTblName, messageId, machineId)) {
            insertInputsDevices(bitSeq, tblName, messageId, machineId);
        }

        return true;
    }

    public void insertAlarms(List<Integer> activeIds, String tbl, int jamErrorType, int machineId) {
        //int machineId = 1;
        if(activeIds.size() > 0) {

            String bigInsertQuery = format("INSERT IGNORE INTO %s (`combo_id`, `machine_id`, `alarm_id`, `alarm_type`) VALUES ", tbl);

            List<String> insertParts = new ArrayList<>();

            for (Integer alarm_id : activeIds) {
                String comboIdString = String.format("%d%d%d", machineId, alarm_id, jamErrorType);
                int comboId = Integer.parseInt(comboIdString);
                String singleInsertPart = format("(%d, %d, %d, %d)", comboId, machineId, alarm_id, jamErrorType);
                insertParts.add(singleInsertPart);
            }

            String insertPartsForQuery = String.join(", ", insertParts);

            bigInsertQuery = bigInsertQuery + insertPartsForQuery;

            dbHandler.append(bigInsertQuery);
        }

    }

    public boolean updateAlarms(List<String> inactiveIds, String tbl, String historyTbl, int jam_error_type, int machineId) {
        //int machineId = 1;
        if(inactiveIds.size() > 0) {
            String inactiveIdstogether = String.join(", ", inactiveIds);
            String sql1 = format("INSERT INTO %s (`id`, `combo_id`, `machine_id`, `alarm_id`, `alarm_type`, `date_active`) SELECT * FROM %s WHERE machine_id=%d AND alarm_id IN (%s) AND alarm_type=%d;",
                        historyTbl,
                        tbl,
                        machineId,
                        inactiveIdstogether,
                        jam_error_type);

            String sql2 = format("DELETE FROM %s WHERE machine_id=%d AND alarm_id IN (%s) AND alarm_type=%d  ORDER BY id;",
                    tbl,
                    machineId,
                    inactiveIdstogether,
                    jam_error_type);

            String bigQuery = sql1+sql2;
            dbHandler.append(bigQuery);
        }

        return true;
    }

    public boolean processAlarms(int[] bitSeq, int messageId, int machineId) {
        //int machineId = 1;
        String tblName = ServerConstants.DB_TABLES.get(messageId);
        String historyTblName = tblName + "_history";

        int jamErrorType = 0;
        if(messageId == 5) {
            jamErrorType = 1;
        }

        List<Integer> machineInputsDevices = dbCache.getMachineAlarms(machineId, jamErrorType);

        List<String> inactiveIds = new ArrayList<>();
        List<Integer> activeIds = new ArrayList<>();
        for(int i=1; i <= bitSeq.length; i++) {
            if(machineInputsDevices.contains(i)) {
                if (bitSeq[i - 1] == 0) {
                    inactiveIds.add(Integer.toString(i));
                } else {
                    activeIds.add(i);
                }
            }
        }

        if(inactiveIds.size() > 0) {
            if(updateAlarms(inactiveIds, tblName, historyTblName, jamErrorType, machineId)) {
                insertAlarms(activeIds, tblName, jamErrorType, machineId);
            }
        }

        return true;
    }


    public void insertBins(List<String> binIds, String tbl, int binStatus, int machineId) {
        //int machineId = 1;
        List<Integer> machineBins = dbCache.getMachineBins(machineId);

        if(binIds.size() > 0) {

            String bigInsertQuery = format("INSERT IGNORE INTO %s (`machine_id`, `bin_id`, `event_type`) VALUES ", tbl);

            List<String> insertParts = new ArrayList<>();

            for (String binId : binIds) {
                if(machineBins.contains(Integer.parseInt(binId))) {
                    String singleInsertPart = format("(%d, %s, %d)", machineId, binId, binStatus);
                    insertParts.add(singleInsertPart);
                }
            }

            String insertPartsForQuery = String.join(", ", insertParts);

            bigInsertQuery = bigInsertQuery + insertPartsForQuery;

            dbHandler.append(bigInsertQuery);
        }

    }

    public boolean updateBins(List<String> binIds, String tbl, String historyTbl, int binStatus, int clearStatus, int machineId) {
        //int machineId = 1;
        if(binStatus != ServerConstants.binClearState && binStatus != ServerConstants.binTrayMissingState) {
            int oppositeBinStatus = ServerConstants.BIN_OPPOSITE_STATUS.get(binStatus);
            if (binIds.size() > 0) {
                String binIdstogether = String.join(", ", binIds);
                //String sql1 = format("INSERT INTO %s SELECT * FROM %s WHERE machine_id=%d AND bin_id IN (%s) AND event_type=%d;",
                String sql1 = format("INSERT INTO %s (`id`, `machine_id`, `bin_id`, `event_type`, `created_at`) SELECT * FROM %s WHERE machine_id=%d AND bin_id IN (%s) AND event_type=%d;",
                        historyTbl,
                        tbl,
                        machineId,
                        binIdstogether,
                        oppositeBinStatus);

                String sql2 = format("DELETE FROM %s WHERE machine_id=%d AND bin_id IN (%s) AND event_type=%d ORDER BY id;",
                        tbl,
                        machineId,
                        binIdstogether,
                        oppositeBinStatus);

                String bigQuery = sql1 + sql2;
                dbHandler.append(bigQuery);
            }
        } else {
            if (binIds.size() > 0) {
                String binIdstogether = String.join(", ", binIds);
                //String sql1 = format("INSERT INTO %s SELECT * FROM %s WHERE machine_id=%d AND bin_id IN (%s) AND event_type=%d;",
                String sql1 = format("INSERT INTO %s (`id`, `machine_id`, `bin_id`, `event_type`, `created_at`) SELECT * FROM %s WHERE machine_id=%d AND bin_id IN (%s) AND event_type=%d;",
                        historyTbl,
                        tbl,
                        machineId,
                        binIdstogether,
                        clearStatus);

                String sql2 = format("DELETE FROM %s WHERE machine_id=%d AND bin_id IN (%s) AND event_type=%d ORDER BY id;",
                        tbl,
                        machineId,
                        binIdstogether,
                        clearStatus);

                String bigQuery = sql1 + sql2;
                dbHandler.append(bigQuery);
            }

        }

        return true;
    }

    public boolean processBins(int[] bitSeq, int messageId, int machineId) {
        //XXXX: ConfirmationPeBlocked=1, ConfirmationPeUnblocked=2, BinEnabled=3, BinPartiallyFull=4, BinFull=5, BinDisabled=6
        //NEW: ConfirmationPeBlocked=6, ConfirmationPeUnblocked=1, BinEnabled=2, BinPartiallyFull=3, BinFull=5, BinDisabled=4, BinMode: Auto=0, Manual=7[Highest]
        //System.out.println("Message Id = " + messageId + ", Bins Bit Sequence" + Arrays.toString(bitSeq));
        String tblName = "bin_states";
        String historyTblName = tblName + "_history";

        if(messageId == 6) {
            //ConfirmationPeBlocked=6, ConfirmationPeUnblocked=1
            List<String> peBlockedIds = new ArrayList<>();
            List<String> peUnblockedIds = new ArrayList<>();
            for(int i=1; i <= bitSeq.length; i++) {
                if(bitSeq[i-1] == 0) {
                    peUnblockedIds.add(Integer.toString(i));
                } else {
                    peBlockedIds.add(Integer.toString(i));
                }
            }

            if(peBlockedIds.size() > 0) {
                if(updateBins(peBlockedIds, tblName, historyTblName, ServerConstants.confirmationPeBlockedState, 0, machineId)) {
                    insertBins(peBlockedIds, tblName, ServerConstants.confirmationPeBlockedState, machineId);
                }
            }

            if(peUnblockedIds.size() > 0) {
                if(updateBins(peUnblockedIds, tblName, historyTblName, ServerConstants.confirmationPeUnBlockedState, 0, machineId)) {
                    insertBins(peUnblockedIds, tblName, ServerConstants.confirmationPeUnBlockedState, machineId);
                }
            }
        } else if(messageId == 8 || messageId == 10 || messageId == 17) {
            //BinPartiallyFull=3, BinFull=5
            //int binStatus = (messageId == 8) ? ServerConstants.binPartiallyFullState : ServerConstants.binFullState;
            int binStatus = ServerConstants.binPartiallyFullState;

            if(messageId == 10) {
                binStatus = ServerConstants.binFullState;
            } else if(messageId == 17) {
                binStatus = ServerConstants.binTrayMissingState;
            }

            List<String> partiallyOrFullIds = new ArrayList<>();
            List<String> partiallyOrFullClearIds = new ArrayList<>();
            for(int i=1; i <= bitSeq.length; i++) {
                if(bitSeq[i-1] == 1) {
                    partiallyOrFullIds.add(Integer.toString(i));
                } else {
                    partiallyOrFullClearIds.add(Integer.toString(i));
                }
            }

            if(partiallyOrFullIds.size() > 0) {
                if(updateBins(partiallyOrFullIds, tblName, historyTblName, binStatus, 0, machineId)) {
                    insertBins(partiallyOrFullIds, tblName, binStatus, machineId);
                }
            }

            if(partiallyOrFullClearIds.size() > 0) {
                updateBins(partiallyOrFullClearIds, tblName, historyTblName, ServerConstants.binClearState, binStatus, machineId);
            }
        } else if(messageId == 12) {
            //BinDisabled=4, BinEnabled=2
            List<String> disalbedIds = new ArrayList<>();
            List<String> enabledIds = new ArrayList<>();
            for(int i=1; i <= bitSeq.length; i++) {
                if(bitSeq[i-1] == 1) {
                    disalbedIds.add(Integer.toString(i));
                } else {
                    enabledIds.add(Integer.toString(i));
                }
            }

            if(disalbedIds.size() > 0) {
                if(updateBins(disalbedIds, tblName, historyTblName, ServerConstants.binDisabledState, 0, machineId)) {
                    insertBins(disalbedIds, tblName, ServerConstants.binDisabledState, machineId);
                }
            }

            if(enabledIds.size() > 0) {

                if(updateBins(enabledIds, tblName, historyTblName, ServerConstants.binEnabledState, 0, machineId)) {
                    insertBins(enabledIds, tblName, ServerConstants.binEnabledState, machineId);
                }
            }
        } else if(messageId == 40) {
            //BinMode Auto=0, Manual=7
            List<String> autoIds = new ArrayList<>();
            List<String> manualIds = new ArrayList<>();
            for(int i=1; i <= bitSeq.length; i++) {
                if(bitSeq[i-1] == 1) {
                    manualIds.add(Integer.toString(i));
                } else {
                    autoIds.add(Integer.toString(i));
                }
            }

            if(autoIds.size() > 0) {
                if(updateBins(autoIds, tblName, historyTblName, ServerConstants.binModeAuto, 0, machineId)) {
                    insertBins(autoIds, tblName, ServerConstants.binModeAuto, machineId);
                }
            }

            if(manualIds.size() > 0) {

                if(updateBins(manualIds, tblName, historyTblName, ServerConstants.binModeManual, 0, machineId)) {
                    insertBins(manualIds, tblName, ServerConstants.binModeManual, machineId);
                }
            }
        }

        return true;
    }


    public boolean processSingleInputDeviceState(int inputOrDeviceId, int stateValue, int messageId, int machineId) {
        //int machineId = 1;
        String tbl = ServerConstants.DB_TABLES.get(messageId);
        String historyTbl = tbl + "_history";

        String columnName = "";
        if(messageId == 3) columnName = "input";
        else if(messageId == 43) columnName = "conveyor";
        else if(messageId == 47) columnName = "induct";
        else columnName = "device";

        List<Integer> machineInputDevices;
        List<Integer> machineHistoryDisabledInputs = dbCache.getMachineHistoryDisabledInputs(machineId);

        if(messageId == 3) {
            machineInputDevices = dbCache.getMachineInputs(machineId);
        } else if(messageId == 43) {
            machineInputDevices = dbCache.getMachineConveyors(machineId);
        } else if(messageId == 47) {
            machineInputDevices = dbCache.getMachineInducts(machineId);
        } else {
            machineInputDevices = dbCache.getMachineDevices(machineId);
        }

        if(machineInputDevices.contains(inputOrDeviceId)) {
            String sql1;
            if(messageId == 3 && machineHistoryDisabledInputs.size() > 0 && machineHistoryDisabledInputs.contains(inputOrDeviceId)) {
                sql1 = "";
            } else {
                sql1 = format("INSERT INTO %s SELECT * FROM %s WHERE machine_id=%d AND %s_id=%d;", historyTbl, tbl, machineId, columnName, inputOrDeviceId);
            }

            String sql2 = format("DELETE FROM %s WHERE machine_id=%d AND %s_id=%d LIMIT 1;", tbl, machineId, columnName, inputOrDeviceId);
            String sql3 = format("INSERT IGNORE INTO %s (`machine_id`, `%s_id`, `%s_state`) VALUES (%d, %d, %d)", tbl, columnName, columnName, machineId, inputOrDeviceId, stateValue);

            String bigQuery = sql1 + sql2 + sql3;

            dbHandler.append(bigQuery);
        }

        return true;
    }

    public boolean processSingleBinState(int binId, int stateValue, int messageId, int machineId) {
        //XXXX: ConfirmationPeBlocked=1, ConfirmationPeUnblocked=2, BinEnabled=3, BinPartiallyFull=4, BinFull=5, BinDisabled=6
        //NEW: ConfirmationPeBlocked=6, ConfirmationPeUnblocked=1, BinEnabled=2, BinPartiallyFull=3, BinFull=5, BinDisabled=4
        //int machineId = 1;
        int binStatus = 0;
        int clearStatus = 0;
        List<Integer> machineBins = dbCache.getMachineBins(machineId);
        if(machineBins.contains(binId)) {
            if (messageId == 7) {
                //ConfirmationPeBlocked=6/Unblocked=1
                if (stateValue == 0) {
                    binStatus = ServerConstants.confirmationPeUnBlockedState;
                } else {
                    binStatus = ServerConstants.confirmationPeBlockedState;
                }
            } else if (messageId == 9) {
                //BinPartiallyFull = 3 Or BinFull = 5
                //binStatus = (messageId == 9) ? 6:3;
                if (stateValue == 0) {
                    binStatus = ServerConstants.binClearState;
                    clearStatus = ServerConstants.binPartiallyFullState;
                } else {
                    binStatus = ServerConstants.binPartiallyFullState;
                }
            } else if (messageId == 11) {
                if (stateValue == 0) {
                    binStatus = ServerConstants.binClearState;
                    clearStatus = ServerConstants.binFullState;
                } else {
                    binStatus = ServerConstants.binFullState;
                }
            } else if (messageId == 18) {
                if (stateValue == 0) {
                    binStatus = ServerConstants.binClearState;
                    clearStatus = ServerConstants.binTrayMissingState;
                } else {
                    binStatus = ServerConstants.binTrayMissingState;
                }
            } else if (messageId == 13) {
                //BinDisabled=4, BinEnabled=2
                if (stateValue == 0) {
                    binStatus = ServerConstants.binEnabledState;
                } else {
                    binStatus = ServerConstants.binDisabledState;
                }
            } else if (messageId == 41) {
                //BinMode Auto=0, Manual=1
                if (stateValue == 0) {
                    binStatus = ServerConstants.binModeAuto;
                } else {
                    binStatus = ServerConstants.binModeManual;
                }
            }

            //if (binStatus != 0) {
                String tbl = "bin_states";
                String historyTbl = tbl + "_history";

                if ((binStatus != 9)) {
                    //for bin full and partialfull and traymissing no need to remove the opposite, just insert it
                    if(messageId == 9 || messageId == 11 || messageId == 18) {
                        String sql = format("INSERT IGNORE INTO %s (`machine_id`, `bin_id`, `event_type`) VALUES (%d, %d, %d)", tbl, machineId, binId, binStatus);

                        dbHandler.append(sql);
                    } else {
                        int oppositeBinStatus = ServerConstants.BIN_OPPOSITE_STATUS.get(binStatus);

                        //String sql1 = format("INSERT INTO %s SELECT * FROM %s WHERE machine_id=%d AND bin_id=%d AND event_type=%d;",
                        String sql1 = format("INSERT INTO %s (`id`, `machine_id`, `bin_id`, `event_type`, `created_at`) SELECT * FROM %s WHERE machine_id=%d AND bin_id=%d AND event_type=%d;",
                                historyTbl,
                                tbl,
                                machineId,
                                binId,
                                oppositeBinStatus);

                        String sql2 = format("DELETE FROM %s WHERE machine_id=%d AND bin_id=%d AND event_type=%d LIMIT 1;",
                                tbl,
                                machineId,
                                binId,
                                oppositeBinStatus);

                        String sql3 = format("INSERT IGNORE INTO %s (`machine_id`, `bin_id`, `event_type`) VALUES (%d, %d, %d)", tbl, machineId, binId, binStatus);

                        String bigQuery = sql1 + sql2 + sql3;
                        dbHandler.append(bigQuery);
                    }

                } else {
                    //String sql1 = format("INSERT INTO %s SELECT * FROM %s WHERE machine_id=%d AND bin_id=%d AND event_type=%d;",
                    String sql1 = format("INSERT INTO %s (`id`, `machine_id`, `bin_id`, `event_type`, `created_at`) SELECT * FROM %s WHERE machine_id=%d AND bin_id=%d AND event_type=%d;",
                            historyTbl,
                            tbl,
                            machineId,
                            binId,
                            clearStatus);

                    String sql2 = format("DELETE FROM %s WHERE machine_id=%d AND bin_id=%d AND event_type=%d LIMIT 1;",
                            tbl,
                            machineId,
                            binId,
                            clearStatus);

                    String bigQuery = sql1 + sql2;
                    dbHandler.append(bigQuery);
                }
            //}
        }

        return true;
    }

    public boolean processSensorHits(long mailId, int machineId) {
        String tbl = "products";
        String overwriteTbl = "overwritten_products";

        String sql1 = format("INSERT INTO %s SELECT * FROM %s WHERE machine_id=%d AND mail_id=%d;", overwriteTbl, tbl, machineId, mailId);
        String sql2 = format("DELETE FROM %s WHERE machine_id=%d AND mail_id=%d LIMIT 1;", tbl, machineId, mailId);
        String sql3 = format("INSERT IGNORE INTO %s (`machine_id`, `mail_id`) VALUES (%d, %d)",
                tbl, machineId, mailId);

        String bigQuery = sql1+sql2+sql3;

        //System.out.println(bigQuery);

        dbHandler.append(bigQuery);

        return true;
    }

    public boolean processDimension(long mailId, long length, long width, long height, long weight, int rejectCode, int machineId) {
        //int machineId = 1;
        String tbl = "products";

        /*String overwriteTbl = "overwritten_products";

        String sql1 = format("INSERT INTO %s SELECT * FROM %s WHERE machine_id=%d AND mail_id=%d;", overwriteTbl, tbl, machineId, mailId);
        String sql2 = format("DELETE FROM %s WHERE machine_id=%d AND mail_id=%d LIMIT 1;", tbl, machineId, mailId);
        String sql3 = format("INSERT IGNORE INTO %s (`machine_id`, `mail_id`, `length`, `width`, `height`, `weight`, `reject_code`) VALUES (%d, %d, %d, %d, %d, %d, %d)",
                tbl, machineId, mailId, length, width, height, weight, rejectCode);

        String bigQuery = sql1+sql2+sql3;*/
        String sql1 = format("UPDATE %s SET length=%d, width=%d, height=%d, weight=%d, reject_code=%d, dimension_at=CURRENT_TIMESTAMP() WHERE machine_id=%d AND mail_id=%d LIMIT 1;",
                tbl, length, width, height, weight, rejectCode, machineId, mailId);
        //System.out.print(sql1);
        String bigQuery = sql1;
        dbHandler.append(bigQuery);

        return true;
    }

    public boolean processBarcodeResult(long mailId, Map<Integer, Map<String, String>> barcodeTypeWithString, int numberOfResults, int machineId) {

        //int machineId = 1;
        String tbl1 = "products";

        int valid_read = 1, no_read = 0, multiple_read = 0, no_code=0;
        //no_code

        if(numberOfResults != 1) {
            if(numberOfResults == 0) {
                //no_read = 1;
                no_code = 1;
            } else {
                multiple_read = 1;
            }

            valid_read = 0;
        }

        List<String> updateColumnQueryParts = new ArrayList<>();
        updateColumnQueryParts.add("`number_of_results`=" + numberOfResults);
        updateColumnQueryParts.add("`barcode_at`=CURRENT_TIMESTAMP()");
        String updateColumnQuery = "";
        if(barcodeTypeWithString.size() > 0) {
            for (Map.Entry<Integer, Map<String, String>> e : barcodeTypeWithString.entrySet()) {
                Integer k = e.getKey();
                Map<String, String> v = e.getValue();
                HashMap<String, String> barcodeAndString = (HashMap<String, String>) v;
                for (Map.Entry<String, String> entry : barcodeAndString.entrySet()) {
                    String k2 = entry.getKey();
                    String v2 = entry.getValue();
                    v2 = v2.replace("'", "");
                    v2 = v2.replace("\"", "");
                    updateColumnQueryParts.add("`" + k2 + "`='" + v2 + "'");
                    if(numberOfResults == 1) {
                        switch (v2) {
                            case ServerConstants.noReadBarcodeString:
                                no_read = 1;
                                valid_read = 0;
                                break;
                            case ServerConstants.multipleReadBarcodeString:
                                multiple_read = 1;
                                valid_read = 0;
                                break;
                            case ServerConstants.noCodeBarcodeString:
                                no_code = 1;
                                valid_read = 0;
                                break;
                        }
                    }
                }
            }
        }

        updateColumnQuery = String.join(", ", updateColumnQueryParts);

        String sql1 = format("UPDATE %s SET %s WHERE machine_id=%d AND mail_id=%d LIMIT 1;", tbl1, updateColumnQuery, machineId, mailId);

        String tbl2 = "statistics";

        String sql21 = format("UPDATE %s SET total_read=total_read+1, no_read=no_read+%d, no_code=no_code+%d, multiple_read=multiple_read+%d, valid=valid+%d WHERE machine_id=%d ORDER BY id DESC LIMIT 1;",
                tbl2,
                no_read,
                no_code,
                multiple_read,
                valid_read,
                machineId);
        String sql22 = format("UPDATE %s SET total_read=total_read+1, no_read=no_read+%d, no_code=no_code+%d, multiple_read=multiple_read+%d, valid=valid+%d WHERE machine_id=%d ORDER BY id DESC LIMIT 1;",
                "statistics_hourly",
                no_read,
                no_code,
                multiple_read,
                valid_read,
                machineId);
        String sql23 = format("UPDATE %s SET total_read=total_read+1, no_read=no_read+%d, no_code=no_code+%d, multiple_read=multiple_read+%d, valid=valid+%d WHERE machine_id=%d ORDER BY id DESC LIMIT 1;",
                "statistics_counter",
                no_read,
                no_code,
                multiple_read,
                valid_read,
                machineId);

        String bigQuery = sql1+sql21+sql22+sql23;

        dbHandler.append(bigQuery);
        return true;
    }

    public boolean processConfirmDestination(long mailId, int destination, int altDestination, int finalDestination, int reason, int machineId) {
        List<Integer> possibleReasons=new ArrayList<>(Arrays.asList(0, 1, 3, 4, 5,6,7,8,9,10,12,14,16,17,18,21));

        String tbl = "products";
        String historyTbl = "product_history";

        String query = format("INSERT INTO %s (`product_id`, " +
                        "`machine_id`, " +
                        "`mail_id`, " +
                        "`length`, " +
                        "`width`, " +
                        "`height`, " +
                        "`weight`, " +
                        "`reject_code`, " +
                        "`number_of_results`, " +
                        "`barcode1_type`, " +
                        "`barcode1_string`, " +
                        "`barcode2_type`, " +
                        "`barcode2_string`, " +
                        "`barcode3_type`, " +
                        "`barcode3_string`, " +
                        "`created_at`, " +
                        "`dimension_at`," +
                        "`barcode_at`," +
                        "`destination`, " +
                        "`alternate_destination`," +
                        "`final_destination`, " +
                        "`reason`)" +
                        "SELECT `id`, " +
                        "`machine_id`, " +
                        "`mail_id`, " +
                        "`length`, " +
                        "`width`, " +
                        "`height`, " +
                        "`weight`, " +
                        "`reject_code`, " +
                        "`number_of_results`, " +
                        "`barcode1_type`, " +
                        "`barcode1_string`, " +
                        "`barcode2_type`, " +
                        "`barcode2_string`, " +
                        "`barcode3_type`, " +
                        "`barcode3_string`, " +
                        "`created_at`, " +
                        "`dimension_at`, " +
                        "`barcode_at`, " +
                        "%d, %d, %d, %d " +
                        " FROM %s WHERE machine_id=%d AND mail_id=%d LIMIT 1;",
                historyTbl,
                destination,
                altDestination,
                finalDestination,
                reason,
                tbl,
                machineId,
                mailId);
        query += format("DELETE FROM %s WHERE machine_id=%d AND mail_id=%d LIMIT 1;", tbl, machineId, mailId);

        JSONObject destBin=null;
        JSONObject finalDestBin=null;
        for(int i = 0; i<DBCache.binsInfo.names().length(); i++){
            JSONObject bin= (JSONObject) DBCache.binsInfo.get(DBCache.binsInfo.names().getString(i));
            if(bin.getInt("sort_manager_id")==destination){
                destBin=bin;
            }
            if(bin.getInt("sort_manager_id")==finalDestination){
                finalDestBin=bin;
            }
        }
        if(destBin!= null && finalDestBin!=null){
            if(possibleReasons.contains(reason)) {
                String scColumn = "sc" + Integer.toString(reason);
                String recircUpdate="";
                String rejectUpdate="";

                //statistics
                if(finalDestBin.getInt("recirc_bin")==1){
                    recircUpdate=" ,recirc=recirc+1";
                }
                else if(finalDestBin.getInt("reject_bin")==1){
                    rejectUpdate=" ,reject=reject+1";
                }
                query += format("UPDATE %s SET %s=%s+1%s%s WHERE machine_id=%d ORDER BY id DESC LIMIT 1;","statistics",scColumn,scColumn,recircUpdate,rejectUpdate,machineId);
                query += format("UPDATE %s SET %s=%s+1%s%s WHERE machine_id=%d ORDER BY id DESC LIMIT 1;","statistics_counter",scColumn,scColumn,recircUpdate,rejectUpdate,machineId);
                query += format("UPDATE %s SET %s=%s+1%s%s WHERE machine_id=%d ORDER BY id DESC LIMIT 1;","statistics_hourly",scColumn,scColumn,recircUpdate,rejectUpdate,machineId);

//                //bin statistics
//                //update short code for all condition
                //if((destBin.getInt("reject_bin")==1)||(destBin==finalDestBin))
                {
                    query += format("UPDATE %s SET %s=%s+1 WHERE machine_id=%d AND bin_id=%d ORDER BY id DESC LIMIT 1;", "statistics_bins", scColumn, scColumn, machineId, finalDestBin.getInt("bin_id"));
                    query += format("UPDATE %s SET %s=%s+1 WHERE machine_id=%d AND bin_id=%d ORDER BY id DESC LIMIT 1;", "statistics_bins_counter", scColumn, scColumn, machineId, finalDestBin.getInt("bin_id"));
                    query += format("UPDATE %s SET %s=%s+1 WHERE machine_id=%d AND bin_id=%d ORDER BY id DESC LIMIT 1;", "statistics_bins_hourly", scColumn, scColumn, machineId, finalDestBin.getInt("bin_id"));
                }
                if((destBin.getInt("reject_bin")!=1)&&(destBin!=finalDestBin))
                {
                    query += format("UPDATE %s SET %s=%s+1%s%s WHERE machine_id=%d AND bin_id=%d ORDER BY id DESC LIMIT 1;", "statistics_bins", scColumn, scColumn,recircUpdate,rejectUpdate, machineId, destBin.getInt("bin_id"));
                    query += format("UPDATE %s SET %s=%s+1%s%s WHERE machine_id=%d AND bin_id=%d ORDER BY id DESC LIMIT 1;", "statistics_bins_counter", scColumn, scColumn,recircUpdate,rejectUpdate, machineId, destBin.getInt("bin_id"));
                    query += format("UPDATE %s SET %s=%s+1%s%s WHERE machine_id=%d AND bin_id=%d ORDER BY id DESC LIMIT 1;", "statistics_bins_hourly", scColumn, scColumn,recircUpdate,rejectUpdate, machineId, destBin.getInt("bin_id"));
                }
            }
        }
        else{
            logger.error("DestBin or FinalDestBin is null");
        }
        dbHandler.append(query);
        return true;
    }

    public boolean processEvents(long eventId, int machineId) {
        //int machineId = 1;
        String tbl = "event_states";
        //System.out.println("E:" + eventId + " M:" + machineId);

        String sql3 = format("INSERT INTO %s (`machine_id`, `event_id`) VALUES (%d, %d)",
                tbl, machineId, eventId);

        //System.out.println(sql3);
        String bigQuery = sql3;

        dbHandler.append(bigQuery);

        return true;
    }

    public boolean processPieceInducted(int inductId, int machineId) {
        List<Integer> inducts =new ArrayList<>(Arrays.asList(1, 2, 3, 4, 5, 6, 7));
        String sql11 = "";
        String sql12 = "";
        String sql13 = "";
        String tbl = "statistics";

        if(inducts.contains(inductId)) {
            String columnName = "i" + Integer.toString(inductId);

            sql11 = format("UPDATE %s SET %s=%s+1 WHERE machine_id=%d ORDER BY id DESC LIMIT 1;",
                    tbl,
                    columnName,
                    columnName,
                    machineId);
            sql12 = format("UPDATE %s SET %s=%s+1 WHERE machine_id=%d ORDER BY id DESC LIMIT 1;",
                    "statistics_hourly",
                    columnName,
                    columnName,
                    machineId);
            sql13 = format("UPDATE %s SET %s=%s+1 WHERE machine_id=%d ORDER BY id DESC LIMIT 1;",
                    "statistics_counter",
                    columnName,
                    columnName,
                    machineId);
        }
        String bigQuery = sql11+sql12+sql13;
        dbHandler.append(bigQuery);
        return true;
    }
}

