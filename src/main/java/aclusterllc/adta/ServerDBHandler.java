package aclusterllc.adta;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.json.JSONArray;
import org.json.JSONObject;

import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.*;
import java.util.Date;
import java.util.concurrent.TimeUnit;
import static java.lang.Math.ceil;
import static java.lang.Math.floorDiv;
import static java.lang.String.format;

public class ServerDBHandler {

    static Connection dbConn = null;
    private DBCache dbCache = DBCache.getInstance();
    Logger logger=LoggerFactory.getLogger(ServerDBHandler.class);
    public ServerDBHandler(){

        try {
            Class.forName("com.mysql.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            logger.error(e.toString());
            //e.printStackTrace();
        }
    }

    public JSONObject getMachineList() {

        JSONObject mainJson = new JSONObject();
        mainJson.put("type", "ip_list");
        JSONObject resultJson = new JSONObject();

        for (Map.Entry<Integer, Map<String, String>> entry : dbCache.getMachineList().entrySet()) {
            Map<String, String> v = entry.getValue();

            int machineID = entry.getKey();
            String ipAddress = v.get("ip_address");
            String machineName = v.get("machine_name");
            String maintenanceIp = v.get("maintenance_ip");

            JSONObject machineJson = new JSONObject();
            machineJson.put("ip_address", ipAddress);
            machineJson.put("machine_name", machineName);
            machineJson.put("maintenance_ip", maintenanceIp);

            resultJson.put(String.valueOf(machineID), machineJson);
        }

        mainJson.put("result", resultJson);
        return mainJson;
    }
    public JSONObject getModSort(int machineId, int device_type, int device_number) throws ParseException {
        JSONObject mainJson = new JSONObject();
        mainJson.put("type", "mod_sort");
        JSONObject resultJson = new JSONObject();
        JSONObject inputsJson = new JSONObject();

        try {
            dbConn = DataSource.getConnection();
            String inputsQuery = String.format("SELECT input_id, input_state FROM input_states WHERE machine_id=%d AND input_id IN (SELECT input_id FROM inputs WHERE device_type=%d AND device_number=%d AND machine_id=%d) ORDER BY id ASC", machineId, device_type, device_number, machineId);
            Statement stmt = dbConn.createStatement();
            ResultSet rs = stmt.executeQuery(inputsQuery);

            while (rs.next())
            {
                int inputID = rs.getInt("input_id");
                String description = dbCache.getInputDescription(machineId, inputID);
                int inputState = rs.getInt("input_state");
                inputsJson.put(description, inputState);
            }

            JSONObject alarmsJson = new JSONObject();
            String alarmsQuery = String.format("SELECT machine_id, alarm_id, alarm_type, date_active, UNIX_TIMESTAMP(date_active) AS date_active_ts FROM active_alarms WHERE machine_id=%d ORDER BY id DESC LIMIT 5", machineId);
            rs = stmt.executeQuery(alarmsQuery);

            while (rs.next())
            {
                String comboId = String.format("%d%d%d", rs.getInt("machine_id"), rs.getInt("alarm_id"), rs.getInt("alarm_type"));
                Map<String, String> singleAlarmData = dbCache.getAlarmData(comboId);

                String dateActive = rs.getTimestamp("date_active").toString();
                Date date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(dateActive);
                Date nowDate = new Date();
                long duration = getDuration(date, nowDate, TimeUnit.SECONDS);

                String dateActiveTs = rs.getString("date_active_ts");

                JSONObject singleAlarmJson = new JSONObject();

                if(singleAlarmData.size() > 0) {
                    singleAlarmData.forEach(singleAlarmJson::put);
                    singleAlarmJson.put("timestamp", dateActiveTs);
                    singleAlarmJson.put("duration", duration);
                    alarmsJson.put(comboId, singleAlarmJson);
                }
            }
            resultJson.put("alarms", alarmsJson);

            resultJson.put("inputs", inputsJson);

            int machineMode = getMachineMode(stmt, machineId);

            resultJson.put("mode", machineMode);

            mainJson.put("result", resultJson);

            rs.close();
            stmt.close();
            dbConn.close(); // connection close

        } catch (SQLException e) {
            logger.error(e.toString());
            //e.printStackTrace();
        }

        return mainJson;
    }

    public JSONObject getInduct(int machineId, int induct_id) throws ParseException {
        JSONObject mainJson = new JSONObject();
        mainJson.put("type", "induct");
        JSONObject resultJson = new JSONObject();
        JSONObject inputsJson = new JSONObject();
        JSONObject conveyorsJson = new JSONObject();

        try {
            dbConn = DataSource.getConnection();
            String inputsQuery = String.format("SELECT input_id, input_state FROM input_states WHERE machine_id=%d AND input_id IN (SELECT input_id FROM inputs WHERE device_type=3 AND device_number=%d AND machine_id=%d) ORDER BY id ASC", machineId, induct_id, machineId);
            Statement stmt = dbConn.createStatement();
            ResultSet rs = stmt.executeQuery(inputsQuery);

            while (rs.next())
            {
                int inputID = rs.getInt("input_id");
                int guiId = dbCache.getGuiId(machineId, inputID, "input");

                int inputState = rs.getInt("input_state");

                String inputIdForHtml = "switch-" + guiId;

                inputsJson.put(inputIdForHtml, inputState);
            }

            String conveyorQuery = String.format("SELECT conveyor_id, conveyor_state FROM conveyor_states WHERE machine_id=%d ORDER BY id ASC", machineId);
            rs = stmt.executeQuery(conveyorQuery);

            while (rs.next())
            {
                int conveyorId = rs.getInt("conveyor_id");
                int guiId = dbCache.getGuiId(machineId, conveyorId, "conveyor");

                int conveyorState = rs.getInt("conveyor_state");

                String conveyorIdForHtml = "conv-" + guiId;

                conveyorsJson.put(conveyorIdForHtml, conveyorState);
            }

            JSONObject alarmsJson = new JSONObject();
            String alarmsQuery = String.format("SELECT machine_id, alarm_id, alarm_type, date_active, UNIX_TIMESTAMP(date_active) AS date_active_ts FROM active_alarms WHERE machine_id=%d ORDER BY id DESC LIMIT 5", machineId);
            rs = stmt.executeQuery(alarmsQuery);

            while (rs.next())
            {
                String comboId = String.format("%d%d%d", rs.getInt("machine_id"), rs.getInt("alarm_id"), rs.getInt("alarm_type"));
                Map<String, String> singleAlarmData = dbCache.getAlarmData(comboId);

                String dateActive = rs.getTimestamp("date_active").toString();
                Date date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(dateActive);
                Date nowDate = new Date();
                long duration = getDuration(date, nowDate, TimeUnit.SECONDS);

                String dateActiveTs = rs.getString("date_active_ts");

                JSONObject singleAlarmJson = new JSONObject();

                if(singleAlarmData.size() > 0) {
                    singleAlarmData.forEach(singleAlarmJson::put);
                    singleAlarmJson.put("timestamp", dateActiveTs);
                    singleAlarmJson.put("duration", duration);
                    alarmsJson.put(comboId, singleAlarmJson);
                }
            }

            String relatedInductColumn = "i" + induct_id;
            //LIMIT = 24x12+1=289 for 24hr, HRx12+1
            String statisticsQuery = "SELECT id, "+ relatedInductColumn +", DATE_FORMAT(created_at, '%Y-%m-%d %H:%i') as formatted_date FROM statistics WHERE machine_id="+ machineId +" ORDER BY id DESC LIMIT 61";
            rs = stmt.executeQuery(statisticsQuery);

            Map<String, Map<String, Integer>> statsFromDb = new LinkedHashMap<>();
            int statCounter = 0; //for latest row tput
            while (rs.next())
            {
                int totalRead = rs.getInt(relatedInductColumn);
                String createdAt = rs.getString("formatted_date");

                //for latest row tput
                if(statCounter == 0) {
                    String referenceDateStr = createdAt + ":00";
                    Date referenceDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(referenceDateStr);

                    String currentDateStr = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(Calendar.getInstance().getTime());
                    Date currentDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(currentDateStr);

                    long secondsDiff = getDuration(referenceDate, currentDate, TimeUnit.SECONDS);

                    if(secondsDiff < 300) {
                        totalRead = (int) ceil(totalRead * (300.0/secondsDiff));
                    }
                }
                //for latest row tput

                Map<String, Integer> statsData = new LinkedHashMap<>();

                statsData.put("total_read", totalRead);

                if(!statsFromDb.containsKey(String.valueOf(createdAt))) {
                    statsFromDb.put(String.valueOf(createdAt), statsData);
                }

                statCounter++;
            }

            String currentDateStr = new java.text.SimpleDateFormat("yyyy-MM-dd ").format(Calendar.getInstance().getTime());

            String currentHourStr = new java.text.SimpleDateFormat("HH").format(Calendar.getInstance().getTime());

            String currentMinuteStr = new java.text.SimpleDateFormat("mm").format(Calendar.getInstance().getTime());
            int currentMinutetoInt = Integer.parseInt(currentMinuteStr);

            int newMinutes = 0;
            int minuteRemainder = currentMinutetoInt % 10;
            if(minuteRemainder > 4) {
                newMinutes = floorDiv(currentMinutetoInt, 10) * 10 + 5;
            } else {
                newMinutes = floorDiv(currentMinutetoInt, 10) * 10;
            }

            String newMinutesStr = String.format("%d", newMinutes);

            if(newMinutes < 10) {
                newMinutesStr = "0" + newMinutesStr;
            }

            if(Integer.parseInt(newMinutesStr) > 59) {
                if (newMinutesStr.equals("60")) {
                    newMinutesStr = "00";
                } else if (newMinutesStr.equals("65")) {
                    newMinutesStr = "05";
                }

                int currentHourInt = Integer.parseInt(currentHourStr) + 1;
                if(currentHourInt == 24) {
                    currentHourStr = "00";
                } else if(currentHourInt < 10) {
                    currentHourStr = "0" + currentHourInt;
                }
            }

            String newCurrentDateStr = currentDateStr + currentHourStr + ":" + newMinutesStr + ":00";
            SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            Date currentDate = format.parse(newCurrentDateStr);
            long currentTimestamp = currentDate.getTime();

            int gapInMinutes =  5 ;  // Define your span-of-time.
            int loops = ( (int) Duration.ofHours( 6 ).toMinutes() / gapInMinutes ) ;

            HashMap<String, String> modifiedDates = new LinkedHashMap<>();
            long modifiedTimestamp = currentTimestamp;

            for( int i = 1 ; i <= loops ; i ++ ) {
                if(i > 1) {
                    modifiedTimestamp = modifiedTimestamp - TimeUnit.MINUTES.toMillis(5);
                }

                Date modifiedTime = new Date(modifiedTimestamp);

                SimpleDateFormat checkDateFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm");
                String modifiedCheckDateString = checkDateFormatter.format(modifiedTime);

                modifiedDates.put(Long.toString(modifiedTimestamp), modifiedCheckDateString);
            }

            JSONObject statsJson = new JSONObject();
            int statsCounter = 0;

            List<String> reverseOrderedDates = new ArrayList<String>(modifiedDates.keySet());
            Collections.reverse(reverseOrderedDates);
            for (String k : reverseOrderedDates) {
                String timeSlot = modifiedDates.get(k);
                JSONObject singleStatsJson = new JSONObject();
                singleStatsJson.put("time_slot", timeSlot);
                singleStatsJson.put("time_stamp", k);
                if(statsFromDb.containsKey(timeSlot)) {
                    singleStatsJson.put("total_read", statsFromDb.get(timeSlot).get("total_read"));
                } else {
                    singleStatsJson.put("total_read", 0);
                }

                statsJson.put(String.valueOf(statsCounter), singleStatsJson);

                statsCounter++;
            }

            resultJson.put("alarms", alarmsJson);
            resultJson.put("inputs", inputsJson);
            resultJson.put("conveyors", conveyorsJson);
            resultJson.put("statistics", statsJson);

            int machineMode = getMachineMode(stmt, machineId);

            resultJson.put("mode", machineMode);

            mainJson.put("result", resultJson);

            rs.close();
            stmt.close();
            dbConn.close(); // connection close

        } catch (SQLException e) {
            logger.error(e.toString());
            //e.printStackTrace();
        }

        return mainJson;
    }
    public JSONObject getAlarmsHistory(int machineId, long startTimestamp, long endTimestamp) throws ParseException {
        int totalRowToDisplay = 2500;

        String startTimeTxt = null;
        String endTimeTxt = null;

        if(startTimestamp != 0 && endTimestamp != 0) {
            startTimeTxt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(TimeUnit.MILLISECONDS.convert(startTimestamp, TimeUnit.SECONDS)));
            endTimeTxt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(TimeUnit.MILLISECONDS.convert(endTimestamp, TimeUnit.SECONDS)));
        }

        JSONObject mainJson = new JSONObject();
        mainJson.put("type", "alarms_history");
        JSONObject resultJson = new JSONObject();
        JSONObject alarmDataJson = new JSONObject();
        JSONArray historyJson = new JSONArray();
        try {
            dbConn = DataSource.getConnection();
            String alarmsQuery = String.format("SELECT machine_id, alarm_id, alarm_type, date_active, date_inactive, UNIX_TIMESTAMP(date_active) AS date_active_ts FROM active_alarms_history WHERE machine_id=%d ORDER BY id DESC LIMIT %d", machineId, totalRowToDisplay);
            if(startTimestamp != 0 && endTimestamp != 0) {
                alarmsQuery = String.format("SELECT machine_id, alarm_id, alarm_type, date_active, date_inactive, UNIX_TIMESTAMP(date_active) AS date_active_ts FROM active_alarms_history WHERE machine_id=%d AND date_active>='%s' AND date_active<'%s' ORDER BY id DESC LIMIT %d",
                        machineId,
                        startTimeTxt,
                        endTimeTxt,
                        totalRowToDisplay);
            }

            Statement stmt = dbConn.createStatement();
            ResultSet rs = stmt.executeQuery(alarmsQuery);

            while (rs.next())
            {
                String comboId = String.format("%d%d%d", rs.getInt("machine_id"), rs.getInt("alarm_id"), rs.getInt("alarm_type"));
                Map<String, String> singleAlarmData = dbCache.getAlarmData(comboId);
                JSONObject singleAlarmDataJson = new JSONObject();

                if(!alarmDataJson.has(comboId)) {
                    singleAlarmData.forEach(singleAlarmDataJson::put);
                    alarmDataJson.put(comboId, singleAlarmData);
                }

                String dateActive = rs.getTimestamp("date_active").toString();
                String dateInactive = rs.getTimestamp("date_inactive").toString();

                Date date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(dateActive);
                Date nowDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(dateInactive);
                long duration = getDuration(date, nowDate, TimeUnit.SECONDS);

                String dateActiveTs = rs.getString("date_active_ts");

                JSONObject singleHistoryAlarmJson = new JSONObject();

                if(singleAlarmData.size() > 0) {

                    singleHistoryAlarmJson.put("mid", rs.getInt("machine_id"));
                    singleHistoryAlarmJson.put("aid", rs.getInt("alarm_id"));
                    singleHistoryAlarmJson.put("at", rs.getInt("alarm_type"));
                    singleHistoryAlarmJson.put("t", dateActiveTs);
                    singleHistoryAlarmJson.put("i", dateInactive);
                    singleHistoryAlarmJson.put("duration", duration);
                    historyJson.put(singleHistoryAlarmJson);
                }
            }

            int machineMode = getMachineMode(stmt, machineId);

            resultJson.put("mode", machineMode);


            resultJson.put("history", historyJson);
            resultJson.put("data", alarmDataJson);
            mainJson.put("result", resultJson);

            rs.close();
            stmt.close();
            dbConn.close(); // connection close

        } catch (SQLException e) {
            //e.printStackTrace();
            logger.error(e.toString());
        }

        return mainJson;
    }

    public JSONObject getAlarmsHitList(int machineId, long startTimestamp, long endTimestamp) throws ParseException {
        int totalRowToDisplay = 2500;

        String startTimeTxt = null;
        String endTimeTxt = null;

        if(startTimestamp != 0 && endTimestamp != 0) {
            startTimeTxt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(TimeUnit.MILLISECONDS.convert(startTimestamp, TimeUnit.SECONDS)));
            endTimeTxt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(TimeUnit.MILLISECONDS.convert(endTimestamp, TimeUnit.SECONDS)));
        }

        JSONObject mainJson = new JSONObject();
        mainJson.put("type", "alarms_hit_list");
        JSONObject resultJson = new JSONObject();
        JSONObject alarmDataJson = new JSONObject();
        JSONArray historyJson = new JSONArray();
        try {
            dbConn = DataSource.getConnection();
            String topListQuery = String.format("SELECT combo_id, COUNT(id) AS magnitude FROM active_alarms_history WHERE machine_id=%d GROUP BY combo_id ORDER BY magnitude DESC", machineId);

            if(startTimestamp != 0 && endTimestamp != 0) {
                topListQuery = String.format("SELECT combo_id, COUNT(id) AS magnitude FROM active_alarms_history WHERE machine_id=%d AND date_active>='%s' AND date_active<'%s' GROUP BY combo_id ORDER BY magnitude DESC",
                        machineId,
                        startTimeTxt,
                        endTimeTxt);
            }

            Statement stmt = dbConn.createStatement();
            ResultSet rs = stmt.executeQuery(topListQuery);

            List<Integer> topListArray = new ArrayList<>();
            Map<Integer, Integer> topListMap = new HashMap<>();
            int totalHitListCount = 0;
            while (rs.next()) {
                topListArray.add(rs.getInt("combo_id"));
                int magnitudeForThisAlarm = rs.getInt("magnitude");
                topListMap.put(rs.getInt("combo_id"), magnitudeForThisAlarm);
                totalHitListCount += magnitudeForThisAlarm;
            }

            float rowRatio = (float) totalRowToDisplay / totalHitListCount;
            //System.out.println("Total Hit: " + totalHitListCount + "Ratio: "  + rowRatio);

            if(topListArray.size() > 0) {
                for (Integer intComboId : topListArray) {
                //for (Map.Entry<Integer, Integer> topListEntry : topList.entrySet()) {
                    //int intComboId = topListEntry.getKey();
                    //int entryCount = topListEntry.getValue();
                    int entryCount = topListMap.get(intComboId);

                    int rowNum = (int) Math.ceil(entryCount * rowRatio);
                    //System.out.println(intComboId + " - " + rowNum);
                    String alarmsQuery = String.format("SELECT machine_id, alarm_id, alarm_type, date_active, date_inactive, UNIX_TIMESTAMP(date_active) AS date_active_ts FROM active_alarms_history WHERE machine_id=%d AND combo_id=%d ORDER BY id DESC LIMIT %d", machineId, intComboId, rowNum);
                    if(startTimestamp != 0 && endTimestamp != 0) {
                        alarmsQuery = String.format("SELECT machine_id, alarm_id, alarm_type, date_active, date_inactive, UNIX_TIMESTAMP(date_active) AS date_active_ts FROM active_alarms_history WHERE machine_id=%d AND combo_id=%d AND date_active>='%s' AND date_active<'%s' ORDER BY id DESC LIMIT %d",
                                machineId,
                                intComboId,
                                startTimeTxt,
                                endTimeTxt,
                                rowNum);
                    }

                    rs = stmt.executeQuery(alarmsQuery);
                    while (rs.next()) {
                        String comboId = Integer.toString(intComboId);
                        Map<String, String> singleAlarmData = dbCache.getAlarmData(comboId);
                        JSONObject singleAlarmDataJson = new JSONObject();

                        if (!alarmDataJson.has(comboId)) {
                            singleAlarmData.forEach(singleAlarmDataJson::put);
                            alarmDataJson.put(comboId, singleAlarmData);
                        }

                        String dateActive = rs.getTimestamp("date_active").toString();
                        String dateInactive = rs.getTimestamp("date_inactive").toString();

                        Date date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(dateActive);
                        Date nowDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(dateInactive);
                        long duration = getDuration(date, nowDate, TimeUnit.SECONDS);

                        JSONObject singleHistoryAlarmJson = new JSONObject();

                        String dateActiveTs = rs.getString("date_active_ts");

                        if (singleAlarmData.size() > 0) {

                            singleHistoryAlarmJson.put("mid", rs.getInt("machine_id"));
                            singleHistoryAlarmJson.put("aid", rs.getInt("alarm_id"));
                            singleHistoryAlarmJson.put("at", rs.getInt("alarm_type"));
                            singleHistoryAlarmJson.put("t", dateActiveTs);
                            singleHistoryAlarmJson.put("i", dateInactive);
                            singleHistoryAlarmJson.put("duration", duration);
                            historyJson.put(singleHistoryAlarmJson);
                        }
                    }
                }
            }

            int machineMode = getMachineMode(stmt, machineId);

            resultJson.put("mode", machineMode);


            resultJson.put("history", historyJson);
            resultJson.put("data", alarmDataJson);
            mainJson.put("result", resultJson);

            rs.close();
            stmt.close();
            dbConn.close(); // connection close

        } catch (SQLException e) {
            //e.printStackTrace();
            logger.error(e.toString());
        }

        return mainJson;
    }

    public JSONObject getErrorStatus(int machineId) throws ParseException {
        JSONObject mainJson = new JSONObject();
        mainJson.put("type", "status");
        JSONObject resultJson = new JSONObject();

        try {
            dbConn = DataSource.getConnection();
            Statement stmt = dbConn.createStatement();
            String deviceQuery = String.format("SELECT machine_id, device_id, device_state FROM device_states WHERE machine_id=%d ORDER BY id ASC", machineId);
            ResultSet rs = stmt.executeQuery(deviceQuery);

            JSONObject devicesJson = new JSONObject();
            while (rs.next())
            {
                int deviceId = rs.getInt("device_id");
                int guiId = dbCache.getGuiId(machineId, deviceId, "device");

                int deviceState = rs.getInt("device_state");

                String deviceIdForHtml = "device-" + guiId;
                if(guiId != 0) {
                    devicesJson.put(deviceIdForHtml, deviceState);
                }
            }

            //connection status of plc with java client
            //System.out.println("PLC CON of "+ machineId+" :" + ServerConstants.plcConnectStatus.get(machineId));
            devicesJson.put("device-mainPlc-Client", ServerConstants.plcConnectStatus.get(machineId));
            devicesJson.put("device-threeSixtyClient", ServerConstants.threeSixtyClientConnected);
            devicesJson.put("device-serverForIngram", ServerConstants.serverForIngramConnected);
            //devicesJson.put("device-ingram", 0);

            String eStopsQuery = String.format("SELECT input_id, input_state FROM input_states WHERE machine_id=%d AND input_id IN (SELECT input_id FROM inputs WHERE input_type=3 AND machine_id=%d)", machineId, machineId);
            rs = stmt.executeQuery(eStopsQuery);

            JSONObject eStopsJson = new JSONObject();
            while (rs.next())
            {
                int inputId = rs.getInt("input_id");
                int guiId = dbCache.getGuiId(machineId, inputId, "input");
                int activeState = dbCache.getInputActiveState(machineId, inputId);

                int inputState = rs.getInt("input_state");

                if(inputState == activeState) {
                    String eStopIdForHtml = "estop-" + guiId;
                    eStopsJson.put(Integer.toString(inputId), eStopIdForHtml);
                }
            }

            JSONObject alarmsJson = new JSONObject();
            String alarmsQuery = String.format("SELECT machine_id, alarm_id, alarm_type, date_active, UNIX_TIMESTAMP(date_active) AS date_active_ts FROM active_alarms WHERE machine_id=%d ORDER BY id DESC LIMIT 5", machineId);
            rs = stmt.executeQuery(alarmsQuery);

            while (rs.next())
            {
                String comboId = String.format("%d%d%d", rs.getInt("machine_id"), rs.getInt("alarm_id"), rs.getInt("alarm_type"));
                Map<String, String> singleAlarmData = dbCache.getAlarmData(comboId);

                String dateActive = rs.getTimestamp("date_active").toString();
                Date date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(dateActive);
                Date nowDate = new Date();
                long duration = getDuration(date, nowDate, TimeUnit.SECONDS);

                String dateActiveTs = rs.getString("date_active_ts");

                JSONObject singleAlarmJson = new JSONObject();

                if(singleAlarmData.size() > 0) {
                    singleAlarmData.forEach(singleAlarmJson::put);
                    singleAlarmJson.put("timestamp", dateActiveTs);
                    singleAlarmJson.put("duration", duration);
                    alarmsJson.put(comboId, singleAlarmJson);
                }
            }

            resultJson.put("alarms", alarmsJson);
            resultJson.put("devices", devicesJson);
            resultJson.put("estops", eStopsJson);

            int machineMode = getMachineMode(stmt, machineId);

            resultJson.put("mode", machineMode);

            mainJson.put("result", resultJson);

            rs.close();
            stmt.close();
            dbConn.close(); // connection close

        } catch (SQLException e) {
            //e.printStackTrace();
            logger.error(e.toString());
        }

        return mainJson;
    }

    public JSONObject getStatistics(int machineId) throws ParseException {
        JSONObject mainJson = new JSONObject();
        mainJson.put("type", "statistics");
        JSONObject resultJson = new JSONObject();

        try {
            dbConn = DataSource.getConnection();
            Statement stmt = dbConn.createStatement();

            int sc1_count = 0;
            int sc2_count = 0;
            int sc3_count = 0;
            int sc4_count = 0;
            int sc5_count = 0;


            String statisticsQuery = "SELECT id, total_read, no_read, multiple_read, valid,sc1,sc2,sc3,sc4,sc5, DATE_FORMAT(created_at, '%Y-%m-%d %H:%i') as formatted_date FROM statistics WHERE machine_id="+ machineId +" ORDER BY id DESC LIMIT 289";
            ResultSet rs = stmt.executeQuery(statisticsQuery);

            Map<String, Map<String, Integer>> statsFromDb = new LinkedHashMap<>();
            int statCounter = 0; //for latest row tput
            while (rs.next())
            {
                sc1_count+=rs.getInt("sc1");
                sc2_count+=rs.getInt("sc2");
                sc3_count+=rs.getInt("sc3");
                sc4_count+=rs.getInt("sc4");
                sc5_count+=rs.getInt("sc5");
                int totalRead = rs.getInt("total_read");
                int valid = rs.getInt("valid");
                int multipleRead = rs.getInt("multiple_read");
                int totalValid = valid + multipleRead;
                String createdAt = rs.getString("formatted_date");

                //for latest row tput
                if(statCounter == 0) {
                    String referenceDateStr = createdAt + ":00";
                    Date referenceDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(referenceDateStr);

                    String currentDateStr = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(Calendar.getInstance().getTime());
                    Date currentDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(currentDateStr);

                    long secondsDiff = getDuration(referenceDate, currentDate, TimeUnit.SECONDS);

                    if(secondsDiff < 300) {
                        totalRead = (int) ceil(totalRead * (300.0/secondsDiff));
                        totalValid = (int) ceil(totalValid * (300.0/secondsDiff));
                    }
                }
                //for latest row tput

                Map<String, Integer> statsData = new LinkedHashMap<>();

                statsData.put("total_read", totalRead);
                statsData.put("valid", totalValid);

                if(!statsFromDb.containsKey(String.valueOf(createdAt))) {
                    statsFromDb.put(String.valueOf(createdAt), statsData);
                }

                statCounter++;
            }
            JSONObject reasonsJson = new JSONObject();
            int total_products_count = sc1_count+sc2_count+sc3_count+sc4_count+sc5_count;
            reasonsJson.put(String.valueOf(1), sc1_count);
            reasonsJson.put(String.valueOf(2), sc2_count);
            reasonsJson.put(String.valueOf(3), sc3_count);
            reasonsJson.put(String.valueOf(4), sc4_count);
            reasonsJson.put(String.valueOf(5), sc5_count);
            reasonsJson.put("total", total_products_count);

            String currentDateStr = new java.text.SimpleDateFormat("yyyy-MM-dd ").format(Calendar.getInstance().getTime());

            String currentHourStr = new java.text.SimpleDateFormat("HH").format(Calendar.getInstance().getTime());

            String currentMinuteStr = new java.text.SimpleDateFormat("mm").format(Calendar.getInstance().getTime());
            int currentMinutetoInt = Integer.parseInt(currentMinuteStr);

            int newMinutes = 0;
            int minuteRemainder = currentMinutetoInt % 10;
            if(minuteRemainder > 4) {
                newMinutes = floorDiv(currentMinutetoInt, 10) * 10 + 5;
            } else {
                newMinutes = floorDiv(currentMinutetoInt, 10) * 10;
            }

            String newMinutesStr = String.format("%d", newMinutes);

            if(newMinutes < 10) {
                newMinutesStr = "0" + newMinutesStr;
            }

            if(Integer.parseInt(newMinutesStr) > 59) {
                if (newMinutesStr.equals("60")) {
                    newMinutesStr = "00";
                } else if (newMinutesStr.equals("65")) {
                    newMinutesStr = "05";
                }

                int currentHourInt = Integer.parseInt(currentHourStr) + 1;
                if(currentHourInt == 24) {
                    currentHourStr = "00";
                } else if(currentHourInt < 10) {
                    currentHourStr = "0" + currentHourInt;
                }
            }

            String newCurrentDateStr = currentDateStr + currentHourStr + ":" + newMinutesStr + ":00";
            SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            Date currentDate = format.parse(newCurrentDateStr);
            long currentTimestamp = currentDate.getTime();

            int gapInMinutes =  5 ;  // Define your span-of-time.
            int loops = ( (int) Duration.ofHours( 24 ).toMinutes() / gapInMinutes ) ;

            HashMap<String, String> modifiedDates = new LinkedHashMap<>();
            long modifiedTimestamp = currentTimestamp;

            for( int i = 1 ; i <= loops ; i ++ ) {
                if(i > 1) {
                    modifiedTimestamp = modifiedTimestamp - TimeUnit.MINUTES.toMillis(5);
                }

                Date modifiedTime = new Date(modifiedTimestamp);

                SimpleDateFormat checkDateFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm");
                String modifiedCheckDateString = checkDateFormatter.format(modifiedTime);

                modifiedDates.put(Long.toString(modifiedTimestamp), modifiedCheckDateString);
            }

            JSONObject statsJson = new JSONObject();
            int statsCounter = 0;

            List<String> reverseOrderedDates = new ArrayList<String>(modifiedDates.keySet());
            Collections.reverse(reverseOrderedDates);
            for (String k : reverseOrderedDates) {
                String timeSlot = modifiedDates.get(k);
                JSONObject singleStatsJson = new JSONObject();
                singleStatsJson.put("time_slot", timeSlot);
                singleStatsJson.put("time_stamp", k);
                if(statsFromDb.containsKey(timeSlot)) {
                    singleStatsJson.put("total_read", statsFromDb.get(timeSlot).get("total_read"));
                    singleStatsJson.put("valid", statsFromDb.get(timeSlot).get("valid"));
                } else {
                    singleStatsJson.put("total_read", 0);
                    singleStatsJson.put("valid", 0);
                }

                statsJson.put(String.valueOf(statsCounter), singleStatsJson);

                statsCounter++;
            }




            JSONObject alarmsJson = new JSONObject();
            String alarmsQuery = String.format("SELECT machine_id, alarm_id, alarm_type, date_active, UNIX_TIMESTAMP(date_active) AS date_active_ts FROM active_alarms WHERE machine_id=%d ORDER BY id DESC LIMIT 5", machineId);
            rs = stmt.executeQuery(alarmsQuery);

            while (rs.next())
            {
                String comboId = String.format("%d%d%d", rs.getInt("machine_id"), rs.getInt("alarm_id"), rs.getInt("alarm_type"));
                Map<String, String> singleAlarmData = dbCache.getAlarmData(comboId);

                String dateActive = rs.getTimestamp("date_active").toString();
                Date date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(dateActive);
                Date nowDate = new Date();
                long duration = getDuration(date, nowDate, TimeUnit.SECONDS);

                String dateActiveTs = rs.getString("date_active_ts");

                JSONObject singleAlarmJson = new JSONObject();

                if(singleAlarmData.size() > 0) {
                    singleAlarmData.forEach(singleAlarmJson::put);
                    singleAlarmJson.put("timestamp", dateActiveTs);
                    singleAlarmJson.put("duration", duration);
                    alarmsJson.put(comboId, singleAlarmJson);
                }
            }

            resultJson.put("alarms", alarmsJson);

            resultJson.put("statistics", statsJson);
            resultJson.put("reasons", reasonsJson);

            int machineMode = getMachineMode(stmt, machineId);

            resultJson.put("mode", machineMode);

            mainJson.put("result", resultJson);

            rs.close();
            stmt.close();
            dbConn.close(); // connection close

        } catch (SQLException e) {
            //e.printStackTrace();
            logger.error(e.toString());
        }

        return mainJson;
    }

    public JSONObject getSortedGraphs(int machineId) throws ParseException {
        JSONObject mainJson = new JSONObject();
        mainJson.put("type", "sorted_graphs");
        JSONObject resultJson = new JSONObject();

        try {
            dbConn = DataSource.getConnection();
            Statement stmt = dbConn.createStatement();
            //0,1,3,4,5,8,10,12,16,21
            String sortingCodeQuery = "SELECT id, sc1, sc2, sc3, sc4, sc5, DATE_FORMAT(created_at, '%Y-%m-%d %H:%i') as formatted_date FROM statistics WHERE machine_id="+ machineId +" ORDER BY id DESC LIMIT 289";
            ResultSet rs = stmt.executeQuery(sortingCodeQuery);

            /*let sorting_codes = {
            "1":"SC01",
            "2":"SC02",
            "3":"SC03",
            "4":"SC04",
            "5":"SC05",
            };*/

            Map<String, String> reasonCodes = new LinkedHashMap<>();
            reasonCodes.put("1", "sc1");
            reasonCodes.put("2", "sc3");
            reasonCodes.put("3", "sc3");
            reasonCodes.put("4", "sc4");
            reasonCodes.put("5", "sc5");

            Map<String, Map<String, Integer>> statsFromDb = new LinkedHashMap<>();
            int statCounter = 0; //for latest row tput
            while (rs.next())
            {
                String createdAt = rs.getString("formatted_date");

                Map<String, Integer> statsData = new LinkedHashMap<>();

                for (Map.Entry<String, String> entry : reasonCodes.entrySet()) {
                    String k = entry.getKey();
                    String v = entry.getValue();
                    statsData.put(k, rs.getInt(v));
                }


                if(!statsFromDb.containsKey(String.valueOf(createdAt))) {
                    statsFromDb.put(String.valueOf(createdAt), statsData);
                }

                statCounter++;
            }

            String currentDateStr = new java.text.SimpleDateFormat("yyyy-MM-dd ").format(Calendar.getInstance().getTime());

            String currentHourStr = new java.text.SimpleDateFormat("HH").format(Calendar.getInstance().getTime());

            String currentMinuteStr = new java.text.SimpleDateFormat("mm").format(Calendar.getInstance().getTime());
            int currentMinutetoInt = Integer.parseInt(currentMinuteStr);

            int newMinutes = 0;
            int minuteRemainder = currentMinutetoInt % 10;
            if(minuteRemainder > 4) {
                newMinutes = floorDiv(currentMinutetoInt, 10) * 10 + 5;
            } else {
                newMinutes = floorDiv(currentMinutetoInt, 10) * 10;
            }

            String newMinutesStr = String.format("%d", newMinutes);

            if(newMinutes < 10) {
                newMinutesStr = "0" + newMinutesStr;
            }

            if(Integer.parseInt(newMinutesStr) > 59) {
                if (newMinutesStr.equals("60")) {
                    newMinutesStr = "00";
                } else if (newMinutesStr.equals("65")) {
                    newMinutesStr = "05";
                }

                int currentHourInt = Integer.parseInt(currentHourStr) + 1;
                if(currentHourInt == 24) {
                    currentHourStr = "00";
                } else if(currentHourInt < 10) {
                    currentHourStr = "0" + currentHourInt;
                }
            }

            String newCurrentDateStr = currentDateStr + currentHourStr + ":" + newMinutesStr + ":00";
            SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            Date currentDate = format.parse(newCurrentDateStr);
            long currentTimestamp = currentDate.getTime();

            int gapInMinutes =  5 ;  // Define your span-of-time.
            int loops = ( (int) Duration.ofHours( 24 ).toMinutes() / gapInMinutes ) ;

            HashMap<String, String> modifiedDates = new LinkedHashMap<>();
            long modifiedTimestamp = currentTimestamp;

            for( int i = 1 ; i <= loops ; i ++ ) {
                if(i > 1) {
                    modifiedTimestamp = modifiedTimestamp - TimeUnit.MINUTES.toMillis(5);
                }

                Date modifiedTime = new Date(modifiedTimestamp);

                SimpleDateFormat checkDateFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm");
                String modifiedCheckDateString = checkDateFormatter.format(modifiedTime);

                modifiedDates.put(Long.toString(modifiedTimestamp), modifiedCheckDateString);
            }

            JSONObject statsJson = new JSONObject();
            int statsCounter = 0;

            List<String> reverseOrderedDates = new ArrayList<String>(modifiedDates.keySet());
            Collections.reverse(reverseOrderedDates);
            for (String k : reverseOrderedDates) {

                String timeSlot = modifiedDates.get(k);
                JSONObject singleStatsJson = new JSONObject();
                singleStatsJson.put("time_slot", timeSlot);
                singleStatsJson.put("time_stamp", k);
                if(statsFromDb.containsKey(timeSlot)) {
                    int totalReasonCount = 0;
                    for (Map.Entry<String, String> reasonEntry : reasonCodes.entrySet()) {
                        String kReason = reasonEntry.getKey();
                        totalReasonCount += statsFromDb.get(timeSlot).get(kReason);
                    }

                    for (Map.Entry<String, String> reasonEntry : reasonCodes.entrySet()) {
                        String kReason = reasonEntry.getKey();
                        double currentReasonCount = (double) statsFromDb.get(timeSlot).get(kReason);
                        double currentReasonPercentage = 0;
                        if(currentReasonCount != 0) {
                            currentReasonPercentage = (currentReasonCount / totalReasonCount) * 100;
                        }

                        singleStatsJson.put(kReason, currentReasonPercentage);
                    }
                } else {
                    for (Map.Entry<String, String> reasonEntry : reasonCodes.entrySet()) {
                        String kReason = reasonEntry.getKey();
                        singleStatsJson.put(kReason, 0);
                    }
                }

                statsJson.put(String.valueOf(statsCounter), singleStatsJson);

                statsCounter++;
            }

            resultJson.put("sc", statsJson);

            int machineMode = getMachineMode(stmt, machineId);

            resultJson.put("mode", machineMode);

            mainJson.put("result", resultJson);

            rs.close();
            stmt.close();
            dbConn.close(); // connection close

        } catch (SQLException e) {
            //e.printStackTrace();
            logger.error(e.toString());
        }

        return mainJson;
    }


    public JSONObject getSettings(int machineId) throws ParseException {
        JSONObject mainJson = new JSONObject();
        mainJson.put("type", "settings");

        try {
            dbConn = DataSource.getConnection();
            Statement stmt = dbConn.createStatement();
            JSONObject modeJson = new JSONObject();
            int machineMode = getMachineMode(stmt, machineId);

            modeJson.put("mode", machineMode);

            mainJson.put("result", modeJson);
            stmt.close();
            dbConn.close(); // connection close

        } catch (SQLException e) {
            //e.printStackTrace();
            logger.error(e.toString());
        }

        return mainJson;
    }

    public JSONObject getPackageList(int machineId) throws ParseException {
        JSONObject mainJson = new JSONObject();
        mainJson.put("type", "package_list");
        JSONObject resultJson = new JSONObject();

        try {
            dbConn = DataSource.getConnection();
            String packagesQuery = String.format("SELECT id, product_id, mail_id, length, barcode1_string, reject_code, reason, destination, final_destination, UNIX_TIMESTAMP(created_at) AS created_at FROM product_history WHERE machine_id=%d ORDER BY id DESC LIMIT 500", machineId);
            Statement stmt = dbConn.createStatement();
            ResultSet rs = stmt.executeQuery(packagesQuery);

            JSONObject packagesJson = new JSONObject();
            int pcCounter = 1;
            while (rs.next())
            {
                String createdAt = rs.getString("created_at");


                JSONObject singlePackageJson = new JSONObject();

                singlePackageJson.put("t", createdAt);
                singlePackageJson.put("p_id", rs.getInt("product_id"));
                singlePackageJson.put("m_id", rs.getInt("mail_id"));
                singlePackageJson.put("l", rs.getInt("length"));
                singlePackageJson.put("r_c", rs.getInt("reject_code"));
                singlePackageJson.put("s_c", rs.getInt("reason"));
                singlePackageJson.put("d", rs.getInt("destination"));
                singlePackageJson.put("f_d", rs.getInt("final_destination"));
                singlePackageJson.put("b_s", rs.getString("barcode1_string"));

                packagesJson.put(Integer.toString(pcCounter), singlePackageJson);
                pcCounter++;
            }

            resultJson.put("packages", packagesJson);
            int machineMode = getMachineMode(stmt, machineId);

            resultJson.put("mode", machineMode);

            rs.close();
            stmt.close();
            dbConn.close(); // connection close

        } catch (SQLException e) {
            //e.printStackTrace();
            logger.error(e.toString());
        }

        mainJson.put("result", resultJson);

        return mainJson;
    }
    public JSONObject getIngramProducts(String cartonId) throws ParseException {

        JSONObject responseJson = new JSONObject();
        responseJson.put("type", "statistics-package-to-sort");

        try {
            dbConn = DataSource.getConnection();
            String query = String.format("SELECT id, carton_id, dest1, dest2 FROM ingram_products where carton_id='%s' ORDER BY id DESC",cartonId);
            Statement stmt = dbConn.createStatement();
            ResultSet rs = stmt.executeQuery(query);
            JSONObject resultsJson = new JSONObject();
            int pcCounter = 1;
            while (rs.next())
            {
                JSONObject resutlJson = new JSONObject();
                resutlJson.put("id", rs.getInt("id"));
                resutlJson.put("carton_id", rs.getString("carton_id"));
                resutlJson.put("dest1", rs.getInt("dest1"));
                resutlJson.put("dest2", rs.getInt("dest2"));
                resultsJson.put(Integer.toString(pcCounter), resutlJson);
                pcCounter++;
            }
            responseJson.put("ingramProducts", resultsJson);
            rs.close();
            stmt.close();
            dbConn.close(); // connection close

        } catch (SQLException e) {
            //e.printStackTrace();
            logger.error(e.toString());
        }
        return responseJson;
    }
    public JSONObject getFilteredPackageList(int machineId, long startTimestamp, long endTimestamp, String sortingCode) throws ParseException {
        JSONObject mainJson = new JSONObject();
        mainJson.put("type", "package_list");
        String startTimeTxt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(TimeUnit.MILLISECONDS.convert(startTimestamp, TimeUnit.SECONDS)));
        String endTimeTxt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(TimeUnit.MILLISECONDS.convert(endTimestamp, TimeUnit.SECONDS)));

        String scQueryPart = "";
        if(!sortingCode.equals("na")) {
            scQueryPart = " AND `reason`=" + sortingCode;
        }

        JSONObject resultJson = new JSONObject();

        try {
            dbConn = DataSource.getConnection();
            String packagesQuery = String.format("SELECT id, product_id, mail_id, length, barcode1_string, reject_code, reason, destination, final_destination, UNIX_TIMESTAMP(created_at) AS created_at FROM product_history WHERE machine_id=%d AND created_at>='%s' AND created_at<'%s' %s  ORDER BY id DESC LIMIT 2500",
                    machineId,
                    startTimeTxt,
                    endTimeTxt,
                    scQueryPart);

            Statement stmt = dbConn.createStatement();
            ResultSet rs = stmt.executeQuery(packagesQuery);

            JSONObject packagesJson = new JSONObject();
            int pcCounter = 1;
            while (rs.next())
            {
                String createdAt = rs.getString("created_at");


                JSONObject singlePackageJson = new JSONObject();

                singlePackageJson.put("t", createdAt);
                singlePackageJson.put("p_id", rs.getInt("product_id"));
                singlePackageJson.put("m_id", rs.getInt("mail_id"));
                singlePackageJson.put("l", rs.getInt("length"));
                singlePackageJson.put("r_c", rs.getInt("reject_code"));
                singlePackageJson.put("s_c", rs.getInt("reason"));
                singlePackageJson.put("d", rs.getInt("destination"));
                singlePackageJson.put("f_d", rs.getInt("final_destination"));
                singlePackageJson.put("b_s", rs.getString("barcode1_string"));

                packagesJson.put(Integer.toString(pcCounter), singlePackageJson);
                pcCounter++;
            }

            resultJson.put("packages", packagesJson);

            int machineMode = getMachineMode(stmt, machineId);

            resultJson.put("mode", machineMode);

            rs.close();
            stmt.close();
            dbConn.close(); // connection close

        } catch (SQLException e) {
            //e.printStackTrace();
            logger.error(e.toString());
        }

        mainJson.put("result", resultJson);

        return mainJson;
    }



    public int getMachineMode(Statement stmt, int machineId) throws SQLException {
        int machineMode = 0;
        String machineModeQuery = String.format("SELECT machine_mode FROM machines WHERE machine_id=%d LIMIT 1", machineId);
        ResultSet rs = stmt.executeQuery(machineModeQuery);

        if(rs.next())
        {
            machineMode = rs.getInt("machine_mode");
        }

        return machineMode;
    }

    public long getDuration(Date date1, Date date2, TimeUnit timeUnit) {
        long diffInMillies = date2.getTime() - date1.getTime();
        return timeUnit.convert(diffInMillies,TimeUnit.MILLISECONDS);
    }
    //--------------------------------------
    public JSONArray getStatistics(int machineId,long from_timestamp,long to_timestamp){
        JSONArray resultsJsonArray = new JSONArray();
        try {
            Connection dbConn = DataSource.getConnection();
            Statement stmt = dbConn.createStatement();
            String query = String.format("SELECT *,UNIX_TIMESTAMP(created_at) AS created_at_timestamp FROM statistics WHERE machine_id=%d AND UNIX_TIMESTAMP(created_at) BETWEEN %d AND %d ORDER BY id DESC", machineId,from_timestamp,to_timestamp);
            ResultSet rs = stmt.executeQuery(query);
            while (rs.next())
            {
                JSONObject row=new JSONObject();
                row.put("id",rs.getInt("id"));
                row.put("total_read",rs.getInt("total_read"));
                row.put("no_read",rs.getInt("no_read"));
                row.put("no_code",rs.getInt("no_code"));
                row.put("multiple_read",rs.getInt("multiple_read"));
                row.put("valid",rs.getInt("valid"));
                row.put("recirc",rs.getInt("recirc"));
                row.put("reject",rs.getInt("reject"));
                row.put("total_good_length",rs.getInt("total_good_length"));
                row.put("sum_length",rs.getInt("sum_length"));
                row.put("total_good_gap",rs.getInt("total_good_gap"));
                row.put("sum_gap",rs.getInt("sum_gap"));
                row.put("sc0",rs.getInt("sc0"));
                row.put("sc1",rs.getInt("sc1"));
                row.put("sc3",rs.getInt("sc3"));
                row.put("sc4",rs.getInt("sc4"));
                row.put("sc5",rs.getInt("sc5"));
                row.put("sc6",rs.getInt("sc6"));
                row.put("sc7",rs.getInt("sc7"));
                row.put("sc8",rs.getInt("sc8"));
                row.put("sc9",rs.getInt("sc9"));
                row.put("sc10",rs.getInt("sc10"));
                row.put("sc12",rs.getInt("sc12"));
                row.put("sc14",rs.getInt("sc14"));
                row.put("sc16",rs.getInt("sc16"));
                row.put("sc17",rs.getInt("sc17"));
                row.put("sc18",rs.getInt("sc18"));
                row.put("sc21",rs.getInt("sc21"));
                row.put("i1",rs.getInt("i1"));
                row.put("i2",rs.getInt("i2"));
                row.put("i3",rs.getInt("i3"));
                row.put("i4",rs.getInt("i4"));
                row.put("i5",rs.getInt("i5"));
                row.put("i6",rs.getInt("i6"));
                row.put("i7",rs.getInt("i7"));
                row.put("created_at",rs.getString("created_at"));
                row.put("created_at_timestamp",rs.getLong("created_at_timestamp"));
                resultsJsonArray.put(row);

            }
            rs.close();
            stmt.close();
            dbConn.close();
        }
        catch (Exception e) {
            logger.error(e.toString());
        }
        return resultsJsonArray;
    }
    public JSONArray getStatisticsHourly(int machineId,long from_timestamp,long to_timestamp){
        JSONArray resultsJsonArray = new JSONArray();
        try {
            Connection dbConn = DataSource.getConnection();
            Statement stmt = dbConn.createStatement();
            String query = String.format("SELECT *,UNIX_TIMESTAMP(created_at) AS created_at_timestamp FROM statistics_hourly WHERE machine_id=%d AND UNIX_TIMESTAMP(created_at) BETWEEN %d AND %d ORDER BY id DESC", machineId,from_timestamp,to_timestamp);
            ResultSet rs = stmt.executeQuery(query);
            while (rs.next())
            {
                JSONObject row=new JSONObject();
                row.put("id",rs.getInt("id"));
                row.put("total_read",rs.getInt("total_read"));
                row.put("no_read",rs.getInt("no_read"));
                row.put("no_code",rs.getInt("no_code"));
                row.put("multiple_read",rs.getInt("multiple_read"));
                row.put("valid",rs.getInt("valid"));
                row.put("recirc",rs.getInt("recirc"));
                row.put("reject",rs.getInt("reject"));
                row.put("total_good_length",rs.getInt("total_good_length"));
                row.put("sum_length",rs.getInt("sum_length"));
                row.put("total_good_gap",rs.getInt("total_good_gap"));
                row.put("sum_gap",rs.getInt("sum_gap"));
                row.put("sc0",rs.getInt("sc0"));
                row.put("sc1",rs.getInt("sc1"));
                row.put("sc3",rs.getInt("sc3"));
                row.put("sc4",rs.getInt("sc4"));
                row.put("sc5",rs.getInt("sc5"));
                row.put("sc6",rs.getInt("sc6"));
                row.put("sc7",rs.getInt("sc7"));
                row.put("sc8",rs.getInt("sc8"));
                row.put("sc9",rs.getInt("sc9"));
                row.put("sc10",rs.getInt("sc10"));
                row.put("sc12",rs.getInt("sc12"));
                row.put("sc14",rs.getInt("sc14"));
                row.put("sc16",rs.getInt("sc16"));
                row.put("sc17",rs.getInt("sc17"));
                row.put("sc18",rs.getInt("sc18"));
                row.put("sc21",rs.getInt("sc21"));
                row.put("i1",rs.getInt("i1"));
                row.put("i2",rs.getInt("i2"));
                row.put("i3",rs.getInt("i3"));
                row.put("i4",rs.getInt("i4"));
                row.put("i5",rs.getInt("i5"));
                row.put("i6",rs.getInt("i6"));
                row.put("i7",rs.getInt("i7"));
                row.put("created_at",rs.getString("created_at"));
                row.put("created_at_timestamp",rs.getLong("created_at_timestamp"));
                resultsJsonArray.put(row);

            }
            rs.close();
            stmt.close();
            dbConn.close();
        }
        catch (Exception e) {
            logger.error(e.toString());
        }
        return resultsJsonArray;
    }
    public JSONArray getStatisticsCounter(int machineId,long from_timestamp,long to_timestamp){
        JSONArray resultsJsonArray = new JSONArray();
        try {
            Connection dbConn = DataSource.getConnection();
            Statement stmt = dbConn.createStatement();
            String query = String.format("SELECT *,UNIX_TIMESTAMP(created_at) AS created_at_timestamp FROM statistics_counter WHERE machine_id=%d AND UNIX_TIMESTAMP(created_at) BETWEEN %d AND %d ORDER BY id DESC", machineId,from_timestamp,to_timestamp);
            ResultSet rs = stmt.executeQuery(query);
            while (rs.next())
            {
                JSONObject row=new JSONObject();
                row.put("id",rs.getInt("id"));
                row.put("total_read",rs.getInt("total_read"));
                row.put("no_read",rs.getInt("no_read"));
                row.put("no_code",rs.getInt("no_code"));
                row.put("multiple_read",rs.getInt("multiple_read"));
                row.put("valid",rs.getInt("valid"));
                row.put("recirc",rs.getInt("recirc"));
                row.put("reject",rs.getInt("reject"));
                row.put("total_good_length",rs.getInt("total_good_length"));
                row.put("sum_length",rs.getInt("sum_length"));
                row.put("total_good_gap",rs.getInt("total_good_gap"));
                row.put("sum_gap",rs.getInt("sum_gap"));
                row.put("sc0",rs.getInt("sc0"));
                row.put("sc1",rs.getInt("sc1"));
                row.put("sc3",rs.getInt("sc3"));
                row.put("sc4",rs.getInt("sc4"));
                row.put("sc5",rs.getInt("sc5"));
                row.put("sc6",rs.getInt("sc6"));
                row.put("sc7",rs.getInt("sc7"));
                row.put("sc8",rs.getInt("sc8"));
                row.put("sc9",rs.getInt("sc9"));
                row.put("sc10",rs.getInt("sc10"));
                row.put("sc12",rs.getInt("sc12"));
                row.put("sc14",rs.getInt("sc14"));
                row.put("sc16",rs.getInt("sc16"));
                row.put("sc17",rs.getInt("sc17"));
                row.put("sc18",rs.getInt("sc18"));
                row.put("sc21",rs.getInt("sc21"));
                row.put("i1",rs.getInt("i1"));
                row.put("i2",rs.getInt("i2"));
                row.put("i3",rs.getInt("i3"));
                row.put("i4",rs.getInt("i4"));
                row.put("i5",rs.getInt("i5"));
                row.put("i6",rs.getInt("i6"));
                row.put("i7",rs.getInt("i7"));
                row.put("created_at",rs.getString("created_at"));
                row.put("created_at_timestamp",rs.getLong("created_at_timestamp"));
                resultsJsonArray.put(row);

            }
            rs.close();
            stmt.close();
            dbConn.close();
        }
        catch (Exception e) {
            logger.error(e.toString());
        }
        return resultsJsonArray;
    }
    public JSONArray getStatisticsCounterLast(int machineId){
        JSONArray resultsJsonArray = new JSONArray();
        try {
            Connection dbConn = DataSource.getConnection();
            Statement stmt = dbConn.createStatement();
            String query = String.format("SELECT *,UNIX_TIMESTAMP(created_at) AS created_at_timestamp FROM statistics_counter WHERE machine_id=%d ORDER BY id DESC LIMIT 1", machineId);
            ResultSet rs = stmt.executeQuery(query);
            while (rs.next())
            {
                JSONObject row=new JSONObject();
                row.put("id",rs.getInt("id"));
                row.put("total_read",rs.getInt("total_read"));
                row.put("no_read",rs.getInt("no_read"));
                row.put("no_code",rs.getInt("no_code"));
                row.put("multiple_read",rs.getInt("multiple_read"));
                row.put("valid",rs.getInt("valid"));
                row.put("recirc",rs.getInt("recirc"));
                row.put("reject",rs.getInt("reject"));
                row.put("total_good_length",rs.getInt("total_good_length"));
                row.put("sum_length",rs.getInt("sum_length"));
                row.put("total_good_gap",rs.getInt("total_good_gap"));
                row.put("sum_gap",rs.getInt("sum_gap"));
                row.put("sc0",rs.getInt("sc0"));
                row.put("sc1",rs.getInt("sc1"));
                row.put("sc3",rs.getInt("sc3"));
                row.put("sc4",rs.getInt("sc4"));
                row.put("sc5",rs.getInt("sc5"));
                row.put("sc6",rs.getInt("sc6"));
                row.put("sc7",rs.getInt("sc7"));
                row.put("sc8",rs.getInt("sc8"));
                row.put("sc9",rs.getInt("sc9"));
                row.put("sc10",rs.getInt("sc10"));
                row.put("sc12",rs.getInt("sc12"));
                row.put("sc14",rs.getInt("sc14"));
                row.put("sc16",rs.getInt("sc16"));
                row.put("sc17",rs.getInt("sc17"));
                row.put("sc18",rs.getInt("sc18"));
                row.put("sc21",rs.getInt("sc21"));
                row.put("i1",rs.getInt("i1"));
                row.put("i2",rs.getInt("i2"));
                row.put("i3",rs.getInt("i3"));
                row.put("i4",rs.getInt("i4"));
                row.put("i5",rs.getInt("i5"));
                row.put("i6",rs.getInt("i6"));
                row.put("i7",rs.getInt("i7"));
                row.put("created_at",rs.getString("created_at"));
                row.put("created_at_timestamp",rs.getLong("created_at_timestamp"));
                resultsJsonArray.put(row);

            }
            rs.close();
            stmt.close();
            dbConn.close();
        }
        catch (Exception e) {
            logger.error(e.toString());
        }
        return resultsJsonArray;
    }
    public JSONArray getStatisticsBins(int machineId,long from_timestamp,long to_timestamp){
        JSONArray resultsJsonArray = new JSONArray();
        try {
            Connection dbConn = DataSource.getConnection();
            Statement stmt = dbConn.createStatement();
            String query = String.format("SELECT *,UNIX_TIMESTAMP(created_at) AS created_at_timestamp FROM statistics_bins WHERE machine_id=%d AND UNIX_TIMESTAMP(created_at) BETWEEN %d AND %d ORDER BY id DESC", machineId,from_timestamp,to_timestamp);
            ResultSet rs = stmt.executeQuery(query);
            while (rs.next())
            {
                JSONObject row=new JSONObject();
                row.put("id",rs.getInt("id"));
                row.put("bin_id",rs.getInt("bin_id"));
                row.put("recirc",rs.getInt("recirc"));
                row.put("reject",rs.getInt("reject"));
                row.put("sc0",rs.getInt("sc0"));
                row.put("sc1",rs.getInt("sc1"));
                row.put("sc3",rs.getInt("sc3"));
                row.put("sc4",rs.getInt("sc4"));
                row.put("sc5",rs.getInt("sc5"));
                row.put("sc6",rs.getInt("sc6"));
                row.put("sc7",rs.getInt("sc7"));
                row.put("sc8",rs.getInt("sc8"));
                row.put("sc9",rs.getInt("sc9"));
                row.put("sc10",rs.getInt("sc10"));
                row.put("sc12",rs.getInt("sc12"));
                row.put("sc14",rs.getInt("sc14"));
                row.put("sc16",rs.getInt("sc16"));
                row.put("sc17",rs.getInt("sc17"));
                row.put("sc18",rs.getInt("sc18"));
                row.put("sc21",rs.getInt("sc21"));
                row.put("created_at",rs.getString("created_at"));
                row.put("created_at_timestamp",rs.getLong("created_at_timestamp"));
                resultsJsonArray.put(row);

            }
            rs.close();
            stmt.close();
            dbConn.close();
        }
        catch (Exception e) {
            logger.error(e.toString());
        }
        return resultsJsonArray;
    }
    public JSONArray getStatisticsBinsHourly(int machineId,long from_timestamp,long to_timestamp){
        JSONArray resultsJsonArray = new JSONArray();
        try {
            Connection dbConn = DataSource.getConnection();
            Statement stmt = dbConn.createStatement();
            String query = String.format("SELECT *,UNIX_TIMESTAMP(created_at) AS created_at_timestamp FROM statistics_bins_hourly WHERE machine_id=%d AND UNIX_TIMESTAMP(created_at) BETWEEN %d AND %d ORDER BY id DESC", machineId,from_timestamp,to_timestamp);
            ResultSet rs = stmt.executeQuery(query);
            while (rs.next())
            {
                JSONObject row=new JSONObject();
                row.put("id",rs.getInt("id"));
                row.put("bin_id",rs.getInt("bin_id"));
                row.put("recirc",rs.getInt("recirc"));
                row.put("reject",rs.getInt("reject"));
                row.put("sc0",rs.getInt("sc0"));
                row.put("sc1",rs.getInt("sc1"));
                row.put("sc3",rs.getInt("sc3"));
                row.put("sc4",rs.getInt("sc4"));
                row.put("sc5",rs.getInt("sc5"));
                row.put("sc6",rs.getInt("sc6"));
                row.put("sc7",rs.getInt("sc7"));
                row.put("sc8",rs.getInt("sc8"));
                row.put("sc9",rs.getInt("sc9"));
                row.put("sc10",rs.getInt("sc10"));
                row.put("sc12",rs.getInt("sc12"));
                row.put("sc14",rs.getInt("sc14"));
                row.put("sc16",rs.getInt("sc16"));
                row.put("sc17",rs.getInt("sc17"));
                row.put("sc18",rs.getInt("sc18"));
                row.put("sc21",rs.getInt("sc21"));
                row.put("created_at",rs.getString("created_at"));
                row.put("created_at_timestamp",rs.getLong("created_at_timestamp"));
                resultsJsonArray.put(row);

            }
            rs.close();
            stmt.close();
            dbConn.close();
        }
        catch (Exception e) {
            logger.error(e.toString());
        }
        return resultsJsonArray;
    }
    public JSONArray getStatisticsBinsCounter(int machineId,long from_timestamp,long to_timestamp){
        JSONArray resultsJsonArray = new JSONArray();
        try {
            Connection dbConn = DataSource.getConnection();
            Statement stmt = dbConn.createStatement();
            String query = String.format("SELECT *,UNIX_TIMESTAMP(created_at) AS created_at_timestamp FROM statistics_bins_counter WHERE machine_id=%d AND UNIX_TIMESTAMP(created_at) BETWEEN %d AND %d ORDER BY id DESC", machineId,from_timestamp,to_timestamp);
            ResultSet rs = stmt.executeQuery(query);
            while (rs.next())
            {
                JSONObject row=new JSONObject();
                row.put("id",rs.getInt("id"));
                row.put("bin_id",rs.getInt("bin_id"));
                row.put("recirc",rs.getInt("recirc"));
                row.put("reject",rs.getInt("reject"));
                row.put("sc0",rs.getInt("sc0"));
                row.put("sc1",rs.getInt("sc1"));
                row.put("sc3",rs.getInt("sc3"));
                row.put("sc4",rs.getInt("sc4"));
                row.put("sc5",rs.getInt("sc5"));
                row.put("sc6",rs.getInt("sc6"));
                row.put("sc7",rs.getInt("sc7"));
                row.put("sc8",rs.getInt("sc8"));
                row.put("sc9",rs.getInt("sc9"));
                row.put("sc10",rs.getInt("sc10"));
                row.put("sc12",rs.getInt("sc12"));
                row.put("sc14",rs.getInt("sc14"));
                row.put("sc16",rs.getInt("sc16"));
                row.put("sc17",rs.getInt("sc17"));
                row.put("sc18",rs.getInt("sc18"));
                row.put("sc21",rs.getInt("sc21"));
                row.put("created_at",rs.getString("created_at"));
                row.put("created_at_timestamp",rs.getLong("created_at_timestamp"));
                resultsJsonArray.put(row);

            }
            rs.close();
            stmt.close();
            dbConn.close();
        }
        catch (Exception e) {
            logger.error(e.toString());
        }
        return resultsJsonArray;
    }

    public JSONObject getBinsStates(int machineId){
        JSONObject binStates = new JSONObject();
        try {
            Connection dbConn = DataSource.getConnection();
            Statement stmt = dbConn.createStatement();
            String query = String.format("SELECT bin_id, event_type FROM bin_states WHERE machine_id=%d ORDER BY event_type ASC", machineId);
            ResultSet rs = stmt.executeQuery(query);
            while (rs.next())
            {
                binStates.put(rs.getString("bin_id"),rs.getInt("event_type"));
            }
            rs.close();
            stmt.close();
            dbConn.close();
        }
        catch (Exception e) {
            logger.error(e.toString());
        }
        return binStates;
    }
    public JSONObject getIoOutputsStates(int machineId){
        JSONObject resultJsonObject = new JSONObject();
        try {
            Connection dbConn = DataSource.getConnection();
            Statement stmt = dbConn.createStatement();
            String query = String.format("SELECT id,output_id,state FROM io_output_states WHERE machine_id=%d", machineId);
            ResultSet rs = stmt.executeQuery(query);
            while (rs.next())
            {
                JSONObject row=new JSONObject();
                row.put("id",rs.getInt("id"));
                row.put("output_id",rs.getInt("output_id"));
                row.put("state",rs.getInt("state"));
                row.put("machineId",machineId);
                resultJsonObject.put(machineId+"_"+rs.getString("output_id"),row);
            }
            rs.close();
            stmt.close();
            dbConn.close();
        }
        catch (Exception e) {
            logger.error(e.toString());
        }
        return resultJsonObject;
    }
    public JSONObject getIoInputsStates(int machineId){
        JSONObject resultJsonObject = new JSONObject();
        try {
            Connection dbConn = DataSource.getConnection();
            Statement stmt = dbConn.createStatement();
            String query = String.format("SELECT id,input_id,state FROM io_input_states WHERE machine_id=%d", machineId);
            ResultSet rs = stmt.executeQuery(query);
            while (rs.next())
            {
                JSONObject row=new JSONObject();
                row.put("id",rs.getInt("id"));
                row.put("input_id",rs.getInt("input_id"));
                row.put("state",rs.getInt("state"));
                row.put("machineId",machineId);
                resultJsonObject.put(machineId+"_"+rs.getString("input_id"),row);
            }
            rs.close();
            stmt.close();
            dbConn.close();
        }
        catch (Exception e) {
            logger.error(e.toString());
        }
        return resultJsonObject;
    }
    public JSONObject getParameterValues(int machineId){
        JSONObject resultJsonObject = new JSONObject();
        try {
            Connection dbConn = DataSource.getConnection();
            Statement stmt = dbConn.createStatement();
            String query = String.format("SELECT param_id,value FROM parameters WHERE machine_id=%d", machineId);
            ResultSet rs = stmt.executeQuery(query);
            while (rs.next())
            {
                JSONObject row=new JSONObject();
                row.put("param_id",rs.getInt("param_id"));
                row.put("value",rs.getInt("value"));
                row.put("machineId",machineId);
                resultJsonObject.put(machineId+"_"+rs.getString("param_id"),row);
            }
            rs.close();
            stmt.close();
            dbConn.close();
        }
        catch (Exception e) {
            logger.error(e.toString());
        }
        return resultJsonObject;
    }
    public JSONObject getInputsStates(int machineId){
        JSONObject resultJsonObject = new JSONObject();
        try {
            Connection dbConn = DataSource.getConnection();
            Statement stmt = dbConn.createStatement();
            String query = String.format("SELECT id,input_id, input_state FROM input_states WHERE machine_id=%d", machineId);
            ResultSet rs = stmt.executeQuery(query);
            while (rs.next())
            {
                JSONObject row=new JSONObject();
                row.put("id",rs.getInt("id"));
                row.put("input_id",rs.getInt("input_id"));
                row.put("input_state",rs.getInt("input_state"));
                row.put("machineId",machineId);
                resultJsonObject.put(machineId+"_"+rs.getString("input_id"),row);
            }
            rs.close();
            stmt.close();
            dbConn.close();
        }
        catch (Exception e) {
            logger.error(e.toString());
        }
        return resultJsonObject;
    }
    public JSONObject getConveyorsStates(int machineId){
        JSONObject conveyorsStates = new JSONObject();
        try {
            Connection dbConn = DataSource.getConnection();
            Statement stmt = dbConn.createStatement();
            String query = String.format("SELECT conveyor_id, conveyor_state FROM conveyor_states WHERE machine_id=%d ORDER BY conveyor_state ASC", machineId);
            ResultSet rs = stmt.executeQuery(query);
            while (rs.next())
            {
                conveyorsStates.put(rs.getString("conveyor_id"),rs.getInt("conveyor_state"));
            }
            rs.close();
            stmt.close();
            dbConn.close();
        }
        catch (Exception e) {
            logger.error(e.toString());
        }
        return conveyorsStates;
    }
    public JSONObject getDevicesStates(int machineId){
        JSONObject resultJsonObject = new JSONObject();
        try {
            Connection dbConn = DataSource.getConnection();
            Statement stmt = dbConn.createStatement();
            String query = String.format("SELECT id,device_id, device_state FROM device_states WHERE machine_id=%d", machineId);
            ResultSet rs = stmt.executeQuery(query);
            while (rs.next())
            {
                JSONObject row=new JSONObject();
                row.put("id",rs.getInt("id"));
                row.put("device_id",rs.getInt("device_id"));
                if(row.getInt("device_id")==2){
                    if(ServerConstants.plcConnectStatus.get(machineId) == 1){
                        row.put("device_state",1);
                    }
                    else{
                        row.put("device_state",0);
                    }
                }
                else{
                    row.put("device_state",rs.getInt("device_state"));
                }
                row.put("machineId",machineId);

                resultJsonObject.put(machineId+"_"+rs.getString("device_id"),row);
            }

            rs.close();
            stmt.close();
            dbConn.close();
        }
        catch (Exception e) {
            logger.error(e.toString());
        }
        return resultJsonObject;
    }
    public JSONArray getActiveAlarms(int machineId) {
        JSONArray resultsJsonArray = new JSONArray();
        try {
            Connection dbConn = DataSource.getConnection();
            Statement stmt = dbConn.createStatement();
            String query = String.format("SELECT *,UNIX_TIMESTAMP(date_active) AS date_active_timestamp FROM active_alarms WHERE machine_id=%d ORDER BY id DESC", machineId);
            ResultSet rs = stmt.executeQuery(query);
            while (rs.next())
            {
                JSONObject row=new JSONObject();
                row.put("id",rs.getString("id"));
                row.put("machine_id",rs.getString("machine_id"));
                row.put("alarm_id",rs.getString("alarm_id"));
                row.put("alarm_type",rs.getString("alarm_type"));
                row.put("date_active",rs.getString("date_active"));
                row.put("date_active_timestamp",rs.getLong("date_active_timestamp"));
                resultsJsonArray.put(row);
            }
            rs.close();
            stmt.close();
            dbConn.close();
        }
        catch (Exception e) {
            logger.error(e.toString());
        }
        return resultsJsonArray;
    }
    public int getMachineMode(int machineId){
        int machineMode = 0;
        try {
            Connection dbConn = DataSource.getConnection();
            Statement stmt = dbConn.createStatement();
            String query = String.format("SELECT machine_mode FROM machines WHERE machine_id=%d LIMIT 1", machineId);
            ResultSet rs = stmt.executeQuery(query);
            if(rs.next())
            {
                machineMode = rs.getInt("machine_mode");
            }
            rs.close();
            stmt.close();
            dbConn.close();
        }
        catch (Exception e) {
            logger.error(e.toString());
        }
        return machineMode;
    }
    public JSONArray getProductsHistory(int machineId,long from_timestamp,long to_timestamp){
        JSONArray resultsJsonArray = new JSONArray();
        try {
            Connection dbConn = DataSource.getConnection();
            Statement stmt = dbConn.createStatement();

            String query = String.format("SELECT *,UNIX_TIMESTAMP(created_at) AS created_at_timestamp FROM product_history WHERE machine_id=%d AND UNIX_TIMESTAMP(created_at) BETWEEN %d AND %d ORDER BY id DESC", machineId,from_timestamp,to_timestamp);
            ResultSet rs = stmt.executeQuery(query);
            while (rs.next())
            {
                JSONObject row=new JSONObject();
                row.put("id",rs.getInt("id"));
                row.put("product_id",rs.getInt("product_id"));
                row.put("machine_id",rs.getInt("machine_id"));
                row.put("mail_id",rs.getInt("mail_id"));
                row.put("length",rs.getInt("length"));
                row.put("width",rs.getInt("width"));
                row.put("height",rs.getInt("height"));
                row.put("weight",rs.getInt("weight"));
                row.put("reject_code",rs.getInt("reject_code"));
                row.put("number_of_results",rs.getInt("number_of_results"));
                row.put("barcode1_type",rs.getInt("barcode1_type"));
                row.put("barcode1_string",rs.getString("barcode1_string"));
                row.put("barcode2_type",rs.getInt("barcode2_type"));
                row.put("barcode2_string",rs.getString("barcode2_string"));
                row.put("barcode3_type",rs.getInt("barcode3_type"));
                row.put("barcode3_string",rs.getString("barcode3_string"));
                row.put("destination",rs.getInt("destination"));
                row.put("alternate_destination",rs.getInt("alternate_destination"));
                row.put("final_destination",rs.getInt("final_destination"));
                row.put("reason",rs.getInt("reason"));
                row.put("created_at",rs.getString("created_at"));
                row.put("created_at_timestamp",rs.getLong("created_at_timestamp"));
                resultsJsonArray.put(row);
            }
            rs.close();
            stmt.close();
            dbConn.close();
        }
        catch (Exception e) {
            logger.error(e.toString());
        }
        return resultsJsonArray;
    }
    public JSONObject getLoginUser(String username, String password){
        JSONObject resultJsonObject = new JSONObject();
        resultJsonObject.put("status",false);
        try {
            Connection dbConn = DataSource.getConnection();
            Statement stmt = dbConn.createStatement();
            String query = String.format("SELECT id,name, role FROM users WHERE username='%s' AND password='%s' LIMIT 1", username, password);
            ResultSet rs = stmt.executeQuery(query);
            if (rs.next())
            {
                resultJsonObject.put("status",true);
                JSONObject row=new JSONObject();
                row.put("id",rs.getInt("id"));
                row.put("name",rs.getString("name"));
                row.put("role",rs.getInt("role"));
                resultJsonObject.put("user",row);
            }
            rs.close();
            stmt.close();
            dbConn.close();
        }
        catch (Exception e) {
            logger.error(e.toString());
        }
        return resultJsonObject;
    }
    public JSONObject changeCurrentUserPassword(int id, String password,String password_new){
        JSONObject resultJsonObject = new JSONObject();
        resultJsonObject.put("status",false);
        try {
            Connection dbConn = DataSource.getConnection();
            Statement stmt = dbConn.createStatement();
            String query = String.format("SELECT id,password FROM users WHERE id='%d' LIMIT 1", id);
            ResultSet rs = stmt.executeQuery(query);
            if (rs.next())
            {
                if(rs.getString("password").equals(password)){
                    Statement stmt2 = dbConn.createStatement();
                    String updateQuery = format("UPDATE %s SET password='%s' WHERE id=%d;","users",password_new,id);
                    dbConn.setAutoCommit(false);
                    int num_row = stmt.executeUpdate(updateQuery);
                    dbConn.commit();
                    dbConn.setAutoCommit(true);
                    stmt2.close();
                    if(num_row>0){
                        resultJsonObject.put("status",true);
                        resultJsonObject.put("message","Successfully Changed");
                    }
                    else{
                        resultJsonObject.put("message","Failed to change password");
                    }
                }
                else{
                    resultJsonObject.put("messages","Old Password did not matched.");
                }
            }
            else{
                resultJsonObject.put("messages","User not found.");
            }
            rs.close();
            stmt.close();
            dbConn.close();
        }
        catch (Exception e) {
            logger.error(e.toString());
        }
        return resultJsonObject;
    }
    public int getDisconnectedDeviceCounter(int machineId){
        int totalDisconnected=0;
        try {
            Connection dbConn = DataSource.getConnection();
            Statement stmt = dbConn.createStatement();
            String query = String.format("SELECT COUNT(id) AS totalDisconnected FROM device_states WHERE machine_id=%d AND device_state=0 AND device_id NOT IN (SELECT device_id FROM devices WHERE machine_id=%d AND (gui_device_id=0 OR device_id=1 OR device_id=2)) LIMIT 1", machineId, machineId);
            ResultSet rs = stmt.executeQuery(query);

            if(rs.next())
            {
                totalDisconnected = rs.getInt("totalDisconnected");
            }
            rs.close();
            stmt.close();
            dbConn.close();
        }
        catch (Exception e) {
            logger.error(e.toString());
        }
        if(ServerConstants.plcConnectStatus.get(machineId) == 0) totalDisconnected++;
//        if(Integer.parseInt(ServerConstants.configuration.get("threesixty_enable"))==1){
//            if(ServerConstants.threeSixtyClientConnected == 0) totalDisconnected++;
//        }
//        if(Integer.parseInt(ServerConstants.configuration.get("ingram_enable"))==1){
//            if(ServerConstants.serverForIngramConnected == 0) totalDisconnected++;
//        }
        return totalDisconnected;
    }
}
