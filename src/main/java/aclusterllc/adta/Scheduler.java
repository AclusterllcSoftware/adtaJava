package aclusterllc.adta;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalTime;

import static java.lang.String.format;
import static java.lang.Thread.sleep;

public class Scheduler {
    private DatabaseHandler databaseHandler;
    ServerForIngram serverForIngram;
    Logger logger = LoggerFactory.getLogger(Scheduler.class);
    public Scheduler( DatabaseHandler databaseHandler,ServerForIngram serverForIngram) {
        this.databaseHandler = databaseHandler;
        this.serverForIngram = serverForIngram;
    }
    public void startPurgeSchedule(){
        String purge_time=ServerConstants.configuration.get("purge_time");
        int delete_ingram_product_older_than=Integer.parseInt(ServerConstants.configuration.get("delete_ingram_product_older_than"));
        int delete_history_tables_older_than=Integer.parseInt(ServerConstants.configuration.get("delete_history_tables_older_than"));
        //System.out.println(purge_time+" "+delete_history_tables_older_than+" "+delete_ingram_product_older_than);
        new Thread(()->{
            while (true){
                LocalTime currentTime= LocalTime.now();
                LocalTime scheduleTime= LocalTime.parse(purge_time);
                int difference=(scheduleTime.toSecondOfDay()- currentTime.toSecondOfDay())*1000;
                if(difference<=0){
                    difference=86400000+difference;
                }
                try {
                    logger.info("Purge Scheduler waiting "+difference+" ms for next schedule");
                    Thread.sleep(difference);
                    logger.info("Purge Scheduler waiting End");
                    if(Integer.parseInt(ServerConstants.configuration.get("ingram_enable"))==1){
                        if(Integer.parseInt(ServerConstants.configuration.get("disconnect_ingram_clients_when_purge"))==1) {
                            logger.info("Disconnecting ALl Ingram Clients");
                            serverForIngram.disConnectAllIngramClient();
                        }
                        else{
                            logger.info("Disconnecting ALl Ingram Clients ignored because it is off");
                        }

                        String query=format("DELETE FROM ingram_products WHERE (updated_at < NOW() - INTERVAL %d DAY) OR ((created_at  < NOW() - INTERVAL %d DAY) AND updated_at IS NULL);",delete_ingram_product_older_than,delete_ingram_product_older_than);
                        databaseHandler.append(query);
                        logger.info("Deleting Ingram Product data older than "+delete_ingram_product_older_than);


                    }
                    else{
                        logger.info("Ingram Client disconnect skipped because ingram is not enabled");
                        logger.info("Ingram product delete skipped because ingram is not enabled");
                    }

                    String query=format("DELETE FROM active_alarms_history WHERE (date_active < NOW() - INTERVAL %d DAY);",delete_history_tables_older_than);
                    databaseHandler.append(query);
                    logger.info("Deleting active_alarms_history data older than "+delete_history_tables_older_than);

                    query=format("DELETE FROM bin_states_history WHERE (created_at < NOW() - INTERVAL %d DAY);",delete_history_tables_older_than);
                    databaseHandler.append(query);
                    logger.info("Deleting bin_states_history data older than "+delete_history_tables_older_than);

                    query=format("DELETE FROM conveyor_states_history WHERE (created_at < NOW() - INTERVAL %d DAY);",delete_history_tables_older_than);
                    databaseHandler.append(query);
                    logger.info("Deleting conveyor_states_history data older than "+delete_history_tables_older_than);

                    query=format("DELETE FROM device_states_history WHERE (created_at < NOW() - INTERVAL %d DAY);",delete_history_tables_older_than);
                    databaseHandler.append(query);
                    logger.info("Deleting device_states_history data older than "+delete_history_tables_older_than);

                    query=format("DELETE FROM induct_states_history WHERE (created_at < NOW() - INTERVAL %d DAY);",delete_history_tables_older_than);
                    databaseHandler.append(query);
                    logger.info("Deleting induct_states_history data older than "+delete_history_tables_older_than);

                    query=format("DELETE FROM input_states_history WHERE (created_at < NOW() - INTERVAL %d DAY);",delete_history_tables_older_than);
                    databaseHandler.append(query);
                    logger.info("Deleting input_states_history data older than "+delete_history_tables_older_than);

                    query=format("DELETE FROM product_history WHERE (created_at < NOW() - INTERVAL %d DAY);",delete_history_tables_older_than);
                    databaseHandler.append(query);
                    logger.info("Deleting product_history data older than "+delete_history_tables_older_than);


                    query=format("DELETE FROM statistics WHERE (created_at < NOW() - INTERVAL %d DAY);" +
                            "DELETE FROM statistics_bins WHERE (created_at < NOW() - INTERVAL %d DAY);" +
                            "DELETE FROM statistics_bins_counter WHERE (created_at < NOW() - INTERVAL %d DAY);" +
                            "DELETE FROM statistics_bins_hourly WHERE (created_at < NOW() - INTERVAL %d DAY);" +
                            "DELETE FROM statistics_counter WHERE (created_at < NOW() - INTERVAL %d DAY);" +
                            "DELETE FROM statistics_hourly WHERE (created_at < NOW() - INTERVAL %d DAY);" +
                            "DELETE FROM statistics_minutely WHERE (created_at < NOW() - INTERVAL %d DAY);"
                            ,delete_history_tables_older_than,delete_history_tables_older_than,delete_history_tables_older_than,delete_history_tables_older_than
                            ,delete_history_tables_older_than,delete_history_tables_older_than,delete_history_tables_older_than);
                    databaseHandler.append(query);
                    logger.info("Deleting All statistics data older than "+delete_history_tables_older_than);


                }
                catch (InterruptedException e) {
                    logger.error("Purge Scheduler failed to Sleep "+difference+" ms");
                    logger.error(e.toString());
                }
            }
        }).start();

    }
}
