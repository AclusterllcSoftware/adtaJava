package aclusterllc.adta;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class HydenConfigs {
    Map<String, String> result = new HashMap<>();
	InputStream inputStream;
	Logger logger = LoggerFactory.getLogger(HydenConfigs.class);

	public HydenConfigs() {
	}

	public Map<String, String> getPropValues(int buildType) throws IOException {

		try {

			Properties prop = new Properties();
			String propFileName = "config.properties";

			/*for jar build*/
			//if(buildType == 0) {
				//FileInputStream inputStream; //For jar build only
				String path = "./resources/" + propFileName;
				inputStream = new FileInputStream(path);
			/*} else {
				*//*for IDE*//*
				inputStream = getClass().getClassLoader().getResourceAsStream(propFileName);
			}*/

			if (inputStream != null) {
				prop.load(inputStream);
			} else {
				throw new FileNotFoundException("property file '" + propFileName + "' not found in the classpath");
			}



			// get the property value and print it out
			result.put("hmi_server_ip", prop.getProperty("hmi_server_ip"));
			result.put("hmi_server_port", prop.getProperty("hmi_server_port"));
			result.put("db.host", prop.getProperty("db.host"));
			result.put("db.name", prop.getProperty("db.name"));
			result.put("db.username", prop.getProperty("db.username"));
			result.put("db.password", prop.getProperty("db.password"));
			result.put("threesixty_enable", prop.getProperty("threesixty_enable"));
			result.put("threesixty_server_ip", prop.getProperty("threesixty_server_ip"));
			result.put("threesixty_server_port", prop.getProperty("threesixty_server_port"));
			result.put("threesixty_client_port", prop.getProperty("threesixty_client_port"));
			result.put("threesixty_client_ip", prop.getProperty("threesixty_client_ip"));
			result.put("threesixty_log_enabled", prop.getProperty("threesixty_log_enabled"));
			result.put("threesixty_weight_unit", prop.getProperty("threesixty_weight_unit"));
			result.put("threesixty_length_unit", prop.getProperty("threesixty_length_unit"));
			result.put("initial_sleep_time", prop.getProperty("initial_sleep_time"));

			result.put("ingram_enable", prop.getProperty("ingram_enable"));
			result.put("delete_after_confirm_destination_message", prop.getProperty("delete_after_confirm_destination_message"));
			result.put("ingram_server_ip", prop.getProperty("ingram_server_ip"));
			result.put("ingram_server_port", prop.getProperty("ingram_server_port"));
			result.put("purge_time", prop.getProperty("purge_time"));
			result.put("delete_ingram_product_older_than", prop.getProperty("delete_ingram_product_older_than"));
			result.put("delete_history_tables_older_than", prop.getProperty("delete_history_tables_older_than"));
			result.put("disconnect_ingram_clients_when_purge", prop.getProperty("disconnect_ingram_clients_when_purge"));

		} catch (Exception e) {
			//System.out.println("Exception: " + e);
			logger.error(e.toString());
		} finally {
            if (inputStream != null) {
                inputStream.close();
            }
        }

		return result;
	}
}
