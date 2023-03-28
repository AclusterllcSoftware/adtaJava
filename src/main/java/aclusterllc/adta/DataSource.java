package aclusterllc.adta;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;

public class DataSource {

    private static HikariConfig config = new HikariConfig();
    private static HikariDataSource ds;
    private static final String dbHost = ServerConstants.configuration.get("db.host");
    private static final String dbName = ServerConstants.configuration.get("db.name");
    private static final String dbUsername = ServerConstants.configuration.get("db.username");
    private static final String dbPassword = ServerConstants.configuration.get("db.password");

    private static final String jdbcUrl = String.format("jdbc:mysql://%s:3306/%s?allowPublicKeyRetrieval=true&useSSL=false&useUnicode=true&characterEncoding=utf8&allowMultiQueries=true",
            dbHost,
            dbName);

    static {
        config.setJdbcUrl(jdbcUrl);
        config.setUsername( dbUsername );
        config.setPassword( dbPassword );
        config.setMaximumPoolSize(30);
        config.setConnectionTimeout(300000);
        config.setLeakDetectionThreshold(300000);
        config.addDataSourceProperty( "cachePrepStmts" , "true" );
        config.addDataSourceProperty( "prepStmtCacheSize" , "250" );
        config.addDataSourceProperty( "prepStmtCacheSqlLimit" , "2048" );
        Logger logger = LoggerFactory.getLogger(DataSource.class);
        try{
            ds = new HikariDataSource( config );
            logger.info("[Database] Connected.");
        }
        catch (Exception ex){

            logger.error("[Database] Connection Failed.Closing Java Program");
            System.exit(0);
        }
    }

    private DataSource() {}

    public static Connection getConnection() throws SQLException {
        return ds.getConnection();
    }
}
