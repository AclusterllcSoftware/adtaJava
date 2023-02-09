package aclusterllc.adta;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import java.sql.Connection;
import java.sql.SQLException;

public class DataSource {

    private static HikariConfig config = new HikariConfig();
    private static HikariDataSource ds;
    private static final String dbHost = ServerConstants.configuration.get("db.host");
    private static final String dbName = ServerConstants.configuration.get("db.name");
    private static final String dbUsername = ServerConstants.configuration.get("db.username");
    private static final String dbPassword = ServerConstants.configuration.get("db.password");

    private static final String jdbcUrl = String.format("jdbc:mysql://%s:3306/%s?useSSL=false&useUnicode=true&characterEncoding=utf8&allowMultiQueries=true",
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
        ds = new HikariDataSource( config );
    }

    private DataSource() {}

    public static Connection getConnection() throws SQLException {
        return ds.getConnection();
    }
}
