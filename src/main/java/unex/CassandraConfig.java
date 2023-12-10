package unex;

import java.io.InputStream;
import java.util.Properties;

public class CassandraConfig {
    private static final String CONFIG_FILE = "cassandra-config.properties";

    private Properties properties;

    public CassandraConfig() {
        this.properties = loadProperties();
    }

    private Properties loadProperties() {
        Properties props = new Properties();
        try (InputStream input = getClass().getClassLoader().getResourceAsStream(CONFIG_FILE)) {
            if (input != null) {
                props.load(input);
            } else {
                throw new RuntimeException("Unable to find " + CONFIG_FILE);
            }
        } catch (Exception e) {
            throw new RuntimeException("Error loading configuration from " + CONFIG_FILE, e);
        }
        return props;
    }

    public String getHost() {
        return properties.getProperty("cassandra.host");
    }

    public int getPort() {
        return Integer.parseInt(properties.getProperty("cassandra.port"));
    }

    public String getUsername() {
        return properties.getProperty("cassandra.username");
    }

    public String getPassword() {
        return properties.getProperty("cassandra.password");
    }

    public String getDataCenter() {
        return properties.getProperty("cassandra.datacenter");
    }

    public String getKeyspace() {
        return properties.getProperty("cassandra.keyspace");
    }


}
