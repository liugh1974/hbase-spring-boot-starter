package lgh.springboot.starter.hbase.config;

import java.util.Map;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * 
 * @author Liuguanghua
 *
 */
@ConfigurationProperties(prefix = "hbase")
public class HBaseClientConfiguration {
    private Map<String, String> config;

    public Map<String, String> getConfig() {
        return config;
    }

    public void setConfig(Map<String, String> config) {
        this.config = config;
    }

}
