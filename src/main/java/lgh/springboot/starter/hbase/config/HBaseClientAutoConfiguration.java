package lgh.springboot.starter.hbase.config;

import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import lgh.springboot.starter.hbase.template.HBaseTemplate;

@Configuration
@EnableConfigurationProperties(HBaseClientConfiguration.class)
@ConditionalOnProperty(prefix = "hbase", value = "enabled", havingValue = "true", matchIfMissing = true)
public class HBaseClientAutoConfiguration {
    private HBaseClientConfiguration config;

    public HBaseClientAutoConfiguration(HBaseClientConfiguration config) {
        this.config = config;
    }

    @Bean
    @ConditionalOnMissingBean
    public HBaseTemplate hbaseTemplate() {
        return new HBaseTemplate(config);
    }
}
