package lgh.springboot.starter.hbase.entity;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

/**
 * 
 * @author Liuguanghua
 *
 */
public abstract class HBaseEntity implements Serializable {
    private static final long serialVersionUID = 522615455732848706L;

    private String rowKey;
    private Map<String, String> properties;

    public HBaseEntity() {
    }

    public HBaseEntity(String rowKey) {
        this.rowKey = rowKey;
    }

    public String getRowKey() {
        return rowKey;
    }

    public void setRowKey(String rowKey) {
        this.rowKey = rowKey;
    }

    public void addProperty(String key, String value) {
        getProperties().put(key, value);
    }

    public void addAllProperties(Map<String, String> props) {
        getProperties().putAll(props);
    }

    public String getProperty(String key) {
        return getProperties().get(key);
    }

    public Map<String, String> getProperties() {
        if (properties == null) {
            properties = new HashMap<>();
        }
        return properties;
    }

    @Override
    public boolean equals(Object obj) {
        return EqualsBuilder.reflectionEquals(this, obj);
    }

    @Override
    public int hashCode() {
        return HashCodeBuilder.reflectionHashCode(this);
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this, ToStringStyle.MULTI_LINE_STYLE);
    }

}
