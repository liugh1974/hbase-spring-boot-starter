package lgh.springboot.starter.hbase.template;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lgh.springboot.starter.hbase.config.HBaseClientConfiguration;
import lgh.springboot.starter.hbase.entity.HBaseEntity;
import lgh.springboot.starter.hbase.entity.HBaseEntityHelper;
import lgh.springboot.starter.hbase.entity.HBaseEntityHelper.HBaseFieldInfo;
import lgh.springboot.starter.hbase.entity.HBaseEntityHelper.HBaseTableInfo;
import lgh.springboot.starter.hbase.exception.HBaseOperationException;
import lgh.springboot.starter.hbase.utils.Convertor;
import lgh.springboot.starter.hbase.utils.HBaseScanUtils;

/**
 * 
 * @author Liuguanghua
 *
 */
public class HBaseTemplate {
    private static final Logger logger = LoggerFactory.getLogger(HBaseTemplate.class);

    private HBaseClientConfiguration config;
    private Connection connection;

    public HBaseTemplate(HBaseClientConfiguration config) {
        this.config = config;
        init();
    }

    public void init() {

        Configuration cfg = HBaseConfiguration.create();
        for (Entry<String, String> entry : config.getConfig().entrySet()) {
            cfg.set(entry.getKey(), entry.getValue());
        }

        try {
            connection = ConnectionFactory.createConnection(cfg);
            logger.info("HBase connection created");
        } catch (IOException ex) {
            logger.error(ex.getMessage(), ex);
            throw new HBaseOperationException(ex);
        }

        // check table and create them if not exist
        List<Class<? extends HBaseEntity>> entityClasses = HBaseScanUtils.scan();
        logger.info("Generate HBase Tables Size: {}", entityClasses.size());
        for (Class<? extends HBaseEntity> entityClass : entityClasses) {
            createTable(entityClass);
        }
    }

    public Connection getConnection() {
        return connection;
    }

    public void createNamespace(String namespace) {
        try (Admin admin = connection.getAdmin();) {
            NamespaceDescriptor[] namespaceDescs = admin.listNamespaceDescriptors();
            for (NamespaceDescriptor npDesc : namespaceDescs) {
                if (npDesc.getName().equals(namespace)) {
                    logger.info("Namespace: {} existed", namespace);
                    return;
                }
            }
            NamespaceDescriptor desc = NamespaceDescriptor.create(namespace).build();
            admin.createNamespace(desc);
            logger.info("Create namespace: {} successfully", namespace);
        } catch (Exception ex) {
            logger.error(ex.getMessage(), ex);
            throw new HBaseOperationException(ex);
        }
    }

    public void createTable(String tableName, List<String> columnFamily, Algorithm algorithm) {
        try (Admin admin = connection.getAdmin();) {
            if (admin.tableExists(TableName.valueOf(tableName))) {
                if (logger.isInfoEnabled()) {
                    logger.info("Table: {} existed", tableName);
                }
            } else {
                //@formatter:off
                List<ColumnFamilyDescriptor> colDescs = new ArrayList<>();
                for (String col : columnFamily) {
                    ColumnFamilyDescriptor desc = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(col))
                            .setCompressionType(algorithm)
                            .setMaxVersions(1)
                            .build();
                    colDescs.add(desc);
                }
                TableDescriptor tableDesc = TableDescriptorBuilder.newBuilder(TableName.valueOf(tableName))
                        .setColumnFamilies(colDescs)
                        .build();
                //@formatter:on
                admin.createTable(tableDesc);
                if (logger.isInfoEnabled()) {
                    logger.info("Create table: {} with column family: {} successfully", tableName, columnFamily);
                }
            }
        } catch (Exception ex) {
            logger.error(ex.getMessage(), ex);
            throw new HBaseOperationException(ex);
        }
    }

    public void createTable(Class<? extends HBaseEntity> type) {
        HBaseTableInfo info = HBaseEntityHelper.getTableInfo(type);

        if (!info.isDefaultNamespace()) {
            createNamespace(info.getNamespace());
        }
        if (logger.isInfoEnabled()) {
            logger.info("HBase Table Entity Info: {}", info);
        }
        createTable(info.getFullName(), Arrays.asList(info.getColumnFamily()), info.getCompression());

    }

    public void deleteTable(String tableName) {
        try (Admin admin = connection.getAdmin()) {
            TableName tname = TableName.valueOf(tableName);
            if (admin.tableExists(tname)) {
                admin.disableTable(tname);
                admin.deleteTable(tname);
                if (logger.isInfoEnabled()) {
                    logger.info("Deleted table: {} successfully", tableName);
                }
            }
        } catch (Exception ex) {
            logger.error(ex.getMessage(), ex);
            throw new HBaseOperationException(ex);
        }
    }

    public void save(String tableName, String rowKey, Map<String, Map<String, String>> data) {
        try (Table table = connection.getTable(TableName.valueOf(tableName))) {
            Put put = new Put(Bytes.toBytes(rowKey));

            for (Entry<String, Map<String, String>> entry : data.entrySet()) {
                for (Entry<String, String> sub : entry.getValue().entrySet()) {
                    put.addColumn(Bytes.toBytes(entry.getKey()), Bytes.toBytes(sub.getKey()),
                            Bytes.toBytes(sub.getValue()));
                }
            }
            table.put(put);
        } catch (Exception ex) {
            logger.error(ex.getMessage(), ex);
            throw new HBaseOperationException(ex);
        }
    }

    public <T extends HBaseEntity> void save(T entity) {
        HBaseTableInfo tableInfo = HBaseEntityHelper.getTableInfo(entity.getClass());
        Map<String, HBaseFieldInfo> fieldInfos = HBaseEntityHelper.getFieldInfo(entity.getClass());

        try (Table table = connection.getTable(TableName.valueOf(tableInfo.getFullName()))) {
            Put put = putOneRow(table, entity, tableInfo, fieldInfos);
            table.put(put);
        } catch (Exception ex) {
            logger.error(ex.getMessage(), ex);
            throw new HBaseOperationException(ex);
        }
    }

    public <T extends HBaseEntity> void save(List<T> entities) {
        if (entities.isEmpty()) {
            return;
        }
        HBaseTableInfo tableInfo = HBaseEntityHelper.getTableInfo(entities.get(0).getClass());
        Map<String, HBaseFieldInfo> fieldInfos = HBaseEntityHelper.getFieldInfo(entities.get(0).getClass());

        try (Table table = connection.getTable(TableName.valueOf(tableInfo.getFullName()))) {
            List<Put> puts = new ArrayList<>(entities.size());
            for (T t : entities) {
                puts.add(putOneRow(table, t, tableInfo, fieldInfos));
            }
            table.put(puts);
        } catch (Exception ex) {
            logger.error(ex.getMessage(), ex);
            throw new HBaseOperationException(ex);
        }
    }

    private <T extends HBaseEntity> Put putOneRow(Table table, T entity, HBaseTableInfo tableInfo,
            Map<String, HBaseFieldInfo> fieldInfos) throws IOException {
        Put put = new Put(Bytes.toBytes(entity.getRowKey()));
        for (Entry<String, HBaseFieldInfo> f : fieldInfos.entrySet()) {
            HBaseFieldInfo info = f.getValue();
            Object val;
            try {
                if (info.isSupperField()) {
                    val = info.getGetter().invoke(entity);
                } else {
                    val = info.getField().get(entity);
                }
            } catch (Exception ex) {
                throw new HBaseOperationException(ex);
            }

            if (info.isRequired() && val == null) {
                throw new HBaseOperationException("Field: " + info.getName() + "'s value is requred");
            }

            if (val != null) {
                byte[] value = Convertor.convertFieldValueToHBaseBytes(val, info.getType());
                put.addColumn(Bytes.toBytes(tableInfo.getColumnFamily()), Bytes.toBytes(info.getKey()), value);
            }
        }

        if (!entity.getProperties().isEmpty()) {
            for (Entry<String, String> entry : entity.getProperties().entrySet()) {
                String val = entry.getValue();
                if (val == null) {
                    continue;
                }
                byte[] value = Bytes.toBytes(val);
                put.addColumn(Bytes.toBytes(tableInfo.getColumnFamily()), Bytes.toBytes(entry.getKey()), value);
            }
        }

        // table.put(put);
        return put;
    }

    public <T extends HBaseEntity> void delete(T t) {
        HBaseTableInfo info = HBaseEntityHelper.getTableInfo(t.getClass());
        deleteByRowKey(info.getFullName(), t.getRowKey());
    }

    public <T extends HBaseEntity> void deleteByRowKey(String tableName, String rowKey) {
        try (Table table = connection.getTable(TableName.valueOf(tableName))) {
            Delete del = new Delete(Bytes.toBytes(rowKey));
            table.delete(del);
            if (logger.isDebugEnabled()) {
                logger.debug("Delete table: {} row data with row key: {}", tableName, rowKey);
            }
        } catch (Exception ex) {
            logger.error(ex.getMessage(), ex);
            throw new HBaseOperationException(ex);
        }

    }

    public Map<String, String> getByRowKey(String tableName, String rowKey) {
        Map<String, String> map = new HashMap<>();
        try (Table table = connection.getTable(TableName.valueOf(tableName))) {
            Get get = new Get(Bytes.toBytes(rowKey));
            Result result = table.get(get);
            for (Cell cell : result.rawCells()) {
                String key = Bytes.toString(CellUtil.cloneQualifier(cell));
                String value = Bytes.toString(CellUtil.cloneValue(cell));
                if (logger.isDebugEnabled()) {
                    logger.debug("Get Column Family: {}, Key: {}, Value: {}",
                            Bytes.toString(CellUtil.cloneFamily(cell)), key, value);
                }
                map.put(key, value);
            }
            if (logger.isDebugEnabled()) {
                logger.debug("Get table: {} rowKey: {} result: {}", tableName, rowKey, map);
            }
        } catch (Exception ex) {
            logger.error(ex.getMessage(), ex);
            throw new HBaseOperationException(ex);
        }
        return map;
    }

    public <T extends HBaseEntity> T getByRowKey(Class<T> type, String rowKey) {
        HBaseTableInfo tableInfo = HBaseEntityHelper.getTableInfo(type);
        Map<String, HBaseFieldInfo> fieldInfos = HBaseEntityHelper.getFieldInfo(type);

        try (Table table = connection.getTable(TableName.valueOf(tableInfo.getFullName()))) {
            Get get = new Get(Bytes.toBytes(rowKey));
            Result result = table.get(get);
            if (result == null || result.isEmpty()) {
                return null;
            }

            T t = type.newInstance();
            for (Cell cell : result.rawCells()) {
                setFieldValue(cell, t, fieldInfos);
            }
            t.setRowKey(rowKey);
            if (logger.isTraceEnabled()) {
                logger.trace("Get table: {} rowKey: {} result: {}", tableInfo.getFullName(), rowKey, t);
            }
            return t;
        } catch (Exception ex) {
            logger.error(ex.getMessage(), ex);
            throw new HBaseOperationException(ex);
        }
    }

    public <T extends HBaseEntity> List<T> getByRowKeys(Class<T> type, List<String> rowKeys) {
        HBaseTableInfo tableInfo = HBaseEntityHelper.getTableInfo(type);
        Map<String, HBaseFieldInfo> fieldInfos = HBaseEntityHelper.getFieldInfo(type);

        try (Table table = connection.getTable(TableName.valueOf(tableInfo.getFullName()))) {
            List<Get> gets = new ArrayList<>();
            for (String rowKey : rowKeys) {
                Get get = new Get(Bytes.toBytes(rowKey));
                gets.add(get);
            }
            Result[] results = table.get(gets);
            if (results == null || results.length == 0) {
                return Collections.emptyList();
            }

            List<T> list = new ArrayList<>(results.length);
            for (Result result : results) {
                T t = type.newInstance();
                for (Cell cell : result.rawCells()) {
                    setFieldValue(cell, t, fieldInfos);
                }
                t.setRowKey(Bytes.toString(result.getRow()));
                if (logger.isTraceEnabled()) {
                    logger.trace("Get table: {} rowKey: {} result: {}", tableInfo.getFullName(), t.getRowKey(), t);
                }
                list.add(t);
            }
            return list;
        } catch (Exception ex) {
            logger.error(ex.getMessage(), ex);
            throw new HBaseOperationException(ex);
        }
    }
    
    /**
     * 
     * @param <T> MUST has rowKey value
     * @param t
     * @return
     */
    @SuppressWarnings("unchecked")
    public <T extends HBaseEntity> T get(T t) {
        return getByRowKey((Class<T>) t.getClass(), t.getRowKey());
    }

    public <T extends HBaseEntity> List<T> query(Class<T> type, String startRowKey, String endRowKey) {
        return query(type, startRowKey, endRowKey, null);
    }

    /**
     * 
     * @param <T>
     * @param type
     * @param startRowKey
     * @param endRowKey
     * @param filterFields
     * @return
     */
    public <T extends HBaseEntity> List<T> query(Class<T> type, String startRowKey, String endRowKey,
            Map<String, Object> filterFields) {
        HBaseTableInfo tableInfo = HBaseEntityHelper.getTableInfo(type);
        Map<String, HBaseFieldInfo> fieldInfos = HBaseEntityHelper.getFieldInfo(type);

        try (Table table = connection.getTable(TableName.valueOf(tableInfo.getFullName()))) {
            Scan scan = new Scan().withStartRow(Bytes.toBytes(startRowKey)).withStopRow(Bytes.toBytes(endRowKey));

            if (filterFields != null && !filterFields.isEmpty()) {
                List<Filter> filters = new ArrayList<>(filterFields.size());
                for (Entry<String, Object> entry : filterFields.entrySet()) {
                    Object val = entry.getValue();
                    if (val == null) {
                        continue;
                    }

                    String fieldName = entry.getKey();
                    Class<?> fieldType = String.class;
                    String key = fieldName;

                    HBaseFieldInfo fieldInfo = getFieldInfo(fieldInfos, fieldName);
                    byte[] value;
                    if (fieldInfo != null) {
                        fieldType = fieldInfo.getType();
                        key = fieldInfo.getKey();
                        if (fieldInfo.getType().equals(val.getClass())) {
                            value = Convertor.convertFieldValueToHBaseBytes(val, fieldType);
                        } else {
                            // if filter value type is not field type, convert it to field type(only support
                            // String)
                            val = Convertor.convetStringToFieldType((String) val, fieldType);
                            value = Convertor.convertFieldValueToHBaseBytes(val, fieldType);
                        }
                    } else {
                        value = Bytes.toBytes(val.toString());
                    }

                    Filter filter = new SingleColumnValueFilter(Bytes.toBytes(tableInfo.getColumnFamily()),
                            Bytes.toBytes(key), CompareOperator.EQUAL, value);
                    filters.add(filter);

                }
                scan.setFilter(new FilterList(filters));
            }

            ResultScanner results = table.getScanner(scan);
            List<T> list = new ArrayList<>();
            for (Result result : results) {
                T t = type.newInstance();
                for (Cell cell : result.rawCells()) {
                    setFieldValue(cell, t, fieldInfos);
                }
                t.setRowKey(Bytes.toString(result.getRow()));
                list.add(t);
            }
            return list;
        } catch (Exception ex) {
            logger.error(ex.getMessage(), ex);
            throw new HBaseOperationException(ex);
        }
    }

    private HBaseFieldInfo getFieldInfo(Map<String, HBaseFieldInfo> fieldInfos, String fieldName) {
        for (Entry<String, HBaseFieldInfo> entry : fieldInfos.entrySet()) {
            if (entry.getValue().getName().equals(fieldName)) {
                return entry.getValue();
            }
        }
        return null;
    }

    private <T extends HBaseEntity> void setFieldValue(Cell cell, T t, Map<String, HBaseFieldInfo> fieldInfos) {
        String key = Bytes.toString(CellUtil.cloneQualifier(cell));
        byte[] val = CellUtil.cloneValue(cell);
        HBaseFieldInfo info = fieldInfos.get(key);
        if (info != null) {
            try {
                Object value = Convertor.convertHBaseBytesToFieldValue(val, info.getType());
                if (info.isSupperField()) {
                    info.getSetter().invoke(t, value);
                } else {
                    info.getField().set(t, value);
                }
                if (logger.isTraceEnabled()) {
                    logger.trace("Set field: {} type: {}, value: {}", info.getName(), info.getType().getSimpleName(),
                            value);
                }
            } catch (Exception ignoreException) {
                if (logger.isTraceEnabled()) {
                    logger.trace(ignoreException.getMessage(), ignoreException);
                }
            }
        } else {
            t.addProperty(key, Bytes.toString(val));
        }
    }

}
