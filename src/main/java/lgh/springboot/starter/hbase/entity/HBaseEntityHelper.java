package lgh.springboot.starter.hbase.entity;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lgh.springboot.starter.hbase.annotation.HBaseField;
import lgh.springboot.starter.hbase.annotation.HBaseTable;

/**
 * 
 * @author Liuguanghua
 *
 */
public final class HBaseEntityHelper {
    private static final Logger logger = LoggerFactory.getLogger(HBaseEntityHelper.class);

    public static final String DEFAULT_NAMESPACE = "default";
    public static final String DEFAULT_COLUMN_FAMILY = "c1";

    private static Map<Class<? extends HBaseEntity>, Map<String, HBaseFieldInfo>> CACHE_TABLE_FIELDS = new HashMap<>();
    private static Map<Class<? extends HBaseEntity>, HBaseTableInfo> CACHE_TABLE_INFOS = new HashMap<>();

    public static HBaseTableInfo getTableInfo(Class<? extends HBaseEntity> type) {
        HBaseTableInfo info = CACHE_TABLE_INFOS.get(type);
        if (info == null) {
            synchronized (HBaseEntityHelper.class) {
                info = CACHE_TABLE_INFOS.get(type);
                if (info == null) {
                    HBaseTable tableAnn = type.getAnnotation(HBaseTable.class);
                    String tableName = type.getSimpleName();
                    String columnFamily = DEFAULT_COLUMN_FAMILY;
                    String namespace = DEFAULT_NAMESPACE;
                    Algorithm algorithm = Algorithm.NONE;
                    if (tableAnn == null) {
                        tableName = type.getSimpleName();
                    } else {
                        if (!isBlank(tableAnn.name())) {
                            tableName = tableAnn.name().trim();
                        }
                        if (!isBlank(tableAnn.columnFamily())) {
                            columnFamily = tableAnn.columnFamily().trim();
                        }
                        namespace = tableAnn.namespace().trim();
                        algorithm = tableAnn.compression();
                    }
                    info = new HBaseTableInfo(type.getName(), tableName, columnFamily, namespace, algorithm);
                    CACHE_TABLE_INFOS.put(type, info);
                    if (logger.isInfoEnabled()) {
                        logger.info("Cached Table Info: {} ", info);
                    }
                }
            }
        }
        return info;
    }

    public static Map<String, HBaseFieldInfo> getFieldInfo(Class<? extends HBaseEntity> type) {
        Map<String, HBaseFieldInfo> infos = CACHE_TABLE_FIELDS.get(type);
        if (infos == null) {
            synchronized (HBaseEntityHelper.class) {
                infos = CACHE_TABLE_FIELDS.get(type);
                if (infos == null) {
                    infos = new HashMap<>();
                    parseEntityFieldInfos(type, infos);
                    CACHE_TABLE_FIELDS.put(type, infos);
                    if (logger.isInfoEnabled()) {
                        logger.info("Cache Table: {} fields info: {}", type.getName(), infos);
                    }
                }
            }
        }
        return infos;
    }

    private static void parseEntityFieldInfos(Class<? extends HBaseEntity> entityClass,
            Map<String, HBaseFieldInfo> infos) {
        parseEntityFieldInfos(entityClass, false, infos);
    }

    @SuppressWarnings("unchecked")
    private static void parseEntityFieldInfos(Class<? extends HBaseEntity> entityClass, boolean isSupper,
            Map<String, HBaseFieldInfo> infos) {
        for (Field f : entityClass.getDeclaredFields()) {
            String name = f.getName();
            if (ignoreField(f) || "rowKey".equals(name) || "properties".equals(name)) {
                continue;
            }

            if (!isSupper) {
                f.setAccessible(true);
            }
            Class<?> type = f.getType();

            String getterName;
            String setterName;
            String suffix = nameToSuffix(name);
            if (isBooleanPrimitive(type)) {
                if (name.length() > 2 && name.startsWith("is") && Character.isUpperCase(name.charAt(2))) {
                    getterName = name;
                    setterName = "set" + name.substring(2);
                } else {
                    getterName = "is" + suffix;
                    setterName = "set" + suffix;
                }
            } else {
                getterName = "get" + suffix;
                setterName = "set" + suffix;
            }

            Method getter = null;
            Method setter = null;
            try {
                getter = entityClass.getMethod(getterName);
                setter = entityClass.getMethod(setterName, type);
            } catch (NoSuchMethodException | SecurityException ex) {
                if (logger.isDebugEnabled()) {
                    logger.debug(ex.getMessage(), ex);
                }
            }

            HBaseField fieldAnn = f.getAnnotation(HBaseField.class);
            String key = name;
            boolean required = false;
            if (fieldAnn != null) {
                if (!isBlank(fieldAnn.name())) {
                    key = fieldAnn.name();
                }
                required = fieldAnn.required();
            }

            infos.put(key, new HBaseFieldInfo(f, name, isSupper, type, getter, setter, key, required));
        }

        if (!entityClass.equals(HBaseEntity.class)) {
            parseEntityFieldInfos((Class<? extends HBaseEntity>) (entityClass.getSuperclass()), true, infos);
        }
    }

    private static String nameToSuffix(String name) {
        if (isFirstLowerCaseAndSecondUpperCase(name)) {
            return name;
        } else {
            return Character.toUpperCase(name.charAt(0)) + (name.length() > 1 ? name.substring(1) : "");
        }
    }

    private static boolean isBooleanPrimitive(Class<?> type) {
        return boolean.class.equals(type) && type.isPrimitive();
    }

    private static boolean isFirstLowerCaseAndSecondUpperCase(String name) {
        return name.length() > 1 && Character.isLowerCase(name.charAt(0)) && Character.isUpperCase(name.charAt(1));
    }

    private static boolean ignoreField(Field f) {
        return Modifier.isStatic(f.getModifiers()) || Modifier.isTransient(f.getModifiers());
    }

    private static boolean isBlank(String s) {
        return s == null || s.trim().length() == 0;
    }

    public static class HBaseTableInfo {

        private final String entityClass;
        private final String tableName;
        private final String columnFamily;
        private final String namespace;
        private final String fullName;
        private final Compression.Algorithm compression;

        //@formatter:off
        public HBaseTableInfo(String entityClass,
                        String tableName, 
                        String columnFamily, 
                        String namespace,
                        Compression.Algorithm compression) {
        //@formatter:on
            this.entityClass = entityClass;
            this.tableName = tableName;
            this.columnFamily = columnFamily;
            this.namespace = namespace;
            this.compression = compression;
            if (!isDefaultNamespace()) {
                this.fullName = namespace + ":" + tableName;
            } else {
                this.fullName = tableName;
            }
        }

        public boolean isDefaultNamespace() {
            return DEFAULT_NAMESPACE.equals(namespace);
        }

        public String getEntityClass() {
            return entityClass;
        }

        public String getTableName() {
            return tableName;
        }

        public String getColumnFamily() {
            return columnFamily;
        }

        public String getNamespace() {
            return namespace;
        }

        public String getFullName() {
            return fullName;
        }

        public Compression.Algorithm getCompression() {
            return compression;
        }

        @Override
        public String toString() {
            return "HBaseTableInfo [entityClass=" + entityClass + ", tableName=" + tableName + ", columnFamily="
                    + columnFamily + ", namespace=" + namespace + ", fullName=" + fullName + ", compression="
                    + compression + "]";
        }

    }

    public static class HBaseFieldInfo {
        private final Field field;
        private final String name;
        private final boolean supperField;
        private final Class<?> type;
        private final Method getter;
        private final Method setter;
        private final String key;
        private final boolean required;

        //@formatter:off
        public HBaseFieldInfo(Field field, 
                        String name, 
                        boolean supperField, 
                        Class<?> type, 
                        Method getter,
                        Method setter, 
                        String key, 
                        boolean required) {
        //@formatter:on
            this.field = field;
            this.name = name;
            this.supperField = supperField;
            this.type = type;
            this.getter = getter;
            this.setter = setter;
            this.key = key;
            this.required = required;
        }

        public Field getField() {
            return field;
        }

        public String getName() {
            return name;
        }

        public boolean isSupperField() {
            return supperField;
        }

        public Class<?> getType() {
            return type;
        }

        public Method getGetter() {
            return getter;
        }

        public Method getSetter() {
            return setter;
        }

        public String getKey() {
            return key;
        }

        public boolean isRequired() {
            return required;
        }

        @Override
        public String toString() {
            return "FieldInfo [name=" + name + ", supperField=" + supperField + ", type=" + type + ", getter="
                    + (getter == null ? "" : getter.getName()) + ", setter=" + (setter == null ? "" : setter.getName())
                    + ", key=" + key + ", required=" + required + "]";
        }

    }
}
