package lgh.springboot.starter.hbase.utils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lgh.springboot.starter.hbase.annotation.HBaseEntityScan;
import lgh.springboot.starter.hbase.entity.HBaseEntity;

/**
 * 
 * @author Liuguanghua
 *
 */
public class HBaseScanUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(HBaseScanUtils.class);

    @SuppressWarnings("unchecked")
    public static List<Class<? extends HBaseEntity>> scan() {
        String mainClassname = ClassUtils.getMainClassName();
        if (mainClassname == null) {
            return Collections.emptyList();
        }

        try {
            List<String> scanPackages = new ArrayList<>();

            Class<?> mainClass = Class.forName(mainClassname);
            scanPackages.add(mainClass.getPackage().getName());

            HBaseEntityScan ann = mainClass.getAnnotation(HBaseEntityScan.class);
            if (ann != null) {
                scanPackages.addAll(Arrays.asList(ann.value()));
            }

            List<Class<?>> list = ClassUtils.scanClasses(scanPackages.toArray(new String[0]),
                    HBaseEntity.class);
            return (List<Class<? extends HBaseEntity>>) (Object) list;
        } catch (ClassNotFoundException ignoreException) {
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace(ignoreException.getMessage(), ignoreException);
            }
        }
        return Collections.emptyList();
    }
}
