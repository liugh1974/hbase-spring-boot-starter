package lgh.springboot.starter.hbase.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;

/**
 * 
 * @author Liuguanghua
 *
 */
@Documented
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface HBaseTable {
    String name() default "";

    // default value is the same 'name'
    String columnFamily() default "c0";

    String namespace() default "default";

    Algorithm compression() default Algorithm.NONE;
}
