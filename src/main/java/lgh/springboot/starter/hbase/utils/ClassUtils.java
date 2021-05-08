package lgh.springboot.starter.hbase.utils;

import java.io.File;
import java.net.JarURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

/**
 * 
 * @author Liuguanghua
 *
 */
public class ClassUtils {

    public static StackTraceElement getCurrentStackTrace() {
        StackTraceElement[] elements = Thread.currentThread().getStackTrace();
        if (elements.length > 1) {
            return elements[1];
        }
        return null;
    }

    public static String getMainClassName() {
        StackTraceElement[] elements = new Throwable().getStackTrace();
        for (StackTraceElement e : elements) {
            if ("main".equals(e.getMethodName())) {
                return e.getClassName();
            }
        }
        return null;
    }

    /**
     * 
     * @param packageNames
     * @param filterParents
     * @return class list which extend or implement filterParents class or interface
     */
    public static List<Class<?>> scanClasses(String[] packageNames, Class<?>... filterParents) {
        List<Class<?>> list = new ArrayList<>();
        for (String packageName : packageNames) {
            list.addAll(scanClasses(packageName));
        }

        if (filterParents == null) {
            return list;
        }

        List<Class<?>> results = new ArrayList<>();
        for (Class<?> type : list) {
            for (Class<?> parent : filterParents) {
                if (parent.isAssignableFrom(type) && !type.equals(parent)) {
                    results.add(type);
                }
            }
        }

        return results;
    }

    public static List<Class<?>> scanClasses(String[] packageNames) {
        List<Class<?>> list = new ArrayList<>();
        for (String packageName : packageNames) {
            list.addAll(scanClasses(packageName));
        }
        return list;
    }

    /**
     * default recursive scan
     * 
     * @param packageName
     * 
     * @return java.util.List&lt;Class&lt;?&gt;&gt;
     */
    public static List<Class<?>> scanClasses(String packageName) {
        return scanClasses(packageName, true);
    }

    /**
     * 
     * @param packageName
     * @param isRecursive
     * @return java.util.List&lt;Class&lt;?&gt;&gt;
     */
    public static List<Class<?>> scanClasses(String packageName, boolean isRecursive) {
        try {
            List<Class<?>> classList = new ArrayList<>();
            Enumeration<URL> urls = Thread.currentThread().getContextClassLoader()
                    .getResources(packageName.replaceAll("\\.", "/"));
            while (urls.hasMoreElements()) {
                URL url = urls.nextElement();
                String protocol = url.getProtocol();
                if ("file".equals(protocol)) {
                    scan(classList, url.getPath(), packageName, isRecursive);
                } else if ("jar".equals(protocol)) {
                    JarFile jarFile = ((JarURLConnection) url.openConnection()).getJarFile();
                    Enumeration<JarEntry> jarEntries = jarFile.entries();
                    while (jarEntries.hasMoreElements()) {
                        JarEntry jarEntry = jarEntries.nextElement();
                        String entryName = jarEntry.getName();
                        if (entryName.endsWith(".class")) {
                            String className = entryName.substring(0, entryName.lastIndexOf(".")).replaceAll("/", ".");
                            if (isRecursive || packageName.equals(className.substring(0, className.lastIndexOf(".")))) {
                                classList.add(Class.forName(className));
                            }
                        }
                    }
                }
            }
            return classList;
        } catch (Exception ex) {
            throw new IllegalArgumentException(ex);
        }
    }

    private static void scan(List<Class<?>> classList, String packagePath, String packageName, boolean isRecurisive)
            throws ClassNotFoundException {
        File[] files = new File(packagePath)
                .listFiles(f -> (f.isFile() && f.getName().endsWith(".class")) || f.isDirectory());
        for (File file : files) {
            String filename = file.getName();
            if (file.isFile()) {
                String className = filename.substring(0, filename.lastIndexOf("."));
                classList.add(Class.forName(packageName + "." + className));
            } else {
                if (isRecurisive) {
                    String childPackagePath = packagePath + File.separator + filename;
                    String childPackageName = packageName + "." + filename;
                    scan(classList, childPackagePath, childPackageName, isRecurisive);
                }
            }
        }
    }
}
