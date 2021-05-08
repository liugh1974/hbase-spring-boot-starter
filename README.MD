# 项目介绍
  
        自定义的spring-boot hbase starter, 基于springboot.2.3.0.RELEASE 和 hbase-client 2.4.2

# 使用

### 1. 如果maven repository 没有相应的包，请先下载源代码使用maven命令打包
    
        maven command: mvn clean install

### 2. 在项目maven pom.xml 添加以下依赖

        <dependency>
            <groupId>lgh.springboot</groupId>
            <artifactId>hbase-spring-boot-starter</artifactId>
            <version>0.0.1-SNAPSHOT</version>
        </dependency>
        
### 3. application.yml 添加 HBase Server 配置

        hbase:
          config:
            hbase.zookeeper.quorum: zk1,zk2,zk3
            hbase.zookeeper.property.clientPort: 2181
            zookeeper.znode.parent: /hbase
            hbase.client.keyvalue.maxsize: 1048576000
            
### 4. 项目中的具体使用
        
        1). Table Entity 继承 HBaseEntity class
            示例：
            
            @HBaseTable(name = "TABLE1", columnFamily = "c0", compression = Algorithm.NONE)
            public class HBaseTable1 extends HBaseEntity {
                
                @HBaseField(name = "field1", required = true)
                private String field1;      

                private int color;

                private LocationDateTime createTime
                ...
              
            @HBaseTable, @HBaseField 不是非必须annotation。
            如果没有@HBaseTable annotation, 以类名为表; columnFamily默认为c0; compression aligrithm默认为NONE。
            如果没有@HBaesField, 以属性名为key name。

        2). 如果有第三方包继承 HBaesEntity，请在程序入口添加 @HBaseEntityScan annotation
            示例：

            @HBaseEntityScan("扫描包路径")
            @SpringBootApplication
            public class ExampleApplication {
                ...

            以上两步后，程序启动，会自动检查是否有相关表，如果没有，则会自动创建。如果有，则忽略。

        3). 如果表有动态属性，请使用 Entity class  下面方法：
        
            public void addProperty(String key, String value) 
            
            public void addAllProperties(Map<String, String> props)
            
        

        4). 操作数据
            
            Bean 引用
            
            @Autowired
            private HBaseTemplate hbaseTemplate;

            主要方法：
            public <T extends HBaseEntity> void save(T entity);
            
            public <T extends HBaseEntity> void save(List<T> entities);
            
            public <T extends HBaseEntity> T getByRowKey(Class<T> type, String rowKey);
            
            public <T extends HBaseEntity> List<T> getByRowKeys(Class<T> type, List<String> rowKeys);
            
            public <T extends HBaseEntity> T get(T t);
            
            public <T extends HBaseEntity> List<T> query(Class<T> type, String startRowKey, String endRowKey);
            
            public <T extends HBaseEntity> List<T> query(Class<T> type, String startRowKey, String endRowKey, Map<String, Object> filterFields);
            
            public <T extends HBaseEntity> void delete(T t);

            如果以上方法不满足需求，可以通过 public Connection getConnection() 方法得到连接自己定义具体操作。
            