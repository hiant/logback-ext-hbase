# logback-ext-hbase

参照 DBAppender 创建表结构，当然表结构做了部分微调，使用了org.apache.hadoop.hbase.client.Admin在logger初始化的时候**自动**创建所需的表。
  使用commons-pool2来构建了一个客户端连接池，避免每次写日志的过程中都创建、关闭链接。

由于 HBase 及其依赖包也会有日志输出，因此在使用时需要注意**仅能**将需要输出到 HBase 的那些日志的 logger 绑定到 io.github.hiant.HBaseAppender 上。

下面是 logback-test.xml 示例
  
    <appender name="HBase" class="io.github.hiant.HBaseAppender">
      <configuration class="io.github.hiant.SimpleConfigurationFactory">
          <quorum>172.22.0.28</quorum>
          <port>2181</port>
      </configuration>
    </appender>
    
    <logger name="io.github.hiant" level="TRACE">
      <appender-ref ref="HBase"/>
    </logger>
