# README

一个基于Jedis的Redis JDBC驱动。

URL：
* GitHub地址：https://github.com/eacdy/redis-jdbc
* Gitee地址：https://gitee.com/itmuch/redis-jdbc


## 特性
* 支持单节点Redis与Redis Cluster
* 支持所有Jedis支持的命令
* 支持在Intellij IDEA database console中连接Redis
* 支持基于JDBC的ORM框架，例如Mybatis、Hibernate等


## 使用

在项目中添加如下依赖：

```xml
<dependency>
    <groupId>com.itmuch.redis</groupId>
    <artifactId>redis-jdbc</artifactId>
    <version>0.0.1</version>
</dependency>
```

### Redis

```java
Class.forName("com.itmuch.redis.jdbc.redis.RedisDriver");

Connection connection = DriverManager.getConnection(
  "jdbc:redis://localhost:6379/0",
  properties
);
Statement statement = connection.createStatement();

connection.setSchema("11");
ResultSet rs = statement.executeQuery("get a");
while (rs.next()) {
  String string = rs.getString(0);
  System.out.println(string);
}
```

其中，properties中的key可如下表所示：

| key      | defaultValue | description          |
| -------- | ------------ | -------------------- |
| user     | null         | the user of Redis    |
| password | null         | the password of user |
| ssl      | false        | whether to use ssl   |
| timeout  | 1000         | Jedis timeout        |



### Redis Cluster

```java
Class.forName("com.itmuch.redis.jdbc.cluster.RedisClusterDriver");

Connection connection = DriverManager.getConnection(
  "jdbc:redis-cluster:///localhost:6379;localhost:6380;localhost:6381",
  properties
);
Statement statement = connection.createStatement();

connection.setSchema("11");
ResultSet rs = statement.executeQuery("get a");
while (rs.next()) {
  String string = rs.getString(0);
  System.out.println(string);
}
```

其中，properties可如下表所示：

| key         | defaultValue | description          |
| ----------- | ------------ | -------------------- |
| user        | null         | the user of Redis    |
| password    | null         | the password of user |
| ssl         | false        | whether to use ssl   |
| timeout     | 1000         | Jedis timeout        |
| maxAttempts | 5            | Jedis maxAttempts    |

