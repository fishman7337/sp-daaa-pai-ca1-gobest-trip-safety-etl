# Vendor Assets

`mysql-connector-j-8.0.33.jar` is the MySQL Connector/J driver used by the
PySpark JDBC jobs. It is third-party software and remains under its upstream
license, not this repository's MIT license.

You can replace it by setting:

```bash
export PAI_MYSQL_JDBC_JAR=/path/to/mysql-connector-j.jar
```
