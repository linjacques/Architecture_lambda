def mysql_jdbc_config():
    jdbc_url = "jdbc:mysql://mysql:3306/spark_db"
    connection_properties = {
        "user": "root",
        "password": "rootpass",
        "driver": "com.mysql.cj.jdbc.Driver"
    }
    return jdbc_url, connection_properties
