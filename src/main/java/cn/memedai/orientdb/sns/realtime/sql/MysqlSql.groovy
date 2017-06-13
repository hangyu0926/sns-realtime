package cn.memedai.orientdb.sns.realtime.sql

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx
import org.springframework.stereotype.Service

import javax.annotation.Resource
import java.sql.Connection
import java.sql.DriverManager
import java.sql.SQLException

/**
 * Created by hangyu on 2017/6/13.
 */
@Service
class MysqlSql {

    @Resource
    private Properties mysqlJdbcConfig

    static {
        try {
            Class.forName("com.mysql.jdbc.Driver")

        } catch (ClassNotFoundException e) {
        }
    }

     public Connection getConnection() {
        try {
            Connection connection = DriverManager.getConnection(mysqlJdbcConfig.url, mysqlJdbcConfig.userName, mysqlJdbcConfig.password)
            return connection
        } catch (SQLException e) {
            throw new RuntimeException(e)
        }
    }
}
