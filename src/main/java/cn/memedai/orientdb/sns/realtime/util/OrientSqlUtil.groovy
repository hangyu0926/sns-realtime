package cn.memedai.orientdb.sns.realtime.util

import com.orientechnologies.orient.core.record.impl.ODocument
import com.orientechnologies.orient.core.sql.query.OResultSet

/**
 * Created by kisho on 2017/6/9.
 */
final class OrientSqlUtil {

    static final String getRid(orientResult) {
        if (orientResult instanceof OResultSet) {
            return orientResult.get(0).field('@rid').getIdentity().toString()
        } else if (orientResult instanceof ODocument) {
            return orientResult.field('@rid').getIdentity().toString()
        }
        return null;
    }

}
