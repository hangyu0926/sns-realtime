package cn.memedai.orientdb.sns.realtime.common

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx
import org.springframework.stereotype.Service

import javax.annotation.Resource

/**
 * Created by kisho on 2017/6/8.
 */
@Service
class OrientDb {

    private static ODatabaseDocumentTx tx

    @Resource
    private Properties orientDbConfig

    ODatabaseDocumentTx getDb() {
        if (tx == null) {
            tx = new ODatabaseDocumentTx(orientDbConfig.url)
            tx.open(orientDbConfig.userName, orientDbConfig.password)
        }
        return tx
    }

}
