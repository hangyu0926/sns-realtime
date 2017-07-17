package cn.memedai.orientdb.sns.realtime.sql

import com.orientechnologies.common.concur.ONeedRetryException
import com.orientechnologies.orient.core.sql.OCommandSQL
import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory
import org.apache.commons.collections.CollectionUtils
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service

import javax.annotation.PostConstruct
import javax.annotation.Resource
import java.text.MessageFormat

/**
 * Created by kisho on 2017/6/8.
 */
@Service
class OrientSql {

    private static final Logger LOG = LoggerFactory.getLogger(OrientSql.class)

    private String checkEdgeSql = 'select from (select expand(out_{0}) from {1}) where in = {2}'
    private String createEdgeSql = 'create edge {0} from {1} to {2}'

    @Resource
    private Properties orientDbConfig

    private OrientGraphFactory factory

    public <RET> RET execute(String sql, Object... args) {
        long start = System.currentTimeMillis()

        RET ret = null
        int retry = 0
        while (++retry <= 20) {
            try {
                ret = factory.getDatabase().command(new OCommandSQL(sql)).execute(args)
                break
            } catch (ONeedRetryException e) {
                try {
                    Thread.sleep(10)
                } catch (InterruptedException e1) {
                    LOG.error('', e1)
                    throw e1
                }
                continue
            } catch (Throwable e) {
                LOG.error("{} @ {}", sql, e.getMessage())
                LOG.error("", e)
                throw e
            }
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("$sql,$args,${System.currentTimeMillis() - start}ms")
        }
        ret
    }

    void createEdge(String edge, String fromRid, String toRid) {
        if (CollectionUtils.isEmpty(execute(MessageFormat.format(checkEdgeSql, edge, fromRid, toRid)))) {
            execute(MessageFormat.format(createEdgeSql, edge, fromRid, toRid))
        }
    }

    @PostConstruct
    private void init() {
        factory = new OrientGraphFactory(orientDbConfig.url, orientDbConfig.userName, orientDbConfig.password)
        factory.setupPool(Integer.parseInt(orientDbConfig.minPool), Integer.parseInt(orientDbConfig.maxPool))
        LOG.info('OrientDB connection pool:minPool->{},maxPool->{}', Integer.parseInt(orientDbConfig.minPool), Integer.parseInt(orientDbConfig.maxPool))
    }

}
