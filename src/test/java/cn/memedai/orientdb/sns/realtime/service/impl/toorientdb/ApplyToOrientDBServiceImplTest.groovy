package cn.memedai.orientdb.sns.realtime.service.impl.toorientdb

import org.junit.Test
import org.springframework.test.context.ContextConfiguration
import org.springframework.test.context.junit4.AbstractJUnit4SpringContextTests

import javax.annotation.Resource

/**
 * Created by kisho on 2017/6/9.
 */
@ContextConfiguration("classpath:applicationContext.xml")
class ApplyToOrientDBServiceImplTest extends AbstractJUnit4SpringContextTests {

    @Resource
    private ApplyToOrientDBServiceImpl applyTeleporterService

    @Test
    void testProcess() {
        def dataList = [
                [
                        'cellphone'       : '13368659853',
                        'member_id'       : 1402582L,
                        'apply_no'        : '1485313547297000',
                        'created_datetime': '2017-01-25 11:05:47',
                        'apply_status'    : 3000,
                        'store_id'        : '2759',
                        'order_no'        : '1496921804405003'
                ]
        ]
        applyTeleporterService.process(dataList)
    }
}
