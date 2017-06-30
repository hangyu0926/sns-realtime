package cn.memedai.orientdb.sns.realtime

import groovy.sql.Sql
import org.junit.Test
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import javax.annotation.Resource

class BatchTest extends AbstractRealTimeTest {

    private static final Logger LOG = LoggerFactory.getLogger(BatchTest.class)

    @Resource
    private Sql groovySql

    @Test
    void batchTestWalletApplyInfo() {
        List<Map> dataList = []
        groovySql.eachRow("select * from network.apply_info where created_datetime between '2017-06-29 00:00:00' and '2017-06-29 23:59:59' or modified_datetime between '2017-06-29 00:00:00' and '2017-06-29 23:59:59'",
                {
                    row ->
                        dataList.add([
                                'cellphone'       : row.cellphone,
                                'apply_no'        : row.apply_no,
                                'member_id'       : row.member_id,
                                'created_datetime': row.created_datetime.toString(),
                                'apply_status'    : row.apply_status,
                                'store_id'        : row.store_id,
                                'order_no'        : row.order_no,
                                '___op___'        : 'insert'
                        ])
                        for (int i = 0; i < 10; i++) {
                            dataList.add([
                                    'cellphone'       : row.cellphone,
                                    'apply_no'        : row.apply_no,
                                    'member_id'       : row.member_id,
                                    'created_datetime': row.created_datetime.toString(),
                                    'apply_status'    : row.apply_status,
                                    'store_id'        : row.store_id,
                                    'order_no'        : row.order_no,
                                    '___op___'        : 'update'
                            ])
                        }
                }

        )
        produce('wallet', 'apply_info', dataList)
    }


    @Test
    void batchTestWalletMoneyBoxOrder() {
        List<Map> dataList = []
        groovySql.eachRow("select * from network.money_box_order where created_datetime between '2017-06-29 00:00:00' and '2017-06-29 23:59:59' or modified_datetime between '2017-06-29 00:00:00' and '2017-06-29 23:59:59'",
                {
                    row ->
                        for (int i = 0; i < 10; i++) {
                            dataList.add([
                                    'mobile'          : row.mobile,
                                    'member_id'       : row.member_id,
                                    'order_no'        : row.order_no,
                                    'created_datetime': row.created_datetime.toString(),
                                    'status'          : row.status,
                                    'store_id'        : row.store_id,
                                    'pay_amount'      : row.pay_amount,
                                    '___op___'        : 'insert'
                            ])
                            dataList.add([
                                    'mobile'          : row.mobile,
                                    'member_id'       : row.member_id,
                                    'order_no'        : row.order_no,
                                    'created_datetime': row.created_datetime.toString(),
                                    'status'          : row.status,
                                    'store_id'        : row.store_id,
                                    'pay_amount'      : row.pay_amount,
                                    '___op___'        : 'update'
                            ])
                        }
                }

        )
        produce('wallet', 'money_box_order', dataList)
    }


}