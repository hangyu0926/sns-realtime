package cn.memedai.orientdb.sns.realtime.service.impl.preprocess

import cn.memedai.orientdb.sns.realtime.KafkaConfig
import cn.memedai.orientdb.sns.realtime.service.RealTimeService
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.avro.io.BinaryEncoder
import org.apache.avro.io.DatumWriter
import org.apache.avro.io.EncoderFactory
import org.apache.avro.specific.SpecificDatumWriter
import org.apache.commons.collections.CollectionUtils
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerRecord
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service

import javax.annotation.PostConstruct
import javax.annotation.Resource

/**
 * Created by kisho on 2017/6/8.
 */
@Service
class CreditAuditCaBurOperatorContactPreprocessServiceImpl implements RealTimeService {

    private static final LOG = LoggerFactory.getLogger(CreditAuditCaBurOperatorContactPreprocessServiceImpl.class)

    @Resource
    private KafkaConfig kafkaConfig

    @Resource
    private Properties kafkaProducerProp

    private Producer<String, String> producer

    private ByteArrayOutputStream baos = new ByteArrayOutputStream()

    private DatumWriter writer

    void process(List<Map<String, Object>> dataList) {
        if (CollectionUtils.isEmpty(dataList)) {
            return
        }
        dataList.each {
            data ->
                GenericRecord record = data['___genericRecord___']

                BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(baos, null)

                getWriter().write(record, encoder)
                encoder.flush()

                baos.flush()

                producer.send(new ProducerRecord<String, Byte[]>('com.mime.bdp.dts.credit_audit2', data.APPL_NO, baos.toByteArray()))
        }
    }

    @PostConstruct
    private void init() {
        producer = new KafkaProducer<>(kafkaProducerProp)
    }

    private DatumWriter getWriter() {
        if (writer == null) {
            writer = new SpecificDatumWriter(getSchema())
        }
        writer
    }

    private Schema getSchema() {
        kafkaConfig.dbtable2SchemaMap['credit_audit2ca_bur_operator_contact']
    }

}
