package cn.memedai.orientdb.sns.realtime.service

import cn.memedai.orientdb.sns.realtime.util.MailUtils
import org.apache.commons.codec.binary.Base64
import org.apache.commons.lang.StringUtils
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service

import javax.annotation.Resource

/**
 * Created by kisho on 2017/7/3.
 */
@Service
class MailService {

    private static final Logger LOG = LoggerFactory.getLogger(MailService.class)

    @Resource
    private Properties mailProp

    void sendMail(String body) {
        try {
            String subject = mailProp.getProperty("mail.subject")
            String shellPath = mailProp.getProperty("shell.path")
            if (StringUtils.isBlank(shellPath)) {
                mailProp.put("mail.subject", subject)
                MailUtils.sendEmail(mailProp, body)
            } else {
                StringBuilder builder = new StringBuilder()
                builder.append(shellPath)
                        .append(" ")
                        .append(mailProp.getProperty("mail.to.user"))
                        .append(" ")
                        .append(subject)
                        .append(" ")
                        .append(new Base64().encodeAsString(body.getBytes()))
                LOG.info("execute mail shell : {}", builder.toString())

                Process process = Runtime.getRuntime().exec(builder.toString())
                BufferedReader br = new BufferedReader(new InputStreamReader(process.getInputStream()))
                StringBuilder builder1 = new StringBuilder()
                String s = null
                while ((s = br.readLine()) != null) {
                    builder1.append(s).append("/n")
                }
                LOG.info("send mail response : {}", builder1.toString())
            }
        }
        catch (Throwable e) {
            LOG.error("send mail exception!", e)
        }
    }

    void sendMail(List<Throwable> throwables) {
        //TODO
    }

}
