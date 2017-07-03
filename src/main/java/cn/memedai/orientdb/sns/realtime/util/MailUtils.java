package cn.memedai.orientdb.sns.realtime.util;

import javax.mail.Message;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;
import java.util.Date;
import java.util.Properties;

public final class MailUtils {

    /**
     * <pre>
     *     Properties中可设置如下值
     *     mail.smtp.host
     *     mail.smtp.port
     *     mail.smtp.auth
     *     mail.from.user
     *     mail.from.password"
     *     mail.from.displayName
     *     mail.subject
     *     mail.to.user
     * </pre>
     *
     * @param prop
     * @param body
     */
    public static void sendEmail(Properties prop, String body) {
        try {
            Session session = Session.getDefaultInstance(prop, null);
            Transport transport = session.getTransport("smtp");
            transport.connect(prop.getProperty("mail.from.user"), prop.getProperty("mail.from.password"));
            MimeMessage msg = new MimeMessage(session);
            msg.setSentDate(new Date());
            InternetAddress fromAddress = new InternetAddress(prop.getProperty("mail.from.user"), prop.getProperty("mail.from.displayName"), "UTF-8");
            msg.setFrom(fromAddress);
            String[] toUsers = prop.getProperty("mail.to.user").split(",");
            InternetAddress[] toAddresses = new InternetAddress[toUsers.length];
            for (int i = 0; i < toUsers.length; i++) {
                toAddresses[i] = new InternetAddress(toUsers[i].trim());
            }
            msg.setRecipients(Message.RecipientType.TO, toAddresses);
            msg.setSubject(prop.getProperty("mail.subject"), "UTF-8");
            msg.setContent(body, "text/html; charset=utf-8");
            msg.saveChanges();
            transport.sendMessage(msg, msg.getAllRecipients());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


}