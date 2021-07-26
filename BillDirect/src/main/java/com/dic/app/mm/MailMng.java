package com.dic.app.mm;

import javax.mail.MessagingException;
import java.io.IOException;

public interface MailMng {
    void sendBillViaEmail() throws IOException, MessagingException;
}
