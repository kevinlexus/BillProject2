package com.dic.app.mm;

import com.ric.cmn.excp.WrongGetMethod;
import com.ric.cmn.excp.WrongParam;

import javax.mail.MessagingException;
import java.io.IOException;

public interface MailMng {
    void sendBillsViaEmail() throws IOException, MessagingException, WrongGetMethod, WrongParam;

    void markBillsNotSended();
}
