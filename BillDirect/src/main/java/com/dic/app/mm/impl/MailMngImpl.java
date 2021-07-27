package com.dic.app.mm.impl;

import com.dic.app.mm.ConfigApp;
import com.dic.app.mm.MailMng;
import com.dic.bill.dao.KartDAO;
import com.dic.bill.mm.ObjParMng;
import com.ric.cmn.excp.WrongGetMethod;
import com.ric.cmn.excp.WrongParam;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.pdfbox.multipdf.PDFMergerUtility;
import org.apache.pdfbox.multipdf.Splitter;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.text.PDFTextStripper;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import javax.mail.*;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeBodyPart;
import javax.mail.internet.MimeMessage;
import javax.mail.internet.MimeMultipart;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Slf4j
@Service
@RequiredArgsConstructor
public class MailMngImpl implements MailMng {


    private final KartDAO kartDao;
    private final ObjParMng objParMng;
    private final ConfigApp configApp;

    @Override
    @Transactional(propagation = Propagation.REQUIRED, rollbackFor = Exception.class)
    public void sendBillViaEmail() throws IOException, MessagingException, WrongGetMethod, WrongParam {
        Date dt = configApp.getCurDt1();
        LocalDate ld = LocalDate.ofInstant(dt.toInstant(), ZoneId.systemDefault());
        String period = ld.format(DateTimeFormatter.ofPattern("MMM yyyy")
                .withLocale(new Locale("ru", "ru_RU")));
        String subject = "РКЦ Киселевск. Платежный документ за период " + period;
        String msg = subject;

        File file = new File("D:\\TEMP\\65_check_pdfbox\\email_014_ОСН_2_0.pdf");
        //File file = new File("D:\\TEMP\\65_check_pdfbox\\014_ОСН_2_0.pdf");
        PDDocument document = PDDocument.load(file);

        Splitter splitter = new Splitter();
        List<PDDocument> Pages = splitter.split(document);
        Iterator<PDDocument> iterator = Pages.listIterator();
        PDFTextStripper pdfStripper = new PDFTextStripper();
        PDFMergerUtility pdfMerger;

        kartDao.findAll().forEach(t -> {
            try {
                objParMng.setBool(t.getKoKw().getId(), "bill_sended_via_email", true);
            } catch (WrongParam | WrongGetMethod wrongParam) {
                wrongParam.printStackTrace();
            }
        });

        //Saving each a as an individual document
        int page = 1;
        int a = 0;
        PDDocument page1;
        Pattern patternEmail = Pattern.compile("(klskId=)(\\d+);");
        while (iterator.hasNext()) {
            page1 = iterator.next();
            String textPd = pdfStripper.getText(page1);
            Matcher matcherKlskId = patternEmail.matcher(textPd);
            if (matcherKlskId.find()) {
                long klskId = Long.parseLong(matcherKlskId.group(2));
                String email = objParMng.getStr(klskId, "email_lk");
                email = "factor@mail.ru"; // fixme email!
                pdfMerger = new PDFMergerUtility();
                if (email != null) {
                    // получаем 2 страницу
                    if (iterator.hasNext()) {
                        PDDocument page2 = iterator.next();
                        textPd = pdfStripper.getText(page2);
                        matcherKlskId = patternEmail.matcher(textPd);
                        if (textPd.length() != 0) {
                            // если это не страница следующего ПД
                            if (!matcherKlskId.find()) {
                                pdfMerger.appendDocument(page1, page2);
                                pdfMerger.mergeDocuments(null);
                                String fileName = "D:\\TEMP\\65_check_pdfbox\\PD_" + klskId + ".pdf";
                                page1.save(fileName);
                                try {
                                    sendEmailMessage(fileName, subject, msg);
                                    log.info("Успешно отправлен ПД: klskId={}, page={}, email={}",
                                            klskId, page, email);
                                } catch (MessagingException | IOException e) {
                                    log.error("Ошибка отправки ПД: klskId={}, page={}, email={}",
                                            klskId, page, email);
                                } finally {
                                    Files.deleteIfExists(Paths.get(fileName));
                                }
                            } else {
                                log.error("Некорректная последовательность страниц в документе! page={}", page);
                            }
                        }

                        page1.close();
                        page2.close();
                    }
                } else {
                    log.error("Не заполнен email, по klskId={}", klskId);
                }
            }
        }
        document.close();

    }

    private void sendEmailMessage(String fileName, String subject, String msg) throws MessagingException, IOException {
        Properties prop = new Properties();
        prop.put("mail.smtp.auth", true);
        prop.put("mail.smtp.host", "smtp.yandex.ru");
        prop.put("mail.smtp.port", "465");
        prop.put("mail.smtp.socketFactory.class", "javax.net.ssl.SSLSocketFactory");
        prop.put("mail.smtp.socketFactory.port", "465");

        Session session = Session.getInstance(prop, new Authenticator() {
            @Override
            protected PasswordAuthentication getPasswordAuthentication() {
                return new PasswordAuthentication("kvitokgku", "bnvwgrdyoxvmzcxr");
            }
        });

        Message message = new MimeMessage(session);
        message.setFrom(new InternetAddress("kvitokgku@yandex.ru"));
        message.setRecipients(
                Message.RecipientType.TO, InternetAddress.parse("factor@mail.ru"));
        message.setSubject(subject);

        MimeBodyPart mimeBodyPart = new MimeBodyPart();
        mimeBodyPart.setContent(msg, "text/html; charset=UTF-8");

        Multipart multipart = new MimeMultipart();
        multipart.addBodyPart(mimeBodyPart);

        MimeBodyPart attachmentBodyPart = new MimeBodyPart();
        attachmentBodyPart.attachFile(new File(fileName));
        multipart.addBodyPart(attachmentBodyPart);

        message.setContent(multipart);
        Transport.send(message);
    }


}