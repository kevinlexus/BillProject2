package com.dic.app.mm.impl;

import com.dic.app.mm.ConfigApp;
import com.dic.app.mm.MailMng;
import com.dic.bill.dao.ObjParDAO;
import com.dic.bill.mm.ObjParMng;
import com.ric.cmn.Utl;
import com.ric.cmn.excp.WrongGetMethod;
import com.ric.cmn.excp.WrongParam;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.pdfbox.multipdf.PDFMergerUtility;
import org.apache.pdfbox.multipdf.Splitter;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.text.PDFTextStripper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
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
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

@Slf4j
@Service
@RequiredArgsConstructor
public class MailMngImpl implements MailMng {


    public static final String TEMP_EXPORT_PATH = "C:\\TEMP\\export\\";
    public static final String BILL_SENDED_VIA_EMAIL = "bill_sended_via_email";
    private final ObjParMng objParMng;
    private final ObjParDAO objParDAO;
    private final ConfigApp configApp;
    private final ApplicationContext applicationContext;

    @Value("${billSendEmailFrom}")
    private String EMAIL_FROM;
    @Value("${billSendSMTPUserName}")
    private String SMTP_USER_NAME;
    @Value("${billSendSMTPPass}")
    private String SMTP_PASSWORD;

    @Override
    @Transactional(propagation = Propagation.REQUIRED, rollbackFor = Exception.class)
    public void sendBillsViaEmail() {
        log.info("Начало отправки ПД по email");
        Date dt = configApp.getCurDt1();
        LocalDate ld = LocalDate.ofInstant(dt.toInstant(), ZoneId.systemDefault());
        String periodFormatted = ld.format(DateTimeFormatter.ofPattern("MMM yyyy")
                .withLocale(new Locale("ru", "ru_RU")));
        String subject = "РКЦ Киселевск. Платежный документ за период " + periodFormatted;
        String pathname = TEMP_EXPORT_PATH + configApp.getPeriod();
        File[] files = new File(pathname).listFiles();
        Set<Long> alreadySent = new HashSet<>();
        if (files != null) {
            Stream<File> lstFiles = Stream.of(files);
            lstFiles.forEach(t -> {
                try {
                    sendOneBill(alreadySent, subject, subject, t.getAbsoluteFile());
                } catch (IOException e) {
                    log.error("Ошибка обработки файла ПД {}", t.getAbsoluteFile());
                } catch (WrongParam | WrongGetMethod e) {
                    log.error("Некорректно использованы параметры в t_objxpar");
                }
            });
        } else {
            log.error("Ошибка получения списка файлов из директории {}", pathname);
        }
        log.info("Окончание отправки ПД по email");
    }

    private void sendOneBill(Set<Long> alreadySent, String subject, String msg, File file) throws IOException, WrongParam, WrongGetMethod {
        log.info("Обработка файла {}", file.getAbsolutePath());
        PDDocument document = PDDocument.load(file);
        Splitter splitter = new Splitter();
        List<PDDocument> Pages = splitter.split(document);
        Iterator<PDDocument> iterator = Pages.listIterator();
        PDFTextStripper pdfStripper = new PDFTextStripper();
        PDFMergerUtility pdfMerger;

        int page = 1;
        PDDocument page1;
        Pattern patternEmail = Pattern.compile("(klskId=)(\\d+);");
        pdfMerger = new PDFMergerUtility();
        while (iterator.hasNext()) {
            page1 = iterator.next();
            String textPd = pdfStripper.getText(page1);
            Matcher matcherKlskId = patternEmail.matcher(textPd);
            if (matcherKlskId.find()) {
                long klskId = Long.parseLong(matcherKlskId.group(2));

                //klskId = 104735;
                String email = objParMng.getStr(klskId, "email_lk");
                //email = "factor@mail.ru";
                Boolean isBillAlreadySent = objParMng.getBool(klskId, "bill_sended_via_email");
                if (!alreadySent.contains(klskId) && (isBillAlreadySent == null || !isBillAlreadySent)) {
                    alreadySent.add(klskId);
                    if (email != null) {
                        // получаем 2 страницу
                        if (iterator.hasNext()) {
                            PDDocument page2 = iterator.next();
                            textPd = pdfStripper.getText(page2);
                            matcherKlskId = patternEmail.matcher(textPd);
                            // если это не страница следующего ПД
                            if (!matcherKlskId.find()) {
                                if (textPd.length() > 10) {
                                    pdfMerger.appendDocument(page1, page2);
                                    pdfMerger.mergeDocuments(null);
                                }
                                String fileName = TEMP_EXPORT_PATH + "\\PD_" + klskId + ".pdf";
                                page1.save(fileName);
                                try {
                                    sendEmailMessage(fileName, subject, msg, email);
                                    log.info("Успешно отправлен ПД: klskId={}, fileName={}, page={}, email={}",
                                            klskId, file.getAbsolutePath(), page, email);
                                    /* setBoolNewTransaction выполняет транзакцию в REQUIRES_NEW, а следующий
                                    objParMng.getBool не увидит изменений, (потребовалось бы em.refresh(objPar)),
                                    поэтому использую Set<Long> alreadySent, для проверки уже отправленных ПД*/
                                    objParMng.setBoolNewTransaction(klskId, BILL_SENDED_VIA_EMAIL, true);
                                } catch (MessagingException | IOException e) {
                                    log.error(Utl.getStackTraceString(e));
                                    log.error("Ошибка отправки ПД: klskId={}, fileName={}, page={}, email={}",
                                            klskId, file.getAbsolutePath(), page, email);
                                } finally {
                                    Files.deleteIfExists(Paths.get(fileName));
                                }
                            } else {
                                log.error("Некорректная последовательность страниц в документе! fileName={}, page={}",
                                        file.getAbsolutePath(), page);
                            }

                            page1.close();
                            page2.close();
                        }
                    } else {
                        log.error("Не заполнен email, по klskId={}", klskId);
                    }
                }
            }
            page1.close();
        }
        document.close();
    }

    private void sendEmailMessage(String fileName, String subject, String msg, String to) throws MessagingException, IOException {
        Properties prop = new Properties();
        prop.put("mail.smtp.auth", true);
        prop.put("mail.smtp.host", "smtp.yandex.ru");
        prop.put("mail.smtp.port", "465");
        prop.put("mail.smtp.socketFactory.class", "javax.net.ssl.SSLSocketFactory");
        prop.put("mail.smtp.socketFactory.port", "465");

        Session session = Session.getInstance(prop, new Authenticator() {
            @Override
            protected PasswordAuthentication getPasswordAuthentication() {
                return new PasswordAuthentication(SMTP_USER_NAME, SMTP_PASSWORD);
            }
        });

        Message message = new MimeMessage(session);
        message.setFrom(new InternetAddress(EMAIL_FROM));
        message.setRecipients(
                Message.RecipientType.TO, InternetAddress.parse(to));
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

    @Override
    @Transactional(propagation = Propagation.REQUIRED, rollbackFor = Exception.class)
    public void markBillsNotSended() {
        objParDAO.getAllByCd(BILL_SENDED_VIA_EMAIL).forEach(t -> t.setN1(BigDecimal.ZERO));
    }

}