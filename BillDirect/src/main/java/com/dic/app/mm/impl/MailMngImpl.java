package com.dic.app.mm.impl;

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
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Slf4j
@Service
@RequiredArgsConstructor
public class MailMngImpl implements MailMng {


    private final KartDAO kartDao;
    private final ObjParMng objParMng;

    @Override
    @Transactional(propagation = Propagation.REQUIRED, rollbackFor = Exception.class)
    public void sendBillViaEmail() throws IOException, MessagingException {
        //Loading an existing PDF document
        File file = new File("D:\\TEMP\\65_check_pdfbox\\014_ОСН_2_0.pdf");
        PDDocument document = PDDocument.load(file);

        //Instantiating Splitter class
        Splitter splitter = new Splitter();

        //splitting the pages of a PDF document
        List<PDDocument> Pages = splitter.split(document);

        //Creating an iterator
        Iterator<PDDocument> iterator = Pages.listIterator();

        PDFTextStripper pdfStripper = new PDFTextStripper();

        PDFMergerUtility pdfMerger = null;

        kartDao.findAll().forEach(t -> {
            try {
                objParMng.setBool(t.getKoKw().getId(), "bill_sended_via_email", true);
            } catch (WrongParam wrongParam) {
                wrongParam.printStackTrace();
            } catch (WrongGetMethod wrongGetMethod) {
                wrongGetMethod.printStackTrace();
            }
        });

        //Saving each a as an individual document
        int page = 1;
        int a = 0;
        PDDocument pdSource = null;
        Pattern patternEmail = Pattern.compile("(klskId=)(\\d+);(.+)");
        while (iterator.hasNext()) {
            PDDocument pd = iterator.next();
            if (a == 0) {
                pdfMerger = new PDFMergerUtility();
                pdSource = pd;
            }
            //System.out.println("doc=" + page++);

            String text = pdfStripper.getText(pd);
            Matcher matcherEmail = patternEmail.matcher(text);

            try {
                long klskId = Long.parseLong(matcherEmail.group(0));
                String email = matcherEmail.group(1);
                log.info("klskId={}, email={}", klskId, email);
                if (a++ > 0) {
                    //pdfMerger.setDestinationFileName("D:\\TEMP\\65_check_pdfbox\\check_" + page + ".pdf");
                    pdfMerger.appendDocument(pdSource, pd);
                    pdfMerger.mergeDocuments(null);
                    String fileName = "D:\\TEMP\\65_check_pdfbox\\check_" + page + ".pdf";
                    pdSource.save(fileName);
                    pdSource.close();
                    pd.close();
                    //sendEmailMessage(fileName);
                    a = 0;
                    //break;
                }
            } catch (NumberFormatException | IllegalStateException e) {
                log.error("Некорректные данные в поле klskId, email ПД, страница={}", page);
            } catch (IOException e) {
                log.error("Ошибка сохранения ПД");
            }

        }
        document.close();

    }

    private void sendEmailMessage(String fileName) throws MessagingException, IOException {
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
        message.setSubject("Mail Subject");

        String msg = "This is my first email using JavaMailer";

        MimeBodyPart mimeBodyPart = new MimeBodyPart();
        mimeBodyPart.setContent(msg, "text/html");

        Multipart multipart = new MimeMultipart();
        multipart.addBodyPart(mimeBodyPart);

        MimeBodyPart attachmentBodyPart = new MimeBodyPart();
        attachmentBodyPart.attachFile(new File(fileName));
        multipart.addBodyPart(attachmentBodyPart);

        message.setContent(multipart);
        Transport.send(message);
    }


}