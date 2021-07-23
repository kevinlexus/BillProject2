package com.dic.app.mm.impl;

import com.dic.app.mm.MailMng;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.apache.pdfbox.multipdf.PDFMergerUtility;
import org.apache.pdfbox.multipdf.Splitter;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.text.PDFTextStripper;

@Slf4j
@Service
public class MailMngImpl implements MailMng {


    @Override
    public void sendBillViaEmail() throws IOException {
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

        //Saving each page as an individual document
        int i = 1;
        int a = 0;
        PDDocument pdSource = null;
        while (iterator.hasNext()) {
            PDDocument pd = iterator.next();
            if (a == 0) {
                pdfMerger = new PDFMergerUtility();
                pdSource = pd;
            }
            System.out.println("doc=" + i++);

            //String text = pdfStripper.getText(pd);
            //System.out.println(text);
            if (a++ > 0) {
                //pdfMerger.setDestinationFileName("D:\\TEMP\\65_check_pdfbox\\check_" + i + ".pdf");
                pdfMerger.appendDocument(pdSource, pd);
                pdfMerger.mergeDocuments(null);
                pdSource.save("D:\\TEMP\\65_check_pdfbox\\check_" + i + ".pdf");
                pdSource.close();
                pd.close();
                a = 0;
                //break;
            }
        }
        document.close();

    }


}