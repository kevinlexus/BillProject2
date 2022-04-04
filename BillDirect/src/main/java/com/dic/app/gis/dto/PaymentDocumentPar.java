package com.dic.app.gis.dto;

import com.dic.bill.model.exs.Eolink;
import com.dic.bill.model.exs.Pdoc;
import lombok.Builder;
import lombok.Getter;
import lombok.Value;
import ru.gosuslugi.dom.schema.integration.bills.ImportPaymentDocumentRequest;

@Value
@Builder
public class PaymentDocumentPar {
    Eolink uk;
    Pdoc pdoc;
    ImportPaymentDocumentRequest req;
    String tguidPay;
    Eolink ukReference;

    /**
     * @param uk          организация
     * @param pdoc        ПД
     * @param req         запрос
     * @param tguidPay    транспортный GUID платежных реквизитов
     * @param ukReference УК, по которой используется справочник услуг для ПД
     */
    public PaymentDocumentPar(Eolink uk, Pdoc pdoc, ImportPaymentDocumentRequest req, String tguidPay, Eolink ukReference) {
        this.uk = uk;
        this.pdoc = pdoc;
        this.req = req;
        this.tguidPay = tguidPay;
        this.ukReference = ukReference;
    }

    public Eolink getUk() {
        return uk;
    }

    public Pdoc getPdoc() {
        return pdoc;
    }

    public ImportPaymentDocumentRequest getReq() {
        return req;
    }

    public String getTguidPay() {
        return tguidPay;
    }

    public Eolink getUkReference() {
        return ukReference;
    }
}
