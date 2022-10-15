package com.dic.app.service.registry.bot;

import com.dic.bill.dto.SumCharge;
import com.dic.bill.dto.SumPayment;
import com.ric.cmn.Utl;
import org.springframework.stereotype.Service;

import java.beans.IntrospectionException;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class PaymentReport extends BotReportBase {

    public static final String DT = "dt";
    public static final String DT_NAME = "Дата платежа";
    public static final String SUM = "summa";
    public static final String SUM_NAME = "Сумма";
    public static final String SOURCE = "source";
    public static final String SOURCE_NAME = "Источник поступления";

    // отчёт - поступление оплаты
    public StringBuilder getStrPaymentFormatted(List<SumPayment> lst) throws IntrospectionException, InvocationTargetException, IllegalAccessException {
        Map<String, Column> columns = new HashMap<>();
        columns.put(DT, new Column(DT, DT_NAME));
        columns.put(SUM, new Column(SUM, SUM_NAME));
        columns.put(SOURCE, new Column(SOURCE, SOURCE_NAME));

        // рассчитать макс.размер столбцов
        setMaxColumSize(lst, columns, SumPayment.class);

        StringBuilder msg = new StringBuilder();
        msg.append("Оплата\r\n");
        StringBuilder preFormatted = new StringBuilder("```\r\n");
        String dtHeader = columns.get(DT).getCaptionWithPrefix();
        String sumHeader = columns.get(SUM).getCaptionWithPrefix();
        String sourceHeader = columns.get(SOURCE).getCaptionWithPrefix();

        preFormatted.append(String.format("|%s|%s|%s|\r\n", dtHeader, sumHeader, sourceHeader));
        for (SumPayment row : lst) {
            String dt = columns.get(DT).getStrFormatted(Utl.getStrFromDate(row.getDt()));
            String sum = columns.get(SUM).getValueFormatted(row.getSumma(), MONEY_PATTERN);
            String source = columns.get(SOURCE).getStrFormatted(row.getSource());
            preFormatted.append(String.format("|%s|%s|%s|\r\n", dt, sum, source));
        }
        Utl.replaceAll(preFormatted, ".", "\\.");
        Utl.replaceAll(preFormatted, "|", "\\|");
        preFormatted.append("```");
        msg.append(preFormatted);
        return msg;
    }


}
