package com.dic.app.service.registry.bot;

import com.dic.bill.dto.SumFinanceFlow;
import com.ric.cmn.Utl;
import org.springframework.stereotype.Service;

import java.beans.IntrospectionException;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class FlowReport extends BotReportBase {

    public static final String PERIOD_NAME = "Период";
    public static final String DEBT = "debt";
    public static final String DEBT_NAME = "Долг";
    public static final String PEN = "pen";
    public static final String PEN_NAME = "Пени";
    public static final String CHRG = "chrg";
    public static final String CHRG_NAME = "Начисление";
    public static final String PAY = "pay";
    public static final String PAY_NAME = "Оплата";
    public static final String PAYPEN = "paypen";
    public static final String PAYPEN_NAME = "В т.ч.пени";


    // отчёт - движение средств
    public StringBuilder getStrFlowFormatted(List<SumFinanceFlow> lst) throws IntrospectionException, InvocationTargetException, IllegalAccessException {
        Map<String, Column> columns = new HashMap<>();
        columns.put(DEBT, new Column(DEBT, DEBT_NAME));
        columns.put(CHRG, new Column(CHRG, CHRG_NAME));
        columns.put(PAY, new Column(PAY, PAY_NAME));
        columns.put(PEN, new Column(PEN, PEN_NAME));
        columns.put(PAYPEN, new Column(PAYPEN, PAYPEN_NAME));

        // рассчитать макс.размер столбцов
        setMaxColumSize(lst, columns, SumFinanceFlow.class);

        StringBuilder msg = new StringBuilder();
        msg.append("Движение средств за последний год\r\n");
        msg.append("Расчет был произведен:\r\n");
        StringBuilder preFormatted = new StringBuilder("```\r\n");
        String chrgHeader = columns.get(CHRG).getCaptionWithPrefix();
        String debtHeader = columns.get(DEBT).getCaptionWithPrefix();
        String payHeader = columns.get(PAY).getCaptionWithPrefix();
        String penHeader = columns.get(PEN).getCaptionWithPrefix();
        String payPenHeader = columns.get(PAYPEN).getCaptionWithPrefix();

        preFormatted.append(String.format("|%s |%s|%s|%s|%s|%s|\r\n", PERIOD_NAME, debtHeader, penHeader, chrgHeader, payHeader, payPenHeader));
        for (SumFinanceFlow row : lst) {
            String debt = columns.get(DEBT).getValueFormatted(row.getDebt(), MONEY_PATTERN);
            String pen = columns.get(PEN).getValueFormatted(row.getPen(), MONEY_PATTERN);
            String chrg = columns.get(CHRG).getValueFormatted(row.getChrg(), MONEY_PATTERN);
            String pay = columns.get(PAY).getValueFormatted(row.getPay(), MONEY_PATTERN);
            String paypen = columns.get(PAYPEN).getValueFormatted(row.getPayPen(), MONEY_PATTERN);
            preFormatted.append(String.format("|%s|%s|%s|%s|%s|%s|\r\n", row.getMg() + " ", debt, pen, chrg, pay, paypen));
        }
        Utl.replaceAll(preFormatted, ".", "\\.");
        Utl.replaceAll(preFormatted, "|", "\\|");
        preFormatted.append("```");
        msg.append(preFormatted);
        return msg;
    }

}
