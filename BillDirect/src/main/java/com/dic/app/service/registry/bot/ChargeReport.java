package com.dic.app.service.registry.bot;

import com.dic.bill.dto.SumCharge;
import com.dic.bill.dto.SumChargeRec;
import com.ric.cmn.Utl;
import org.apache.commons.collections4.CollectionUtils;
import org.springframework.stereotype.Service;

import java.beans.IntrospectionException;
import java.lang.reflect.InvocationTargetException;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class ChargeReport extends BotReportBase {

    public static final String USL = "name";
    public static final String USL_NAME = "Услуга";

    public static final String VOL = "vol";
    public static final String VOL_NAME = "Объем";
    public static final String PRICE = "price";
    public static final String PRICE_NAME = "Цена,руб.";
    public static final String UNIT = "unit";
    public static final String UNIT_NAME = "";
    public static final String SUMMA = "summa";

    public static final String SUMMA_NAME = "Cумма,руб.";

    // отчёт - начисление
    public StringBuilder getStrChargeFormatted(List<SumChargeRec> lst, String period) throws IntrospectionException, InvocationTargetException, IllegalAccessException {
        if (CollectionUtils.isEmpty(lst)) {
            return new StringBuilder();
        }
        Map<String, Column> columns = new HashMap<>();
        columns.put(USL, new Column(USL, USL_NAME));
        columns.put(VOL, new Column(VOL, VOL_NAME));
        columns.put(PRICE, new Column(PRICE, PRICE_NAME));
        columns.put(UNIT, new Column(UNIT, UNIT_NAME));
        columns.put(SUMMA, new Column(SUMMA, SUMMA_NAME));

        // рассчитать макс.размер столбцов
        setMaxColumSize(lst, columns, SumChargeRec.class);
        Column columnVol = columns.get(VOL);
        columnVol.size += columns.get(UNIT).size + 2; // добавляем ед.изм, так как вместе идут эти поля в отчёте +2 - точка и пробел

        StringBuilder msg = new StringBuilder();
        StringBuilder preFormatted = new StringBuilder();
        Column columnUsl = columns.get(USL);
        String uslHeader = columnUsl.getCaptionWithPrefix();
        String volHeader = columnVol.getCaptionWithPrefix();
        Column columnPrice = columns.get(PRICE);
        String priceHeader = columnPrice.getCaptionWithPrefix();
        Column columnSumma = columns.get(SUMMA);
        String summaHeader = columnSumma.getCaptionWithPrefix();
        BigDecimal amnt = BigDecimal.ZERO;
        preFormatted.append("\r\n");
        preFormatted.append("Начисление за период: ").append(Utl.getPeriodName(period, 1)).append("\r\n");
        preFormatted.append(String.format("|%s|%s|%s|%s|\r\n", uslHeader, volHeader, priceHeader, summaHeader));
        for (SumCharge row : lst) {
            amnt = amnt.add(BigDecimal.valueOf(row.getSumma()));

            String usl = columnUsl.getStrFormatted(row.getName());
            String vol = columnVol.getStrFormatted(Utl.getMoneyStrWithLpad(row.getVol(), 0, null, MONEY_PATTERN) +
                    ", " + row.getUnit());
            String price = columnPrice.getValueFormatted(row.getPrice(), MONEY_PATTERN);
            String summa = columnSumma.getValueFormatted(row.getSumma(), MONEY_PATTERN);
            preFormatted.append(String.format("|%s|%s|%s|%s|\r\n", usl, vol, price, summa));
        }
        preFormatted.append(String.format("|%s|%s|%s|%s|\r\n", columnUsl.getStrFormatted(""), columnVol.getStrFormatted(""),
                columnPrice.getStrFormatted("Итого"), columnSumma.getValueFormatted(amnt.doubleValue(), MONEY_PATTERN)));
        msg.append(preFormatted);
        return msg;
    }


}
