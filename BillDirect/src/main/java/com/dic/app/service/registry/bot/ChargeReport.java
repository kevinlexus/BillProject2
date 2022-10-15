package com.dic.app.service.registry.bot;

import com.dic.bill.dto.SumCharge;
import com.dic.bill.dto.SumChargeRec;
import com.ric.cmn.Utl;
import org.springframework.stereotype.Service;

import java.beans.IntrospectionException;
import java.lang.reflect.InvocationTargetException;
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
    public static final String UNIT_NAME = "Ед.изм.";
    public static final String SUMMA = "summa";

    public static final String SUMMA_NAME = "Cумма,руб.";

    // отчёт - текущее начисление
    public StringBuilder getStrChargeFormatted(List<SumChargeRec> lst) throws IntrospectionException, InvocationTargetException, IllegalAccessException {
        Map<String, Column> columns = new HashMap<>();
        columns.put(USL, new Column(USL, USL_NAME));
        columns.put(VOL, new Column(VOL, VOL_NAME));
        columns.put(PRICE, new Column(PRICE, PRICE_NAME));
        columns.put(UNIT, new Column(UNIT, UNIT_NAME));
        columns.put(SUMMA, new Column(SUMMA, SUMMA_NAME));

        // рассчитать макс.размер столбцов
        setMaxColumSize(lst, columns, SumChargeRec.class);

        StringBuilder msg = new StringBuilder();
        msg.append("Начисление\r\n");
        StringBuilder preFormatted = new StringBuilder("```\r\n");
        String uslHeader = columns.get(USL).getCaptionWithPrefix();
        String volHeader = columns.get(VOL).getCaptionWithPrefix();
        String priceHeader = columns.get(PRICE).getCaptionWithPrefix();
        String unitHeader = columns.get(UNIT).getCaptionWithPrefix();
        String summaHeader = columns.get(SUMMA).getCaptionWithPrefix();

        preFormatted.append(String.format("|%s|%s|%s|%s|%s|\r\n", uslHeader, volHeader, priceHeader, unitHeader, summaHeader));
        for (SumCharge row : lst) {
            String usl = columns.get(USL).getStrFormatted(row.getName());
            String vol = columns.get(VOL).getValueFormatted(row.getVol(), MONEY_PATTERN);
            String price = columns.get(PRICE).getValueFormatted(row.getPrice(), MONEY_PATTERN);
            String unit = columns.get(UNIT).getStrFormatted(row.getUnit());
            String summa = columns.get(SUMMA).getValueFormatted(row.getSumma(), MONEY_PATTERN);
            preFormatted.append(String.format("|%s|%s|%s|%s|%s|\r\n", usl, vol, price, unit, summa));
        }
        Utl.replaceAll(preFormatted, ".", "\\.");
        Utl.replaceAll(preFormatted, "|", "\\|");
        preFormatted.append("```");
        msg.append(preFormatted);
        return msg;
    }


}
