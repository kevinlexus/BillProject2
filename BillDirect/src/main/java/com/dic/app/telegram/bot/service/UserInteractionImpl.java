package com.dic.app.telegram.bot.service;

import com.dic.app.service.ConfigApp;
import com.dic.app.service.registry.RegistryMngImpl;
import com.dic.app.telegram.bot.message.TelegramMessage;
import com.dic.app.telegram.bot.service.client.Env;
import com.dic.app.telegram.bot.service.menu.MeterValSaveState;
import com.dic.app.telegram.bot.service.message.MessageStore;
import com.dic.bill.dao.OrgDAO;
import com.dic.bill.mm.MeterMng;
import com.dic.bill.mm.ObjParMng;
import com.dic.bill.model.scott.Ko;
import com.ric.cmn.Utl;
import com.ric.dto.KoAddress;
import com.ric.dto.MapKoAddress;
import com.ric.dto.MapMeter;
import com.ric.dto.SumMeterVolExt;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.javatuples.Pair;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.telegram.telegrambots.meta.api.objects.Update;
import org.telegram.telegrambots.meta.api.objects.replykeyboard.InlineKeyboardMarkup;
import org.telegram.telegrambots.meta.api.objects.replykeyboard.buttons.InlineKeyboardButton;

import javax.persistence.EntityManager;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.dic.app.telegram.bot.service.menu.Buttons.*;
import static com.dic.app.telegram.bot.service.menu.MeterValSaveState.RESTRICTED_BY_DAY_OF_MONTH;


@Slf4j
@Service
@RequiredArgsConstructor
@Transactional
public class UserInteractionImpl {

    public static final int PERIOD_BACK = -12; // охват отчета, начиная с N месяцев назад
    private final Env env;
    private final ObjParMng objParMng;
    private final MeterMng meterMng;
    private final ConfigApp config;
    private final RegistryMngImpl registryMng;
    private final EntityManager entityManager;
    private final OrgDAO orgDAO;

    private final Map<Integer, MeterValSaveState> statusCode =
            Map.of(0, MeterValSaveState.SUCCESSFUL,
                    3, MeterValSaveState.VAL_SAME_OR_LOWER,
                    4, MeterValSaveState.METER_NOT_FOUND,
                    5, MeterValSaveState.VAL_TOO_HIGH,
                    6, RESTRICTED_BY_DAY_OF_MONTH,
                    7, MeterValSaveState.VAL_OUT_OF_RANGE,
                    -1, MeterValSaveState.INTERNAL_ERROR
            );
    private final Pattern ptrnPeriod = Pattern.compile("period:(\\d+)");
    private final Pattern ptrnKlskId = Pattern.compile("klskId:(\\d+)");
    private final Pattern ptrnMeterId = Pattern.compile("meterId:(\\d+)");


    public TelegramMessage selectAddress(Update update, long userId, Map<Long, MapKoAddress> registeredKo, AtomicInteger cnt) {
        StringBuilder msg = new StringBuilder();
        MapKoAddress mapKoAddress = registeredKo.get(userId);
        //msg.append("_Выберите адрес,_").append(cnt.getAndIncrement());
        msg.append("_Выберите адрес,_");
        String city = orgDAO.getByOrgTp("Город").getName();
        msg.append("* г.").append(city).append(":*\r\n");

        InlineKeyboardMarkup inlineKeyboardMarkup = new InlineKeyboardMarkup();
        List<List<InlineKeyboardButton>> keyboards = new ArrayList<>();
        inlineKeyboardMarkup.setKeyboard(keyboards);

        MessageStore messageStore = new MessageStore(update);
        mapKoAddress.getMapKoAddress().values().stream().sorted(Comparator.comparing(KoAddress::getOrd))
                .forEach(t -> messageStore.addButtonCallBack(ADDRESS_KLSK.getCallBackData() + "_klskId:" + t.getKlskId()+"_queryId:"+cnt.getAndIncrement(), t.getAddress()));

        return messageStore.build(msg);
    }


    public TelegramMessage selectMeter(Update update, Long klskId, String callBackData, long userId) {
        StringBuilder msg = new StringBuilder();
        msg.append("*Адрес: ").append(env.getUserCurrentKo().get(userId).getAddress()).append("*\r\n");

        boolean isHead = false;
        for (SumMeterVolExt sumMeterVol : env.getMetersByKlskId().get(klskId).getMapKoMeter().values()) {
            // если у счетчика дата поверки меньше или равна дата начала периода + 2 мес
            LocalDate dtTo = LocalDate.ofInstant(config.getCurDt1().toInstant(), ZoneId.systemDefault()).plusMonths(2);
            if (LocalDate.ofInstant(sumMeterVol.getDtTo().toInstant(), ZoneId.systemDefault()).isBefore(dtTo)) {
                if (!isHead) {
                    isHead = true;
                    msg.append("\r\n");
                    msg.append("*Внимание\\! Наступает срок проведения поверки следующих счетчиков:*\r\n");
                }
                String serviceName = sumMeterVol.getServiceName();
                msg.append("№ ").append(sumMeterVol.getNpp()).append(". ").append(serviceName).append(" - ")
                        .append(Utl.getStrFromDate(sumMeterVol.getDtTo())).append("\r\n");
            }
        }
        if (isHead) msg.append("\r\n");

        msg.append("_Выберите:_\r\n");
        // настройки, для выбора счетчиков
        MessageStore messageStore = new MessageStore(update);
        if (klskId != null) {
            for (SumMeterVolExt sumMeterVol : env.getMetersByKlskId().get(klskId).getMapKoMeter().values().stream()
                    .sorted(Comparator.comparing(SumMeterVolExt::getServiceNpp).thenComparing(SumMeterVolExt::getNpp))
                    .collect(Collectors.toList())) {
                String serviceName = sumMeterVol.getServiceName();
                msg.append("№ ");
                msg.append(sumMeterVol.getNpp()).append(". ").append(serviceName);
                msg.append(", текущ: показания: ");
                msg.append("*").append(sumMeterVol.getN1().toString()).append("*");
                msg.append(", расход: ");
                msg.append("*").append(sumMeterVol.getVol().toString()).append("*");
                msg.append("\r\n");
                messageStore.addButton(METER.getCallBackData() + "_klskId:" + klskId + "_meterId:" + sumMeterVol.getMeterId(),
                        sumMeterVol.getNpp() + ". " + (serviceName != null ? serviceName : sumMeterVol.getUslId()));
            }
        } else {
            log.error("Не определен klskId");
        }

        messageStore.addButton(REPORTS.getCallBackData() + "_klskId:" + klskId, REPORTS.toString());
        messageStore.addButton(BACK.getCallBackData() + "_klskId:" + klskId, BACK.toString());
        return messageStore.build(msg);
    }


    public TelegramMessage selectReport(Update update, Long klskId) {
        MessageStore messageStore = new MessageStore(update);
        messageStore.addButton(BILLING_FLOW.getCallBackData() + "_klskId:" + klskId, BILLING_FLOW.toString());
        messageStore.addButton(BILLING_CHARGES.getCallBackData() + "_klskId:" + klskId, BILLING_CHARGES.toString());
        messageStore.addButton(BILLING_PAYMENTS.getCallBackData() + "_klskId:" + klskId, BILLING_PAYMENTS.toString());
        messageStore.addButton(BACK.getCallBackData() + "_klskId:" + klskId, BACK.toString());

        StringBuilder msg = new StringBuilder("      \r\nВыберите:");
        return messageStore.build(msg);
    }

    public TelegramMessage selectChargeReport(Update update, Long klskId) {
        MessageStore messageStore = new MessageStore(update);
        LocalDate curDt = LocalDate.ofInstant(config.getCurDt1().toInstant(), ZoneId.systemDefault());
        int i = 11;
        do {
            LocalDate dt = curDt.minusMonths(i);
            messageStore.addButton(
                    //BILLING_CHARGES_PERIOD.getCallBackData() + "_" + dt.getYear() + StringUtils.leftPad(String.valueOf(dt.getMonthValue()), 2, "0"),
                    //Utl.getMonthName(dt.getMonthValue(), 1) + " " + dt.getYear()
                    BILLING_CHARGES_PERIOD.getCallBackData() + "_klskId:" + klskId + "_period:" + dt.getYear() + StringUtils.leftPad(String.valueOf(dt.getMonthValue()), 2, "0"),
                    Utl.getMonthName(dt.getMonthValue(), 1) + " " + dt.getYear()
            );
        } while (--i >= 0);

        messageStore.addButton(BACK);

        StringBuilder msg = new StringBuilder("      \r\nВыберите период:");
        return messageStore.build(msg);
    }

    public TelegramMessage showFlow(Update update, long klskId, long userId) {
        MessageStore messageStore = new MessageStore(update);
        messageStore.addButton(BACK);

        String periodBack = config.getPeriodBackByMonth(PERIOD_BACK);
        String period = config.getPeriod();
        //Long klskId = getCurrentKlskId(userId);
        StringBuilder msg = registryMng.getFlowFormatted(klskId, periodBack, period);

        Ko ko = entityManager.find(Ko.class, klskId);
        msg.append("\r\nРасчет был произведен:").append(Utl.getStrFromDate(ko.getDtGenDebPen(), "dd.MM.yyyy HH:mm"));
        return messageStore.buildPhotoMessage(msg, "Движение по лицевому счету");
    }

    public TelegramMessage showCharge(Update update, Long klskId, String callBackData) {
        MessageStore messageStore = new MessageStore(update);
        messageStore.addButton(BACK);
        //String period = callBackData.substring(BILLING_CHARGES_PERIOD.getCallBackData().length() + 1);

        String period = getSubstr(callBackData, ptrnPeriod).orElseThrow(() ->
                new RuntimeException("Некорректен callback, не найден period:" + ObjectUtils.firstNonNull(callBackData, "null")));
        //Long klskId = getCurrentKlskId(userId);
        //Long klskId = Long.parseLong(getSubstr(callBackData, ptrnKlskId).orElseThrow(() ->
        //      new RuntimeException("Не корректен callback, не найден klskId:" + ObjectUtils.firstNonNull(callBackData, "null"))));

        StringBuilder msg = registryMng.getChargeFormatted(klskId, period);
        if (StringUtils.isEmpty(msg)) {
            msg = new StringBuilder("Повторите запрос позже");
        }
        return messageStore.buildPhotoMessage(msg, "Начисление");
    }

    public TelegramMessage showPayment(Update update, long klskId) {
        MessageStore messageStore = new MessageStore(update);
        messageStore.addButton(BACK);

        String periodFrom = config.getPeriodBackByMonth(PERIOD_BACK);
        String periodTo = config.getPeriod();
        //Long klskId = getCurrentKlskId(userId);
        StringBuilder msg = registryMng.getPaymentFormatted(klskId, periodFrom, periodTo);

        return messageStore.buildPhotoMessage(msg, "Оплата");
    }


/*
    private Long getCurrentKlskId(long userId) {
        return env.getUserCurrentKo().get(userId) != null ? env.getUserCurrentKo().get(userId).getKlskId() : null;
    }
*/

    public void updateMapMeterByKlskId(long userId, long klskId) {
        MapKoAddress registeredKoByUser = env.getUserRegisteredKo().get(userId);
        if (registeredKoByUser != null) {
            KoAddress currentKo = registeredKoByUser.getMapKoAddress().get(klskId);
            if (currentKo != null) {
                env.getUserCurrentKo().put(userId, currentKo);
                env.getMetersByKlskId().put(klskId, meterMng.getMapMeterByKlskId(klskId, config.getCurDt1(), config.getCurDt2()));
            }
        }
    }


    public TelegramMessage inputVol(Update update, String callBackData, long userId) {
        // присвоить счетчик
        Integer meterId;
        //if (callBackData != null) {
        //meterId = Integer.parseInt((callBackData.substring(METER.getCallBackData().length() + 1)));
        meterId = Integer.parseInt(getSubstr(callBackData, ptrnMeterId).orElseThrow(() ->
                new RuntimeException("Некорректен callback, не найден meterId:" +
                        ObjectUtils.firstNonNull(callBackData, "null"))));
        //} else {
//            meterId = env.getUserCurrentMeter().get(userId).getMeterId();
        //      }

        StringBuilder msg = new StringBuilder();
        //if (meterId != null) {
        Long currKlskId = env.getUserCurrentKo().get(userId).getKlskId();
        MapMeter mapMeter = env.getMetersByKlskId().get(currKlskId);
        SumMeterVolExt meter = mapMeter.getMapKoMeter().get(meterId);
        env.getUserCurrentMeter().put(userId, meter);
        Map<Integer, SumMeterVolExt> mapKoMeter = env.getMetersByKlskId().get(currKlskId)
                .getMapKoMeter();
        SumMeterVolExt sumMeterVolExt = mapKoMeter.get(meterId);
        env.getMeterVolExtByMeterId().put(meterId, sumMeterVolExt);
        msg.append("*Передача показаний возможна с 5 по 27 число.*\r\n");
        msg.append("\r\n");
        msg.append("Введите новое показание счетчика, и после ввода, дождитесь сообщения *ПОКАЗАНИЯ ПРИНЯТЫ\\!* ");
        msg.append(sumMeterVolExt.getNpp()).append(". ").append(sumMeterVolExt.getServiceName());
        msg.append(", текущие показания: ");
        msg.append(sumMeterVolExt.getN1().toString());
        msg.append(", расход: ");
        msg.append(sumMeterVolExt.getVol().toString());

        //} else {
//            log.error("Не определен meterId");
//        }
        MessageStore messageStore = new MessageStore(update);
        messageStore.addButton(BACK);
        return messageStore.build(msg);
    }

    public TelegramMessage inputVolAccept(Update update, long userId) {

        Pair<MeterValSaveState, Double> result = saveMeterValByMeterId(env
                        .getUserCurrentMeter().get(userId).getMeterId(),
                update.getMessage().getText());
        MeterValSaveState status = result.getValue0();
        SumMeterVolExt sumMeterVolExt = env.getUserCurrentMeter().get(userId);
        StringBuilder msg = new StringBuilder();
        if (status.equals(MeterValSaveState.SUCCESSFUL)) {
            msg.append("*ПОКАЗАНИЯ ПО СЧЕТЧИКУ* ")
                    .append(sumMeterVolExt.getNpp())
                    .append(". ")
                    .append(sumMeterVolExt.getServiceName())
                    .append(": ").append(result.getValue1()
                    ).append(": ").append(" *ПРИНЯТЫ\\!*");
        } else if (status.equals(MeterValSaveState.WRONG_FORMAT)) {
            log.error("*ВНИМАНИЕ\\!* Некорректное показание по счетчику, фин.лиц klskId={}, {}",
                    env.getUserCurrentKo().get(userId).getKlskId(),
                    update.getMessage().getText());
            msg.append("*ВНИМАНИЕ\\!* Некорректное показание по счетчику\\!");
        } else if (status.equals(MeterValSaveState.VAL_SAME_OR_LOWER)) {
            msg.append("*ВНИМАНИЕ\\!* Показания те же или меньше текущих\\!");
        } else if (status.equals(MeterValSaveState.VAL_TOO_HIGH)) {
            msg.append("*ВНИМАНИЕ\\!* Показания слишком большие\\!");
        } else if (status.equals(RESTRICTED_BY_DAY_OF_MONTH)) {
            msg.append(RESTRICTED_BY_DAY_OF_MONTH);
        } else if (status.equals(MeterValSaveState.VAL_OUT_OF_RANGE) || status.equals(MeterValSaveState.VAL_TOO_BIG_OR_LOW)) {
            msg.append("*ВНИМАНИЕ\\!* Показания вне допустимого диапазона\\!");
        } else {
            log.error("Ошибка передачи показаний по счетчику, фин.лиц klskId={}, {}",
                    env.getUserCurrentKo().get(userId).getKlskId(),
                    status);
            msg.append("*ВНИМАНИЕ\\!* Попробуйте передать показания позже");
        }
        MessageStore messageStore = new MessageStore(update);
        messageStore.addButton(BACK);
        return messageStore.build(msg);
    }

    /**
     * @param userId Id пользователя в Telegram
     * @return код для процедуры сопоставления Id пользователя с Директ klskId
     */

    @Transactional(propagation = Propagation.REQUIRED, readOnly = true, rollbackFor = Exception.class)
    public long authenticateUser(long userId) {
        MapKoAddress mapKoAddress = objParMng.getMapKoAddressByObjPar("TelegramId", userId);
        if (mapKoAddress.getMapKoAddress().size() == 0) {
            // Id, для регистрации
            return userId;
        } else {
            env.getUserRegisteredKo().put(userId, mapKoAddress);
            return 0;
        }
    }


    public Pair<MeterValSaveState, Double> saveMeterValByMeterId(int meterId, String strVal) {
        try {
            double val = Double.parseDouble(strVal.replace(",", "."));
            BigDecimal rounded = new BigDecimal(val).setScale(5, RoundingMode.HALF_UP);
            // все проверки по допустимому объему счетчика, осуществляются в хранимой процедуре p_meter.ins_data_meter
            Integer ret = meterMng.saveMeterValByMeterId(meterId, rounded.doubleValue());
            return new Pair<>(statusCode.get(ret), rounded.doubleValue());
        } catch (NumberFormatException e) {
            return new Pair<>(MeterValSaveState.WRONG_FORMAT, null);
        } catch (Exception e) {
            log.error("Ошибка сохранения показаний {}, по счетчику meterId={}", strVal, meterId, e);
            return new Pair<>(MeterValSaveState.WRONG_FORMAT, null);
        }
    }

    private Optional<String> getSubstr(String str, Pattern pattern) {
        Matcher matcher = pattern.matcher(str);
        if (matcher.find()) {
            return Optional.of(matcher.group(1));
        }
        return Optional.empty();
    }

    public Optional<Long> getKlskIdFromCallback(String callBackStr) {
        return getSubstr(callBackStr, ptrnKlskId).map(Long::parseLong);
    }
}
