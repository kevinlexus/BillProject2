package com.dic.app.telegram.bot.service;

import com.dic.app.service.ConfigApp;
import com.dic.app.service.registry.RegistryMngImpl;
import com.dic.app.telegram.bot.message.TelegramMessage;
import com.dic.app.telegram.bot.service.client.Env;
import com.dic.app.telegram.bot.service.menu.MeterValSaveState;
import com.dic.app.telegram.bot.service.message.MessageStore;
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
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import static com.dic.app.telegram.bot.service.menu.Buttons.*;


@Slf4j
@Service
@RequiredArgsConstructor
@Transactional
public class UserInteractionImpl {

    private final Env env;
    private final ObjParMng objParMng;
    private final MeterMng meterMng;
    private final ConfigApp config;
    private final RegistryMngImpl registryMng;
    private final EntityManager entityManager;
    private final Map<Integer, MeterValSaveState> statusCode =
            Map.of(0, MeterValSaveState.SUCCESSFUL,
                    3, MeterValSaveState.VAL_SAME_OR_LOWER,
                    4, MeterValSaveState.METER_NOT_FOUND,
                    5, MeterValSaveState.VAL_TOO_BIG,
                    6, MeterValSaveState.RESTRICTED_BY_DAY_OF_MONTH
            );


    public TelegramMessage selectAddress(Update update, long userId, Map<Long, MapKoAddress> registeredKo) {
        StringBuilder msg = new StringBuilder();
        MapKoAddress mapKoAddress = registeredKo.get(userId);
        msg.append("_Выберите адрес:_\r\n");

        InlineKeyboardMarkup inlineKeyboardMarkup = new InlineKeyboardMarkup();
        List<List<InlineKeyboardButton>> keyboards = new ArrayList<>();
        inlineKeyboardMarkup.setKeyboard(keyboards);

        MessageStore messageStore = new MessageStore(update);
        mapKoAddress.getMapKoAddress().values().stream().sorted(Comparator.comparing(KoAddress::getOrd))
                .forEach(t -> messageStore.addButtonCallBack(ADDRESS_KLSK.getCallBackData() + "_" + t.getKlskId(), t.getAddress()));

        return messageStore.build(msg);
    }


    public TelegramMessage selectMeter(Update update, String callBackData, long userId) {
        StringBuilder msg = new StringBuilder();
        Long klskId = getCurrentKlskId(userId);

        if (callBackData != null) {
            // присвоить адрес, если не установлен
            if (callBackData.startsWith(ADDRESS_KLSK.getCallBackData())) {
                klskId = Long.parseLong(callBackData.substring(ADDRESS_KLSK.getCallBackData().length() + 1));
                updateMapMeterByCurrentKlskId(userId, klskId);
            }
        }
        msg.append("*Адрес: ").append(env.getUserCurrentKo().get(userId).getAddress()).append("*\r\n");
        msg.append("_Выберите:_\r\n");

        // настройки, для выбора счетчиков
        MessageStore messageStore = new MessageStore(update);
        if (klskId != null) {
            for (SumMeterVolExt sumMeterVol : env.getMetersByKlskId().get(klskId).getMapKoMeter().values()) {
                String serviceName = sumMeterVol.getServiceName();
                msg.append(serviceName);
                msg.append(", текущ: показания:");
                msg.append("*" + sumMeterVol.getN1().toString() + "*");
                msg.append(", расход:");
                msg.append("*" + sumMeterVol.getVol().toString() + "*");
                msg.append("\r\n");
                messageStore.addButton(METER.getCallBackData() + "_" + sumMeterVol.getMeterId(), serviceName);
            }
        } else {
            log.error("Не определен klskId");
        }

        messageStore.addButton(REPORTS);
        messageStore.addButton(BACK);
        return messageStore.build(msg);
    }


    public TelegramMessage selectReport(Update update, long userId) {
        MessageStore messageStore = new MessageStore(update);
        messageStore.addButton(BILLING_FLOW);
        messageStore.addButton(BILLING_CHARGES);
        messageStore.addButton(BILLING_PAYMENTS);
        messageStore.addButton(BACK);

        StringBuilder msg = new StringBuilder("Выберите");
        return messageStore.build(msg);
    }

    public TelegramMessage showFlow(Update update, long userId) {
        MessageStore messageStore = new MessageStore(update);
        messageStore.addButton(BACK);

        String periodBack = config.getPeriodBackByMonth(12);
        Long klskId = getCurrentKlskId(userId);
        StringBuilder msg = registryMng.getFlowFormatted(klskId, periodBack);

        Ko ko = entityManager.find(Ko.class, klskId);
        msg.append("_Расчет был произведен:").append(Utl.getStrFromDate(ko.getDtGenDebPen(), "dd.MM.yyyy HH:mm")).append("_\r\n");
        msg.append("_При необходимости, поверните экран смартфона, для лучшего чтения информации_");
        return messageStore.build(msg);
    }

    public TelegramMessage showCharge(Update update, long userId) {
        MessageStore messageStore = new MessageStore(update);
        messageStore.addButton(BACK);

        Long klskId = getCurrentKlskId(userId);
        StringBuilder msg = registryMng.getChargeFormatted(klskId);
        if (msg == null) {
            msg.append("_Повторите запрос позже_");
        }
        return messageStore.buildPhoto(msg, "Начисление.png");
    }

    public TelegramMessage showPayment(Update update, long userId) {
        MessageStore messageStore = new MessageStore(update);
        messageStore.addButton(BACK);

        String periodFrom = config.getPeriodBackByMonth(12);
        String periodTo = config.getPeriod();
        Long klskId = getCurrentKlskId(userId);
        StringBuilder msg = registryMng.getPaymentFormatted(klskId, periodFrom, periodTo);

        return messageStore.build(msg);
    }


    private Long getCurrentKlskId(long userId) {
        return env.getUserCurrentKo().get(userId) != null ? env.getUserCurrentKo().get(userId).getKlskId() : null;
    }

    public void updateMapMeterByCurrentKlskId(long userId, long klskId) {
        MapKoAddress registeredKoByUser = env.getUserRegisteredKo().get(userId);
        env.getUserCurrentKo().put(userId, registeredKoByUser.getMapKoAddress().get(klskId));
        env.getMetersByKlskId().put(klskId, meterMng.getMapMeterByKlskId(klskId, config.getCurDt1(), config.getCurDt2()));
    }


    public TelegramMessage inputVol(Update update, String callBackData, long userId) {
        // присвоить счетчик
        Integer meterId = null;
        if (callBackData != null) {
            meterId = Integer.parseInt((callBackData.substring(METER.getCallBackData().length() + 1)));
        } else {
            meterId = env.getUserCurrentMeter().get(userId).getMeterId();
        }

        StringBuilder msg = new StringBuilder();
        if (meterId != null) {
            Long currKlskId = env.getUserCurrentKo().get(userId).getKlskId();
            MapMeter mapMeter = env.getMetersByKlskId().get(currKlskId);
            SumMeterVolExt meter = mapMeter.getMapKoMeter().get(meterId);
            env.getUserCurrentMeter().put(userId, meter);
            Map<Integer, SumMeterVolExt> mapKoMeter = env.getMetersByKlskId().get(currKlskId)
                    .getMapKoMeter();
            SumMeterVolExt sumMeterVolExt = mapKoMeter.get(meterId);
            env.getMeterVolExtByMeterId().put(meterId, sumMeterVolExt);
            msg.append("Введите новое показание счетчика по услуге: ");
            msg.append(sumMeterVolExt.getServiceName());
            msg.append(", текущие показания:");
            msg.append(sumMeterVolExt.getN1().toString());
            msg.append(", расход:");
            msg.append(sumMeterVolExt.getVol().toString());

        } else {
            log.error("Не определен meterId");
        }
        MessageStore messageStore = new MessageStore(update);
        messageStore.addButton(BACK);
        return messageStore.build(msg);
    }

    public TelegramMessage inputVolAccept(Update update, long userId) {

        Pair<MeterValSaveState, Double> result = saveMeterValByMeterId(env
                        .getUserCurrentMeter().get(userId).getMeterId(),
                update.getMessage().getText());
        MeterValSaveState status = result.getValue0();
        updateMapMeterByCurrentKlskId(userId, env.getUserCurrentKo().get(userId).getKlskId());
        SumMeterVolExt sumMeterVolExt = env.getUserCurrentMeter().get(userId);
        StringBuilder msg = new StringBuilder();
        if (status.equals(MeterValSaveState.SUCCESSFUL)) {
            msg.append("Показания по услуге ")
                    .append(sumMeterVolExt.getServiceName())
                    .append(": ").append(result.getValue1()
                    ).append(": ").append(" приняты");
        } else if (status.equals(MeterValSaveState.WRONG_FORMAT)) {
            log.error("Некорректное показание по счетчику, фин.лиц klskId={}, {}",
                    env.getUserCurrentKo().get(userId).getKlskId(),
                    update.getMessage().getText());
            msg.append("Некорректное показание по счетчику\\!");
        } else if (status.equals(MeterValSaveState.VAL_SAME_OR_LOWER)) {
            msg.append("Показания те же или меньше текущих\\!");
        } else if (status.equals(MeterValSaveState.VAL_TOO_BIG)) {
            msg.append("Показания слишком большие\\!");
        } else if (status.equals(MeterValSaveState.VAL_TOO_BIG_OR_LOW)) {
            msg.append("Показания вне допустимого диапазона\\!");
        } else {
            log.error("Ошибка передачи показаний по счетчику, фин.лиц klskId={}, {}",
                    env.getUserCurrentKo().get(userId).getKlskId(),
                    status);
            msg.append("Попробуйте передать показания позже");
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
            e.printStackTrace();
            return new Pair<>(MeterValSaveState.WRONG_FORMAT, null);
        }
    }

}
