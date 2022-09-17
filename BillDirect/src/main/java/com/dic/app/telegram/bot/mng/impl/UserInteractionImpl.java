package com.dic.app.telegram.bot.mng.impl;

import com.dic.app.mm.ConfigApp;
import com.dic.app.mm.RegistryMng;
import com.dic.app.telegram.bot.message.TelegramMessage;
import com.dic.app.telegram.bot.mng.UserInteraction;
import com.dic.bill.mm.MeterMng;
import com.dic.bill.mm.ObjParMng;
import com.ric.dto.KoAddress;
import com.ric.dto.MapKoAddress;
import com.ric.dto.MapMeter;
import com.ric.dto.SumMeterVolExt;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.telegram.telegrambots.meta.api.objects.Update;
import org.telegram.telegrambots.meta.api.objects.replykeyboard.InlineKeyboardMarkup;
import org.telegram.telegrambots.meta.api.objects.replykeyboard.buttons.InlineKeyboardButton;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import static com.dic.app.telegram.bot.mng.impl.Buttons.*;


@Slf4j
@Service
@RequiredArgsConstructor
public class UserInteractionImpl implements UserInteraction {

    private final Env env = new Env();
    private final ObjParMng objParMng;
    private final MeterMng meterMng;
    private final ConfigApp config;
    private final RegistryMng registryMng;
    private final Map<Integer, MeterValSaveState> statusCode =
            Map.of(0, MeterValSaveState.SUCCESSFUL,
                    3, MeterValSaveState.VAL_SAME_OR_LOWER,
                    4, MeterValSaveState.METER_NOT_FOUND,
                    5, MeterValSaveState.VAL_TOO_BIG,
                    6, MeterValSaveState.RESTRICTED_BY_DAY_OF_MONTH
            );


    @Override
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

    @Override
    public TelegramMessage selectMeter(Update update, long userId) {
        StringBuilder msg = new StringBuilder();
        Long klskId = getCurrentKlskId(userId);

        if (update.getCallbackQuery() != null) {
            // присвоить адрес, если не установлен
            String callBackStr = update.getCallbackQuery().getData();
            if (callBackStr != null && callBackStr.startsWith(ADDRESS_KLSK.getCallBackData())) {
                klskId = Long.parseLong(callBackStr.substring(ADDRESS_KLSK.getCallBackData().length() + 1));
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
                msg.append(serviceName.replace(".", "\\."));
                msg.append(", текущ\\.: показания:");
                msg.append("*" + sumMeterVol.getN1().toString().replace(".", "\\.") + "*");
                msg.append(", расход:");
                msg.append("*" + sumMeterVol.getVol().toString().replace(".", "\\.") + "*");
                msg.append("\r\n");
                messageStore.addButton(METER.getCallBackData() + "_" + sumMeterVol.getMeterId(), serviceName);
            }
        } else {
            log.error("Не определен klskId");
        }

        messageStore.addButton(Buttons.BILLING);
        messageStore.addButton(Buttons.METER_BACK);
        return messageStore.build(msg);
    }

    @Override
    public TelegramMessage showBilling(Update update, long userId) {
        MessageStore messageStore = new MessageStore(update);
        messageStore.addButton(Buttons.BILLING_CHARGES);
        messageStore.addButton(Buttons.BILLING_PAYMENTS);
        messageStore.addButton(Buttons.BILLING_BACK);

        String periodBack = config.getPeriodBackByMonth(12);
        Long klskId = getCurrentKlskId(userId);
        klskId = 104880L; // todo
        periodBack = "201309"; // todo
        StringBuilder msg = registryMng.getFlowFormatted(klskId, periodBack);

        msg.append("_При необходимости, поверните экран смартфона, для лучшего чтения информации_");
        return messageStore.build(msg);
    }
    @Override
    public TelegramMessage showCharge(Update update, long userId) {
        MessageStore messageStore = new MessageStore(update);
        messageStore.addButton(Buttons.BILLING_PAYMENTS);
        messageStore.addButton(Buttons.BILLING_BACK);

        String periodBack = config.getPeriodBackByMonth(12);
        Long klskId = getCurrentKlskId(userId);
        klskId = 104880L; // todo
        StringBuilder msg = registryMng.getChargeFormatted(klskId);

        msg.append("_При необходимости, поверните экран смартфона, для лучшего чтения информации_");
        return messageStore.build(msg);
    }


    private Long getCurrentKlskId(long userId) {
        return env.getUserCurrentKo().get(userId) != null ? env.getUserCurrentKo().get(userId).getKlskId() : null;
    }

    @Override
    public TelegramMessage wrongInput(Update update, long userId) {
        MessageStore messageStore = new MessageStore(update);
        messageStore.addButton(Buttons.METER_BACK);
        StringBuilder msg = new StringBuilder();
        msg.append("Некорректный выбор\\!");
        return messageStore.build(msg);
    }

    @Override
    public void updateMapMeterByCurrentKlskId(long userId, long klskId) {
        MapKoAddress registeredKoByUser = env.getUserRegisteredKo().get(userId);
        env.getUserCurrentKo().put(userId, registeredKoByUser.getMapKoAddress().get(klskId));
        env.getMetersByKlskId().put(klskId, meterMng.getMapMeterByKlskId(klskId, config.getCurDt1(), config.getCurDt2()));
    }

    @Override
    public TelegramMessage inputVol(Update update, long userId) {
        // присвоить счетчик
        Integer meterId = null;
        if (update.getCallbackQuery() != null) {
            String callBackStr = update.getCallbackQuery().getData();
            if (callBackStr != null) {
                meterId = Integer.parseInt((callBackStr.substring(14)));
            }
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
            msg.append(sumMeterVolExt.getServiceName().replace(".", "\\."));
            msg.append(", текущие показания:");
            msg.append(sumMeterVolExt.getN1().toString().replace(".", "\\."));
            msg.append(", расход:");
            msg.append(sumMeterVolExt.getVol().toString().replace(".", "\\."));

        } else {
            log.error("Не определен meterId");
        }
        MessageStore messageStore = new MessageStore(update);
        messageStore.addButton(INPUT_BACK);
        return messageStore.build(msg);
    }


    /**
     * @param userId Id пользователя в Telegram
     * @return код для процедуры сопоставления Id пользователя с Директ klskId
     */
    @Override
    public int authenticateUser(long userId) {
        MapKoAddress mapKoAddress = objParMng.getMapKoAddressByObjPar("TelegramId", userId);
        if (mapKoAddress.getMapKoAddress().size() == 0) {
            // временный код, для регистрации
            return env.getUserTemporalCode().computeIfAbsent(userId, t -> {
                        int randCode = -1;
                        while (randCode == -1 || !env.getIssuedCodes().add(randCode)) {
                            randCode = ThreadLocalRandom.current().nextInt(1000, 10000);
                        }
                        return randCode;
                    }
            );
        } else {
            env.getUserRegisteredKo().put(userId, mapKoAddress);
            return 0;
        }
    }

    @Override
    public MeterValSaveState saveMeterValByMeterId(int meterId, String strVal) {
        try {
            double val = Double.parseDouble(strVal.replace(",", "."));
            if (val > 9999999 || val < -9999999) {
                return MeterValSaveState.VAL_TOO_BIG_OR_LOW;
            }
            Integer ret = meterMng.saveMeterValByMeterId(meterId, val);
            return statusCode.get(ret);
        } catch (NumberFormatException e) {
            return MeterValSaveState.WRONG_FORMAT;
        } catch (Exception e) {
            e.printStackTrace();
            return MeterValSaveState.ERROR_WHILE_SENDING;
        }
    }

    @Override
    public Env getEnv() {
        return env;
    }

}
