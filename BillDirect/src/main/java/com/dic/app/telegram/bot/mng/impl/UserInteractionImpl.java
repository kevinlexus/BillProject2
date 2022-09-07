package com.dic.app.telegram.bot.mng.impl;

import com.dic.app.config.Config;
import com.dic.app.mm.ConfigApp;
import com.dic.app.telegram.bot.message.SimpleMessage;
import com.dic.app.telegram.bot.message.TelegramMessage;
import com.dic.app.telegram.bot.message.UpdateMessage;
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
import org.telegram.telegrambots.meta.api.methods.ParseMode;
import org.telegram.telegrambots.meta.api.methods.send.SendMessage;
import org.telegram.telegrambots.meta.api.methods.updatingmessages.EditMessageText;
import org.telegram.telegrambots.meta.api.objects.Update;
import org.telegram.telegrambots.meta.api.objects.replykeyboard.InlineKeyboardMarkup;
import org.telegram.telegrambots.meta.api.objects.replykeyboard.buttons.InlineKeyboardButton;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

import static com.dic.app.telegram.bot.mng.impl.Buttons.*;


@Slf4j
@Service
@RequiredArgsConstructor
public class UserInteractionImpl implements UserInteraction {

    private final Env env = new Env();
    private final ObjParMng objParMng;
    private final MeterMng meterMng;
    private final ConfigApp config;
    private final Map<Integer, MeterValSaveState> statusCode =
            Map.of(0, MeterValSaveState.SUCCESSFUL,
                    3, MeterValSaveState.VAL_SAME_OR_LOWER,
                    4, MeterValSaveState.METER_NOT_FOUND,
                    5, MeterValSaveState.VAL_TOO_BIG,
                    6, MeterValSaveState.RESTRICTED_BY_DAY_OF_MONTH
            );

    //@Value("${bot.billHost}")
    //private String billHost;

    @Override
    public TelegramMessage selectAddress(Update update, long userId, Map<Long, MapKoAddress> registeredKo) {
        StringBuilder msg = new StringBuilder();
        MapKoAddress mapKoAddress = registeredKo.get(userId);
        msg.append("_Выберите адрес:_\r\n");
        msg.append(String.join("\r\n",
                mapKoAddress.getMapKoAddress().values().stream().map(t -> t.getOrd() + "\\. " + t.getAddress())
                        .collect(Collectors.toSet())));

        InlineKeyboardMarkup inlineKeyboardMarkup = new InlineKeyboardMarkup();
        List<List<InlineKeyboardButton>> keyboards = new ArrayList<>();
        inlineKeyboardMarkup.setKeyboard(keyboards);
        List<InlineKeyboardButton> buttons = new ArrayList<>();

        for (KoAddress koAddress : mapKoAddress.getMapKoAddress().values()) {
            addButton(ADDRESS_KLSK.getCallBackData() + "_" + koAddress.getKlskId(), String.valueOf(koAddress.getOrd()), buttons);
        }
        addRowList(inlineKeyboardMarkup, buttons);

        return createMessage(update, msg, inlineKeyboardMarkup);
    }

    @Override
    public TelegramMessage selectMeter(Update update, long userId) {
        StringBuilder msg = new StringBuilder();
        Long klskId = env.getUserCurrentKo().get(userId) != null ? env.getUserCurrentKo().get(userId).getKlskId() : null;

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
        InlineKeyboardMarkup inlineKeyboardMarkup = new InlineKeyboardMarkup();
        List<InlineKeyboardButton> buttons = new ArrayList<>();

        if (klskId != null) {
            int i = 1;
            for (SumMeterVolExt sumMeterVol : env.getMetersByKlskId().get(klskId).getMapKoMeter().values()) {
                msg.append(i);
                msg.append("\\. ");
                String serviceName = sumMeterVol.getServiceName();
                msg.append(serviceName.replace(".", "\\."));
                msg.append(", текущ\\.: показания:");
                msg.append("*" + sumMeterVol.getN1().toString().replace(".", "\\.") + "*");
                msg.append(", расход:");
                msg.append("*" + sumMeterVol.getVol().toString().replace(".", "\\.") + "*");
                msg.append("\r\n");
                addButton(METER.getCallBackData() + "_" + sumMeterVol.getMeterId(), ++i + "." + serviceName, buttons);
            }
        } else {
            log.error("Не определен klskId");
        }

        addButton(Buttons.BILLING, buttons);
        addButton(Buttons.METER_BACK, buttons);
        addRowList(inlineKeyboardMarkup, buttons);

        return createMessage(update, msg, inlineKeyboardMarkup);
    }

    @Override
    public TelegramMessage wrongInput(Update update, long userId) {
        List<InlineKeyboardButton> buttons = new ArrayList<>();
        InlineKeyboardMarkup inlineKeyboardMarkup = new InlineKeyboardMarkup();
        addRowList(inlineKeyboardMarkup, buttons);
        StringBuilder msg = new StringBuilder();
        msg.append("Некорректный выбор\\!");

        return createMessage(update, msg, inlineKeyboardMarkup);
    }

    @Override
    public TelegramMessage showBilling(Update update, long userId) {
        List<InlineKeyboardButton> buttons = new ArrayList<>();
        InlineKeyboardMarkup inlineKeyboardMarkup = new InlineKeyboardMarkup();
        addRowList(inlineKeyboardMarkup, buttons);
        addButton(BILLING_CHARGES, buttons);
        addButton(BILLING_PAYMENTS, buttons);
        addButton(BILLING_BACK, buttons);

        StringBuilder msg = new StringBuilder();
        msg.append("Движение по лицевому счету\r\n");
        String preFormatted = "```\r\n" +
                "| Период | Долг    | Пени  | Начисление| Оплата |\r\n" +
                "| 10.2021| 2235.55 | 102.23| 150.22    | 250.85 |\r\n" +
                "| 11.2021| 2235.55 | 102.23| 150.22    | 250.85 |\r\n" +
                "| 12.2021| 2235.55 | 102.23| 150.22    | 250.85 |\r\n" +
                "| 13.2021| 2235.55 | 102.23| 150.22    | 250.85 |\r\n" + "```";
        msg.append(preFormatted.replace(".", "\\.").replace("|","\\|"));
        msg.append("_Переверните экран смартфона, для лучшего чтения информации_");
        return createMessage(update, msg, inlineKeyboardMarkup);
    }


    private TelegramMessage createMessage(Update update, StringBuilder msg, InlineKeyboardMarkup inlineKeyboardMarkup) {
        if (update.getMessage() == null) {
            EditMessageText em = new EditMessageText();
            em.setText(msg.toString());
            em.setChatId(update.getCallbackQuery().getMessage().getChatId().toString());
            em.setMessageId(update.getCallbackQuery().getMessage().getMessageId());
            em.setReplyMarkup(inlineKeyboardMarkup);

            em.setParseMode(ParseMode.MARKDOWNV2);
            return new UpdateMessage(em);
        } else {
            SendMessage sm = new SendMessage();
            sm.setText(msg.toString());

            sm.setParseMode(ParseMode.MARKDOWNV2);

            sm.setChatId(update.getMessage().getChatId());
            sm.setReplyMarkup(inlineKeyboardMarkup);
            return new SimpleMessage(sm);
        }
    }

    /**
     * Обновить показания счетчиков по klskId
     *
     * @param userId пользователь
     * @param klskId klskId фин.лиц.
     */
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
        EditMessageText em = new EditMessageText();
        if (update.getCallbackQuery() != null) {
            String callBackStr = update.getCallbackQuery().getData();
            if (callBackStr != null) {
                meterId = Integer.parseInt((callBackStr.substring(14)));
            }
        } else {
            meterId = env.getUserCurrentMeter().get(userId).getMeterId();
        }

        // настройки для ввода показаний
        InlineKeyboardMarkup inlineKeyboardMarkup = new InlineKeyboardMarkup();
        //List<InlineKeyboardButton> buttons = new ArrayList<>();
        String msgKeyb = "Назад";
        if (update.getMessage() == null) {
            em.setChatId(update.getCallbackQuery().getMessage().getChatId().toString());
            em.setMessageId(update.getCallbackQuery().getMessage().getMessageId());
        } else {
            em.setChatId(update.getMessage().getChatId());
        }
        if (meterId != null) {
            Long currKlskId = env.getUserCurrentKo().get(userId).getKlskId();
            MapMeter mapMeter = env.getMetersByKlskId().get(currKlskId);
            SumMeterVolExt meter = mapMeter.getMapKoMeter().get(meterId);
            env.getUserCurrentMeter().put(userId, meter);
            Map<Integer, SumMeterVolExt> mapKoMeter = env.getMetersByKlskId().get(currKlskId)
                    .getMapKoMeter();
            SumMeterVolExt sumMeterVolExt = mapKoMeter.get(meterId);
            env.getMeterVolExtByMeterId().put(meterId, sumMeterVolExt);
            String msg = "Введите новое показание счетчика по услуге: " + sumMeterVolExt.getServiceName() + ", текущие показания="
                    + sumMeterVolExt.getN1() + ", расход=" + sumMeterVolExt.getVol();

            em.setText(msg);
        } else {
            log.error("Не определен meterId");
        }
        List<InlineKeyboardButton> buttons = new ArrayList<>();
        addButton(INPUT_BACK.getCallBackData(), msgKeyb, buttons);
        addRowList(inlineKeyboardMarkup, buttons);
        em.setReplyMarkup(inlineKeyboardMarkup);

        return new UpdateMessage(em);
    }


    /**
     * @param userId Id пользователя в Telegram
     * @return код для процедуры сопоставления Id пользователя с Директ klskId
     */
    @Override
    public int authenticateUser(long userId) {
        MapKoAddress mapKoAddress = objParMng.getMapKoAddressByObjPar("TelegramId", userId);;
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

    private void addButton(String callBackData, String caption, List<InlineKeyboardButton> buttons) {
        InlineKeyboardButton inlineKeyboardButton = new InlineKeyboardButton();
        inlineKeyboardButton.setCallbackData(callBackData);
        inlineKeyboardButton.setText(caption);
        buttons.add(inlineKeyboardButton);
    }

    private void addButton(Buttons button, List<InlineKeyboardButton> buttons) {
        InlineKeyboardButton inlineKeyboardButton = new InlineKeyboardButton();
        inlineKeyboardButton.setCallbackData(button.getCallBackData());
        inlineKeyboardButton.setText(button.toString());
        buttons.add(inlineKeyboardButton);
    }

    private void addRowList(InlineKeyboardMarkup inlineKeyboardMarkup, List<InlineKeyboardButton> buttons) {
        List<List<InlineKeyboardButton>> rowList = new ArrayList<>();
        rowList.add(buttons);
        inlineKeyboardMarkup.setKeyboard(rowList);
    }


}
