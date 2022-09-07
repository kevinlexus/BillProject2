package com.dic.app.telegram.bot.mng.impl;

import com.dic.app.telegram.bot.message.PlainMessage;
import com.dic.app.telegram.bot.message.SimpleMessage;
import com.dic.app.telegram.bot.message.TelegramMessage;
import com.dic.app.telegram.bot.message.UpdateMessage;
import com.dic.app.telegram.bot.mng.UserInteraction;
import com.ric.dto.SumMeterVolExt;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.telegram.telegrambots.bots.TelegramLongPollingBot;
import org.telegram.telegrambots.meta.api.methods.send.SendMessage;
import org.telegram.telegrambots.meta.api.methods.updatingmessages.EditMessageText;
import org.telegram.telegrambots.meta.api.objects.Message;
import org.telegram.telegrambots.meta.api.objects.Update;
import org.telegram.telegrambots.meta.api.objects.User;
import org.telegram.telegrambots.meta.exceptions.TelegramApiException;

import java.util.Map;

import static com.dic.app.telegram.bot.mng.impl.Buttons.*;

@Service
@Slf4j
@RequiredArgsConstructor
public class Bot extends TelegramLongPollingBot {
    @Value("${bot.name}")
    private String botUsername;

    @Value("${bot.token}")
    private String botToken;

    private final UserInteraction ui;


    @Override
    public void onUpdateReceived(Update update) {
        try {
            Message message = update.getMessage();
            User user;
            long userId;
            if (update.hasCallbackQuery()) {
                user = update.getCallbackQuery().getFrom();
            } else {
                user = message.getFrom();
            }
            userId = user.getId();
            Map<Long, Menu> menuPosition = ui.getEnv().getMenuPosition();
            Menu curMenu = menuPosition.get(userId);
            if (curMenu == null) {
                curMenu = Menu.UNDEFINED;
            }
            if (!ui.getEnv().getUserRegisteredKo().containsKey(userId)) {
                // аутентификация пользователя в системе
                int code = ui.authenticateUser(userId);
                if (code != 0) {
                    executeSendMessage(update, "Сообщите код оператору, для регистрации = " + code);
                    menuPosition.put(userId, Menu.MAIN);
                } else {
                    menuPosition.put(userId, Menu.SELECT_ADDRESS);
                }
            } else {
                if (update.hasCallbackQuery()) {
                    // нажата кнопка (выбрано меню)
                    String callBackStr = update.getCallbackQuery().getData();
                    if (callBackStr.startsWith(ADDRESS_KLSK.getCallBackData()+"_")) {
                        menuPosition.put(userId, Menu.SELECT_METER);
                    } else if (callBackStr.startsWith(METER.getCallBackData()+"_")) {
                        menuPosition.put(userId, Menu.INPUT_VOL);
                    } else if (callBackStr.equals(Buttons.METER_BACK.getCallBackData())) {
                        menuPosition.put(userId, Menu.SELECT_ADDRESS);
                    } else if (callBackStr.startsWith(INPUT_BACK.getCallBackData())) {
                        menuPosition.put(userId, Menu.SELECT_METER);
                    } else if (callBackStr.equals(Buttons.BILLING.getCallBackData())) {
                        menuPosition.put(userId, Menu.SELECT_BILLING);
                    } else if (callBackStr.equals(BILLING_BACK.getCallBackData())) {
                        menuPosition.put(userId, Menu.SELECT_METER);
                    } else if (callBackStr.equals(BILLING_CHARGES.getCallBackData())) {
                        menuPosition.put(userId, Menu.SELECT_BILLING); // todo
                    } else if (callBackStr.equals(BILLING_PAYMENTS.getCallBackData())) {
                        menuPosition.put(userId, Menu.SELECT_BILLING); // todo
                    }
                } else if (curMenu.equals(Menu.INPUT_VOL)) {
                    // введены показания
                    MeterValSaveState status = ui.saveMeterValByMeterId(ui.getEnv()
                                    .getUserCurrentMeter().get(userId).getMeterId(),
                            message.getText());
                    ui.updateMapMeterByCurrentKlskId(userId, ui.getEnv().getUserCurrentKo().get(userId).getKlskId());
                    SumMeterVolExt sumMeterVolExt = ui.getEnv().getUserCurrentMeter().get(userId);
                    if (status.equals(MeterValSaveState.SUCCESSFUL)) {
                        executeSendMessage(update, "Показания по услуге " + sumMeterVolExt.getServiceName() + ": "
                                + message.getText() + " приняты");
                    } else {
                        log.error("Ошибка передачи показаний по счетчику, фин.лиц klskId={}, {}",
                                ui.getEnv().getUserCurrentKo().get(userId).getKlskId(),
                                status);
                        executeSendMessage(update, status.toString());
                    }
                    menuPosition.put(userId, Menu.SELECT_METER);
                } else {
                    menuPosition.put(userId, Menu.INPUT_WRONG);
                }
            }

            TelegramMessage tm = null;
            switch (menuPosition.get(userId)) {
                case SELECT_ADDRESS -> tm = ui.selectAddress(update, userId, ui.getEnv().getUserRegisteredKo());
                case SELECT_METER -> tm = ui.selectMeter(update, userId);
                case INPUT_VOL -> tm = ui.inputVol(update, userId);
                case INPUT_WRONG -> tm = ui.wrongInput(update, userId);
                case SELECT_BILLING -> tm = ui.showBilling(update, userId);
            }

            if (tm instanceof PlainMessage) {
                executeSendMessage(update, ((PlainMessage) tm).getMsg());
            } else if (tm instanceof SimpleMessage) {
                sendSimpleMessage(tm);
            } else if (tm instanceof UpdateMessage) {
                updateMessage(tm);
            }
        } catch (
                TelegramApiException e) {
            e.printStackTrace();
        }

    }

    private void executeSendMessage(Update update, String msg) throws TelegramApiException {
        SendMessage sm = new SendMessage();
        sm.setText(msg);
        if (update.getMessage() == null) {
            sm.setChatId(update.getCallbackQuery().getMessage().getChatId().toString());
        } else {
            sm.setChatId(update.getMessage().getChatId().toString());
        }
        execute(sm);
    }


    private void sendSimpleMessage(TelegramMessage tm) throws TelegramApiException {
        SendMessage sm = ((SimpleMessage) tm).getSm();
        execute(sm);
    }

    private void updateMessage(TelegramMessage tm) throws TelegramApiException {
        EditMessageText em = ((UpdateMessage) tm).getEm();
        execute(em);
    }


    public String getBotUsername() {
        return botUsername;
    }

    public String getBotToken() {
        return botToken;
    }
}