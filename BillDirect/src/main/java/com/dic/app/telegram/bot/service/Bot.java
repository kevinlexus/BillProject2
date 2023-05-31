package com.dic.app.telegram.bot.service;

import com.dic.app.telegram.bot.message.*;
import com.dic.app.telegram.bot.service.client.Env;
import com.dic.app.telegram.bot.service.menu.Menu;
import com.dic.app.telegram.bot.service.menu.MenuStep;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.telegram.telegrambots.bots.TelegramLongPollingBot;
import org.telegram.telegrambots.meta.api.methods.send.SendMessage;
import org.telegram.telegrambots.meta.api.methods.send.SendPhoto;
import org.telegram.telegrambots.meta.api.methods.updatingmessages.DeleteMessage;
import org.telegram.telegrambots.meta.api.methods.updatingmessages.EditMessageText;
import org.telegram.telegrambots.meta.api.objects.Message;
import org.telegram.telegrambots.meta.api.objects.Update;
import org.telegram.telegrambots.meta.api.objects.User;
import org.telegram.telegrambots.meta.exceptions.TelegramApiException;

import java.util.LinkedList;
import java.util.concurrent.atomic.AtomicInteger;

import static com.dic.app.telegram.bot.service.menu.Buttons.*;

@Service
@Slf4j
@RequiredArgsConstructor
public class Bot extends TelegramLongPollingBot {
    @Value("${bot.name}")
    private String botUsername;

    @Value("${bot.token}")
    private String botToken;

    private final UserInteractionImpl ui;
    private final Env env;
    private final AtomicInteger cnt = new AtomicInteger(0);


    @Override
    public void onUpdateReceived(Update update) {
        Message message = update.getMessage();
        User user;
        long userId;
        if (update.hasCallbackQuery()) {
            user = update.getCallbackQuery().getFrom();
        } else {
            user = message.getFrom();
        }
        userId = user.getId();

        LinkedList<MenuStep> menuPath = env.getUserMenuPath().get(userId);
        if (menuPath == null) {
            env.getUserMenuPath().put(userId, new LinkedList<>());
            menuPath = env.getUserMenuPath().get(userId);
        }

        String callBackStr;
        TelegramMessage tm = null;

        try {
            if (checkAuth(update, userId)) return;
            if (!env.getUserRegisteredKo().containsKey(userId)) {
                // аутентификация пользователя в системе
                menuPath.add(new MenuStep(Menu.ROOT, ""));
                tm = ui.selectAddress(update, userId, env.getUserRegisteredKo(), cnt);

            } else {
                if (update.hasCallbackQuery()) {
                    // нажата кнопка (выбрано меню)
                    callBackStr = update.getCallbackQuery().getData();
                    boolean isBack = false;
                    boolean wrongButton = false;
                    if (callBackStr.startsWith(BACK.getCallBackData())) {
                        isBack = true;
                        if (menuPath.size() > 0) {
                            menuPath.removeLast();
                            if (menuPath.size() > 0) {
                                callBackStr = menuPath.getLast().getCallBackData();
                            } else {
                                // что то пошло не так и выкинуть на главный экран
                                menuPath.add(new MenuStep(Menu.ROOT, ""));
                                tm = ui.selectAddress(update, userId, env.getUserRegisteredKo(), cnt);
                                wrongButton = true; // нажата кнопка из "старого" чата, что может приводить к ошибке
                            }
                        }
                    }

                    Long klskId = ui.getKlskIdFromCallback(callBackStr).orElse(null);
                    if (klskId == null || !env.checkAccessByKlskId(userId, klskId)) {
                        menuPath.add(new MenuStep(Menu.ROOT, ""));
                        tm = ui.selectAddress(update, userId, env.getUserRegisteredKo(), cnt);
                        wrongButton = true; // нажата кнопка из "старого" чата, что может приводить к ошибке
                    }

                    if (!wrongButton) {
                        if (callBackStr.startsWith(ADDRESS_KLSK.getCallBackData() + "_")) {
                            if (!isBack) {
                                menuPath.add(new MenuStep(Menu.SELECT_METER, null));
                                menuPath.getLast().setCallBackData(callBackStr);
                            }
                            tm = ui.selectMeter(update, klskId, callBackStr, userId);
                        } else if (callBackStr.startsWith(METER.getCallBackData() + "_")) {
                            if (!isBack) {
                                menuPath.add(new MenuStep(Menu.INPUT_VOL, null));
                                menuPath.getLast().setCallBackData(callBackStr);
                            }
                            tm = ui.inputVol(update, callBackStr, userId);
                        } else if (callBackStr.startsWith(REPORTS.getCallBackData())) {
                            if (!isBack) {
                                menuPath.add(new MenuStep(Menu.SELECT_REPORT, callBackStr));
                                menuPath.getLast().setCallBackData(callBackStr);
                            }
                            tm = ui.selectReport(update, klskId);
                        } else if (callBackStr.startsWith(BILLING_FLOW.getCallBackData())) {
                            if (!isBack) {
                                menuPath.add(new MenuStep(Menu.SELECT_FLOW, callBackStr));
                                menuPath.getLast().setCallBackData(callBackStr);
                            }
                            tm = ui.showFlow(update, klskId, userId);
                        } else if (callBackStr.startsWith(BILLING_CHARGES.getCallBackData())) {
                            if (!isBack) {
                                menuPath.add(new MenuStep(Menu.SELECT_CHARGE, callBackStr));
                                menuPath.getLast().setCallBackData(callBackStr);
                            }
                            tm = ui.selectChargeReport(update, klskId);
                            //tm = ui.showCharge(update, userId);
                        } else if (callBackStr.startsWith(BILLING_CHARGES_PERIOD.getCallBackData() + "_")) {
                            if (!isBack) {
                                menuPath.add(new MenuStep(Menu.SELECT_CHARGE_PERIOD, callBackStr));
                                menuPath.getLast().setCallBackData(callBackStr);
                            }
                            tm = ui.showCharge(update, klskId, callBackStr);
                        } else if (callBackStr.startsWith(BILLING_PAYMENTS.getCallBackData())) {
                            if (!isBack) {
                                menuPath.add(new MenuStep(Menu.SELECT_PAYMENTS, callBackStr));
                                menuPath.getLast().setCallBackData(callBackStr);
                            }
                            tm = ui.showPayment(update, klskId);
                        } else {
                            tm = ui.selectAddress(update, userId, env.getUserRegisteredKo(), cnt);
                        }
                    }
                } else if (menuPath.size() > 0 && menuPath.getLast().getMenu().equals(Menu.INPUT_VOL)) {
                    // введены показания
                    tm = ui.inputVolAccept(update, userId);
                } else {
                    menuPath.add(new MenuStep(Menu.ROOT, ""));
                    tm = ui.selectAddress(update, userId, env.getUserRegisteredKo(), cnt);
                }

            }

            if (tm instanceof PlainMessage) {
                executeSendMessage(update, ((PlainMessage) tm).getMsg());
            } else if (tm instanceof SimpleMessage) {
                sendSimpleMessage(tm);
            } else if (tm instanceof UpdateMessage) {
                updateMessage(tm);
            } else if (tm instanceof PhotoMessage) {
                sendPhotoMessage(tm);
            }
        } catch (
                Exception e) {
            e.printStackTrace();

/*
            menuPath.add(new MenuStep(Menu.ROOT, ""));
            tm = ui.selectAddress(update, userId, env.getUserRegisteredKo());
            try {
                if (tm instanceof PlainMessage) {
                    executeSendMessage(update, ((PlainMessage) tm).getMsg());
                } else if (tm instanceof SimpleMessage) {
                    sendSimpleMessage(tm);
                } else if (tm instanceof UpdateMessage) {
                    updateMessage(tm);
                } else if (tm instanceof PhotoMessage) {
                    sendPhotoMessage(tm);
                }
            } catch (TelegramApiException ex) {
                throw new RuntimeException(ex);
            }
*/

        }

    }

    private boolean checkAuth(Update update, long userId) throws TelegramApiException {
        long code = ui.authenticateUser(userId);
        if (code != 0) {
            executeSendMessage(update, String.format("Сообщите код оператору, для регистрации = %,d", code).replace(",", " "));
            return true;
        }
        return false;
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
        sm.setText(sm.getText().replace("-", "\\-").replace(".", "\\.").replace("|", "\\|"));
        execute(sm);
    }

    private void updateMessage(TelegramMessage tm) throws TelegramApiException {
        EditMessageText em = ((UpdateMessage) tm).getEm();
        em.setText(em.getText().replace("-", "\\-").replace(".", "\\.").replace("|", "\\|"));
        execute(em);
    }

    private void sendPhotoMessage(TelegramMessage tm) throws TelegramApiException {
        SendPhoto pm = ((PhotoMessage) tm).getPm();
        execute(pm);
    }

    private void deleteMessage(Update update) throws TelegramApiException {
        DeleteMessage dm = new DeleteMessage();
        dm.setChatId(update.getCallbackQuery().getMessage().getChatId().toString());
        dm.setMessageId(update.getCallbackQuery().getMessage().getMessageId());
        execute(dm);
    }


    public String getBotUsername() {
        return botUsername;
    }

    public String getBotToken() {
        return botToken;
    }
}