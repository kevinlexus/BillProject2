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

        Long klskId;
        try {
            log.info("BOT: 1.аутентификация пользователя в системе userid={}", userId);
            if (checkAuth(update, userId)) return;
            log.info("BOT: 2.аутентификация пользователя в системе userid={}", userId);
            if (!env.getUserRegisteredKo().containsKey(userId)) {
                // аутентификация пользователя в системе
                menuPath.add(new MenuStep(Menu.ROOT, ""));
                tm = ui.selectAddress(update, userId, env.getUserRegisteredKo(), cnt);
                log.info("BOT: 3.аутентификация пользователя в системе userid={}", userId);
            } else {
                if (update.hasCallbackQuery()) {
                    // нажата кнопка (выбрано меню)
                    callBackStr = update.getCallbackQuery().getData();
                    boolean isBack = false;
                    boolean wrongButton = false;
                    if (callBackStr.startsWith(BACK.getCallBackData())) {
                        log.info("BOT: 4 userid={}", userId);
                        isBack = true;
                        if (!menuPath.isEmpty()) {
                            menuPath.removeLast();
                            if (!menuPath.isEmpty()) {
                                callBackStr = menuPath.getLast().getCallBackData();
                                log.info("BOT: 5 userid={}", userId);
                            } else {
                                // что то пошло не так и выкинуть на главный экран
                                menuPath.add(new MenuStep(Menu.ROOT, ""));
                                tm = ui.selectAddress(update, userId, env.getUserRegisteredKo(), cnt);
                                wrongButton = true; // нажата кнопка из "старого" чата, что может приводить к ошибке
                                log.info("BOT: 6 userid={}", userId);
                            }
                        }
                    }

                    klskId = ui.getKlskIdFromCallback(callBackStr).orElse(null);
                    log.info("BOT: 7 userid={}", userId);
                    if (klskId == null || !env.checkAccessByKlskId(userId, klskId)) {
                        log.info("BOT: 8 userid={}", userId);
                        menuPath.add(new MenuStep(Menu.ROOT, ""));
                        tm = ui.selectAddress(update, userId, env.getUserRegisteredKo(), cnt);
                        wrongButton = true; // нажата кнопка из "старого" чата, что может приводить к ошибке
                        log.info("BOT: 9 userid={}", userId);
                    } else {
                        ui.updateMapMeterByKlskId(userId, klskId);
                        log.info("BOT: 9.1 userid={}", userId);
                    }

                    if (!wrongButton) {
                        log.info("BOT: 10 userid={}", userId);

                        if (callBackStr.startsWith(ADDRESS_KLSK.getCallBackData() + "_")) {
                            log.info("BOT: 11 userid={}", userId);
                            if (!isBack) {
                                log.info("BOT: 12 userid={}", userId);
                                menuPath.add(new MenuStep(Menu.SELECT_METER, null));
                                menuPath.getLast().setCallBackData(callBackStr);
                                log.info("BOT: 13 userid={}", userId);
                            }
                            tm = ui.selectMeter(update, klskId, callBackStr, userId);
                            log.info("BOT: 14 userid={}", userId);
                        } else if (callBackStr.startsWith(METER.getCallBackData() + "_")) {
                            log.info("BOT: 15 userid={}", userId);
                            if (!isBack) {
                                log.info("BOT: 16 userid={}", userId);
                                menuPath.add(new MenuStep(Menu.INPUT_VOL, null));
                                menuPath.getLast().setCallBackData(callBackStr);
                                log.info("BOT: 17 userid={}", userId);
                            }
                            tm = ui.inputVol(update, callBackStr, userId);
                            log.info("BOT: 18 userid={}", userId);
                        } else if (callBackStr.startsWith(REPORTS.getCallBackData())) {
                            log.info("BOT: 19 userid={}", userId);
                            if (!isBack) {
                                log.info("BOT: 20 userid={}", userId);
                                menuPath.add(new MenuStep(Menu.SELECT_REPORT, callBackStr));
                                menuPath.getLast().setCallBackData(callBackStr);
                                log.info("BOT: 21 userid={}", userId);
                            }
                            tm = ui.selectReport(update, klskId);
                            log.info("BOT: 22 userid={}", userId);
                        } else if (callBackStr.startsWith(BILLING_FLOW.getCallBackData())) {
                            log.info("BOT: 23 userid={}", userId);
                            if (!isBack) {
                                log.info("BOT: 24 userid={}", userId);
                                menuPath.add(new MenuStep(Menu.SELECT_FLOW, callBackStr));
                                menuPath.getLast().setCallBackData(callBackStr);
                                log.info("BOT: 25 userid={}", userId);
                            }
                            tm = ui.showFlow(update, klskId, userId);
                            log.info("BOT: 26 userid={}", userId);
                        } else if (callBackStr.startsWith(BILLING_CHARGES.getCallBackData())) {
                            log.info("BOT: 27 userid={}", userId);
                            if (!isBack) {
                                log.info("BOT: 28 userid={}", userId);
                                menuPath.add(new MenuStep(Menu.SELECT_CHARGE, callBackStr));
                                menuPath.getLast().setCallBackData(callBackStr);
                                log.info("BOT: 29 userid={}", userId);
                            }
                            tm = ui.selectChargeReport(update, klskId);
                            log.info("BOT: 30 userid={}", userId);
                            //tm = ui.showCharge(update, userId);
                        } else if (callBackStr.startsWith(BILLING_CHARGES_PERIOD.getCallBackData() + "_")) {
                            log.info("BOT: 31 userid={}", userId);
                            if (!isBack) {
                                log.info("BOT: 32 userid={}", userId);
                                menuPath.add(new MenuStep(Menu.SELECT_CHARGE_PERIOD, callBackStr));
                                menuPath.getLast().setCallBackData(callBackStr);
                                log.info("BOT: 33 userid={}", userId);
                            }
                            tm = ui.showCharge(update, klskId, callBackStr);
                            log.info("BOT: 34 userid={}", userId);
                        } else if (callBackStr.startsWith(BILLING_PAYMENTS.getCallBackData())) {
                            log.info("BOT: 35 userid={}", userId);
                            if (!isBack) {
                                log.info("BOT: 36 userid={}", userId);
                                menuPath.add(new MenuStep(Menu.SELECT_PAYMENTS, callBackStr));
                                menuPath.getLast().setCallBackData(callBackStr);
                                log.info("BOT: 37 userid={}", userId);
                            }
                            tm = ui.showPayment(update, klskId);
                            log.info("BOT: 38 userid={}", userId);
                        } else {
                            log.info("BOT: 39 userid={}", userId);
                            tm = ui.selectAddress(update, userId, env.getUserRegisteredKo(), cnt);
                            log.info("BOT: 40 userid={}", userId);
                        }
                    }
                } else if (!menuPath.isEmpty() && menuPath.getLast().getMenu().equals(Menu.INPUT_VOL)) {
                    // введены показания
                    log.info("BOT: 41 userid={}", userId);
                    if (env.getUserCurrentKo().get(userId)!=null) {
                        ui.updateMapMeterByKlskId(userId, env.getUserCurrentKo().get(userId).getKlskId());
                        tm = ui.inputVolAccept(update, userId);
                        log.info("BOT: 41.1 userid={}", userId);
                    } else {
                        log.error("Должно быть заполнено env.getUserCurrentKo().get(userId) по userId={}!!!", userId);
                    }
                    log.info("BOT: 42 userid={}", userId);
                } else {
                    log.info("BOT: 43 userid={}", userId);
                    menuPath.add(new MenuStep(Menu.ROOT, ""));
                    tm = ui.selectAddress(update, userId, env.getUserRegisteredKo(), cnt);
                    log.info("BOT: 44 userid={}", userId);
                }
            }

            log.info("BOT: 45 userid={}", userId);
            if (tm instanceof PlainMessage) {
                executeSendMessage(update, ((PlainMessage) tm).getMsg());
                log.info("BOT: 46 userid={}", userId);
            } else if (tm instanceof SimpleMessage) {
                sendSimpleMessage(tm);
                log.info("BOT: 47 userid={}", userId);
            } else if (tm instanceof UpdateMessage) {
                updateMessage(tm);
                log.info("BOT: 48 userid={}", userId);
            } else if (tm instanceof PhotoMessage) {
                sendPhotoMessage(tm);
                log.info("BOT: 49 userid={}", userId);
            }
        } catch (Exception e) {
            log.error("Error:", e);
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