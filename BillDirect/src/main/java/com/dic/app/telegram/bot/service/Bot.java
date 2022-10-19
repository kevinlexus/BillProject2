package com.dic.app.telegram.bot.service;

import com.dic.app.telegram.bot.message.PlainMessage;
import com.dic.app.telegram.bot.message.SimpleMessage;
import com.dic.app.telegram.bot.message.TelegramMessage;
import com.dic.app.telegram.bot.message.UpdateMessage;
import com.dic.app.telegram.bot.service.client.Env;
import com.dic.app.telegram.bot.service.menu.Menu;
import com.dic.app.telegram.bot.service.menu.MenuStep;
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

import java.util.LinkedList;

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

            LinkedList<MenuStep> menuPath = env.getUserMenuPath().get(userId);
            if (menuPath == null) {
                env.getUserMenuPath().put(userId, new LinkedList<>());
                menuPath = env.getUserMenuPath().get(userId);
            }

            String callBackStr;
            TelegramMessage tm = null;
            if (!env.getUserRegisteredKo().containsKey(userId)) {
                // аутентификация пользователя в системе
                long code = ui.authenticateUser(userId);
                if (code != 0) {
                    executeSendMessage(update, String.format("Сообщите код оператору, для регистрации = %,d", code).replace(","," "));
                    return;
                } else {
                    menuPath.add(new MenuStep(Menu.ROOT, ""));
                    tm = ui.selectAddress(update, userId, env.getUserRegisteredKo());
                }
            } else {
                if (update.hasCallbackQuery()) {
                    // нажата кнопка (выбрано меню)
                    callBackStr = update.getCallbackQuery().getData();
                    boolean isBack = false;
                    if (callBackStr.startsWith(BACK.getCallBackData())) {
                        isBack = true;
                        if (menuPath.size() > 0) {
                            menuPath.removeLast();
                            callBackStr = menuPath.getLast().getCallBackData();
                        }
                    }

                    if (callBackStr.startsWith(ADDRESS_KLSK.getCallBackData() + "_")) {
                        if (!isBack) {
                            menuPath.add(new MenuStep(Menu.SELECT_METER, null));
                            menuPath.getLast().setCallBackData(callBackStr);
                        }
                        tm = ui.selectMeter(update, callBackStr, userId);
                    } else if (callBackStr.startsWith(METER.getCallBackData() + "_")) {
                        if (!isBack) {
                            menuPath.add(new MenuStep(Menu.INPUT_VOL, null));
                            menuPath.getLast().setCallBackData(callBackStr);
                        }
                        tm = ui.inputVol(update, callBackStr, userId);
                    } else if (callBackStr.equals(REPORTS.getCallBackData())) {
                        if (!isBack) {
                            menuPath.add(new MenuStep(Menu.SELECT_REPORT, callBackStr));
                            menuPath.getLast().setCallBackData(callBackStr);
                        }
                        tm = ui.selectReport(update, userId);
                    } else if (callBackStr.equals(BILLING_FLOW.getCallBackData())) {
                        if (!isBack) {
                            menuPath.add(new MenuStep(Menu.SELECT_FLOW, callBackStr));
                            menuPath.getLast().setCallBackData(callBackStr);
                        }
                        tm = ui.showFlow(update, userId);
                    } else if (callBackStr.equals(BILLING_CHARGES.getCallBackData())) {
                        if (!isBack) {
                            menuPath.add(new MenuStep(Menu.SELECT_CHARGE, callBackStr));
                            menuPath.getLast().setCallBackData(callBackStr);
                        }
                        tm = ui.showCharge(update, userId);
                    } else if (callBackStr.equals(BILLING_PAYMENTS.getCallBackData())) {
                        if (!isBack) {
                            menuPath.add(new MenuStep(Menu.SELECT_PAYMENTS, callBackStr));
                            menuPath.getLast().setCallBackData(callBackStr);
                        }
                        tm = ui.showPayment(update, userId);
                    } else {
                        tm = ui.selectAddress(update, userId, env.getUserRegisteredKo());
                    }
                } else if (menuPath.getLast().getMenu().equals(Menu.INPUT_VOL)) {
                    // введены показания
                    tm = ui.inputVolAccept(update, userId);
                } else {
                    menuPath.add(new MenuStep(Menu.ROOT, ""));
                    tm = ui.selectAddress(update, userId, env.getUserRegisteredKo());
                }
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
        log.info("text={}", em.getText());
        em.setText(em.getText().replace("-","\\-")); // todo перенести сюда все исправления . и прочих символов
        execute(em);
    }


    public String getBotUsername() {
        return botUsername;
    }

    public String getBotToken() {
        return botToken;
    }
}