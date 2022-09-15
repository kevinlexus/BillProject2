package com.dic.app.telegram.bot.mng.impl;

import com.dic.app.telegram.bot.message.SimpleMessage;
import com.dic.app.telegram.bot.message.TelegramMessage;
import com.dic.app.telegram.bot.message.UpdateMessage;
import lombok.Getter;
import org.telegram.telegrambots.meta.api.methods.ParseMode;
import org.telegram.telegrambots.meta.api.methods.send.SendMessage;
import org.telegram.telegrambots.meta.api.methods.updatingmessages.EditMessageText;
import org.telegram.telegrambots.meta.api.objects.Update;
import org.telegram.telegrambots.meta.api.objects.replykeyboard.InlineKeyboardMarkup;
import org.telegram.telegrambots.meta.api.objects.replykeyboard.buttons.InlineKeyboardButton;

import java.util.ArrayList;
import java.util.List;

class MessageStore {
    private final Update update;
    private List<InlineKeyboardButton> buttons = new ArrayList<>();
    @Getter
    private List<List<InlineKeyboardButton>> rowList = new ArrayList<>();

    InlineKeyboardMarkup inlineKeyboardMarkup = new InlineKeyboardMarkup();

    @Getter
    private int buttonNum = 1;

    public MessageStore(Update update) {
        this.update = update;
    }

    public void addButton(String callBackData, String caption) {
        InlineKeyboardButton inlineKeyboardButton = new InlineKeyboardButton();
        inlineKeyboardButton.setCallbackData(callBackData);
        inlineKeyboardButton.setText(caption);
        buttons.add(inlineKeyboardButton);
        if (buttonNum % 2 == 0) {
            rowList.add(buttons);
            buttons = new ArrayList<>();
        }
        buttonNum++;
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
    public void addButton(Buttons button) {
        addButton(button.getCallBackData(), button.toString());
    }

    public void addButtonCallBack(String callBackData, String msgKeyb) {
        addButton(callBackData, msgKeyb);
    }

    public TelegramMessage build(StringBuilder msg) {
        if (buttons.size()>0) {
            rowList.add(buttons);
        }
        inlineKeyboardMarkup.setKeyboard(rowList);
        return createMessage(update, msg, inlineKeyboardMarkup);
    }

    public TelegramMessage buildUpdateMessage(String msg) {
        inlineKeyboardMarkup.setKeyboard(rowList);
        EditMessageText em = new EditMessageText();
        if (update.getMessage() == null) {
            em.setChatId(update.getCallbackQuery().getMessage().getChatId().toString());
            em.setMessageId(update.getCallbackQuery().getMessage().getMessageId());
        } else {
            em.setChatId(update.getMessage().getChatId());
        }
        em.setText(msg);
        em.setReplyMarkup(inlineKeyboardMarkup);
        return new UpdateMessage(em);
    }

    public TelegramMessage buildUpdateMessage2(StringBuilder msg) {
        inlineKeyboardMarkup.setKeyboard(rowList);
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
}

