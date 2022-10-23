package com.dic.app.telegram.bot.service.message;

import com.dic.app.telegram.bot.message.PhotoMessage;
import com.dic.app.telegram.bot.message.SimpleMessage;
import com.dic.app.telegram.bot.message.TelegramMessage;
import com.dic.app.telegram.bot.message.UpdateMessage;
import com.dic.app.telegram.bot.service.menu.Buttons;
import com.ric.cmn.Utl;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.telegram.telegrambots.meta.api.methods.ParseMode;
import org.telegram.telegrambots.meta.api.methods.send.SendMessage;
import org.telegram.telegrambots.meta.api.methods.send.SendPhoto;
import org.telegram.telegrambots.meta.api.methods.updatingmessages.EditMessageText;
import org.telegram.telegrambots.meta.api.objects.CallbackQuery;
import org.telegram.telegrambots.meta.api.objects.InputFile;
import org.telegram.telegrambots.meta.api.objects.Message;
import org.telegram.telegrambots.meta.api.objects.Update;
import org.telegram.telegrambots.meta.api.objects.replykeyboard.InlineKeyboardMarkup;
import org.telegram.telegrambots.meta.api.objects.replykeyboard.buttons.InlineKeyboardButton;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

@Slf4j
public class MessageStore {
    public static final int WIDTH_ADD = 25; // магическое число, если шрифт будет меньше 14, начнут вылезать поля
    public static final String FONT_LUCIDA_CONSOLE = "Lucida Console";
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
            CallbackQuery callbackQuery = update.getCallbackQuery();
            Message message = callbackQuery.getMessage();
            if (callbackQuery.getMessage().hasPhoto()) {
                SendMessage sm = new SendMessage();
                sm.setText(msg.toString());

                sm.setParseMode(ParseMode.MARKDOWNV2);

                sm.setChatId(message.getChatId().toString());
                sm.setReplyMarkup(inlineKeyboardMarkup);
                return new SimpleMessage(sm);
            } else {
                EditMessageText em = new EditMessageText();
                em.setText(msg.toString());
                em.setChatId(message.getChatId().toString());
                em.setMessageId(message.getMessageId());
                em.setReplyMarkup(inlineKeyboardMarkup);

                em.setParseMode(ParseMode.MARKDOWNV2);
                return new UpdateMessage(em);
            }
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
        if (buttons.size() > 0) {
            rowList.add(buttons);
        }
        inlineKeyboardMarkup.setKeyboard(rowList);
        return createMessage(update, msg, inlineKeyboardMarkup);
    }

    public TelegramMessage buildPhoto(StringBuilder msg, String fileName) {
        SendPhoto pm = new SendPhoto();
        ByteArrayInputStream stream = Utl.renderImage(msg, FONT_LUCIDA_CONSOLE, 14, 25);
        InputFile inputFile = new InputFile();
        inputFile.setMedia(stream, fileName + ".png");
        if (buttons.size() > 0) {
            rowList.add(buttons);
        }
        inlineKeyboardMarkup.setKeyboard(rowList);
        pm.setReplyMarkup(inlineKeyboardMarkup);
        pm.setPhoto(inputFile);
        pm.setCaption(fileName);
        if (update.getMessage() == null) {
            pm.setChatId(update.getCallbackQuery().getMessage().getChatId().toString());
        } else {
            pm.setChatId(update.getMessage().getChatId());
        }
        return new PhotoMessage(pm);
    }

}

