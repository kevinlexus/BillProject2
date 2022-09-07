package com.dic.app.telegram.bot.message;

import lombok.Value;
import org.telegram.telegrambots.meta.api.methods.send.SendMessage;

@Value
public class SimpleMessage implements TelegramMessage {
    SendMessage sm;

    public SimpleMessage(SendMessage sm) {

        this.sm = sm;
    }
}
