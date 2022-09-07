package com.dic.app.telegram.bot.message;

import lombok.Value;
import org.telegram.telegrambots.meta.api.methods.updatingmessages.EditMessageText;

@Value
public class UpdateMessage implements TelegramMessage {
    EditMessageText em;

    public UpdateMessage(EditMessageText em) {

        this.em = em;
    }
}
