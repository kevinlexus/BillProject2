package com.dic.app.telegram.bot.message;

import lombok.Value;
import org.telegram.telegrambots.meta.api.methods.send.SendPhoto;

@Value
public class PhotoMessage implements TelegramMessage {
    SendPhoto pm;

    public PhotoMessage(SendPhoto pm) {

        this.pm = pm;
    }
}
