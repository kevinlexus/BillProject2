package com.dic.app.telegram.bot.message;

import lombok.Value;

@Value
public class PlainMessage implements TelegramMessage {
    String msg;

    public PlainMessage(String msg) {
        this.msg = msg;
    }
}
