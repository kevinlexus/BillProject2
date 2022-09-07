package com.dic.app.telegram.bot.mng;

import com.dic.app.telegram.bot.message.TelegramMessage;
import com.dic.app.telegram.bot.mng.impl.Env;
import com.dic.app.telegram.bot.mng.impl.MeterValSaveState;
import com.ric.dto.MapKoAddress;
import org.telegram.telegrambots.meta.api.objects.Update;
import org.telegram.telegrambots.meta.exceptions.TelegramApiException;

import java.util.Map;

public interface UserInteraction {
    TelegramMessage selectAddress(Update update, long userId, Map<Long, MapKoAddress> registeredKo)
            throws TelegramApiException;

    TelegramMessage selectMeter(Update update, long userId)
            throws TelegramApiException;

    void updateMapMeterByCurrentKlskId(long userId, long klskId);

    TelegramMessage inputVol(Update update, long userId)
            throws TelegramApiException;

    int authenticateUser(long userId);

    /**
     *
     * @param meterId Id счетчика
     * @param strVal показание
     * @return результат сохранения
     */
    MeterValSaveState saveMeterValByMeterId(int meterId, String strVal);

    Env getEnv();

    TelegramMessage wrongInput(Update update, long userId);

    TelegramMessage showBilling(Update update, long userId);
}
