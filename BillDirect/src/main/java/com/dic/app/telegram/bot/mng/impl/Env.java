package com.dic.app.telegram.bot.mng.impl;

import com.ric.dto.KoAddress;
import com.ric.dto.MapKoAddress;
import com.ric.dto.MapMeter;
import com.ric.dto.SumMeterVolExt;
import lombok.Getter;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

@Getter
public class Env {
    // зарегистрированные на пользователя адреса (фин.лиц.сч.)
    private final Map<Long, MapKoAddress> userRegisteredKo = new ConcurrentHashMap<>();
    // текущий, выбранный адрес пользователя
    private final Map<Long, KoAddress> userCurrentKo = new ConcurrentHashMap<>();
    // текущий, выбранный счетчик пользователя
    private final Map<Long, SumMeterVolExt> userCurrentMeter = new ConcurrentHashMap<>();
    private final Map<Long, Integer> userTemporalCode = new ConcurrentHashMap<>();
    private final Set<Integer> issuedCodes = Collections.newSetFromMap(new ConcurrentHashMap<>());
    private final Map<Long, MapMeter> metersByKlskId = new ConcurrentHashMap<>();
    private final Map<Integer, SumMeterVolExt> meterVolExtByMeterId = new ConcurrentHashMap<>();
    private final Map<Long, Menu> menuPosition = new ConcurrentHashMap<>();
    private final Map<Long, MenuInputVol> menuInputVolPosition = new ConcurrentHashMap<>();
}
