package com.dic.app.telegram.bot.service.client;

import com.dic.app.telegram.bot.service.menu.MenuStep;
import com.ric.dto.KoAddress;
import com.ric.dto.MapKoAddress;
import com.ric.dto.MapMeter;
import com.ric.dto.SumMeterVolExt;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

@Getter
@Service
@Slf4j
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
    private final Map<Long, LinkedList<MenuStep>> userMenuPath= new ConcurrentHashMap<>(); // путь по меню пользователя


}
