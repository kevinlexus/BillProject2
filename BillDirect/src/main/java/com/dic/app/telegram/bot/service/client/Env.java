package com.dic.app.telegram.bot.service.client;

import com.dic.app.telegram.bot.service.menu.Menu;
import com.dic.app.telegram.bot.service.menu.MenuNode;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.ric.dto.KoAddress;
import com.ric.dto.MapKoAddress;
import com.ric.dto.MapMeter;
import com.ric.dto.SumMeterVolExt;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
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
    private final Map<Long, Menu> menuPosition = new ConcurrentHashMap<>(); // текущий выбор меню
    private final Map<Long, MenuNode> menuTree = new ConcurrentHashMap<>(); // дерево меню пользователя


    @PostConstruct
    public void loadMenuTree() {
        ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory());
        try (InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream("bot/menu.yaml")) {
            if (is == null) {
                throw new RuntimeException("Файл menu.yaml не найден");
            }
            JsonNode jsonNode = objectMapper.readTree(is);
            //MenuNode menuNode = new MenuNode(Menu.getByNameIgnoreCase("root"), null);
            MenuNode menu = processYaml("root", jsonNode, null);
            System.out.println("check!");
        } catch (IOException e) {
            throw new IllegalStateException("Ошибка чтения файла menu.yaml", e);
        }
    }

    private MenuNode processYaml(String prefix, JsonNode currentNode, MenuNode parent) {
        Menu menu = Menu.getByNameIgnoreCase(prefix);
        MenuNode menuNode = new MenuNode(menu, parent);
        if (currentNode.isArray()) {
            log.info("array:{}", prefix);
            parent.getLeaves().add(menuNode);
            ArrayNode arrayNode = (ArrayNode) currentNode;
            Iterator<JsonNode> node = arrayNode.elements();
            int index = 1;
            while (node.hasNext()) {
                processYaml(!prefix.isEmpty() ? prefix + "-" + index : String.valueOf(index), node.next(), menuNode);
                index += 1;
            }
        } else if (currentNode.isObject()) {
            //currentNode.fields().forEachRemaining(entry -> processYaml(!prefix.isEmpty() ? prefix + "-" + entry.getKey() : entry.getKey(), entry.getValue()));
            log.info("object:{}", prefix);
            if (parent != null) {
                parent.getLeaves().add(menuNode);
            }
            currentNode.fields().forEachRemaining(entry -> processYaml(entry.getKey(), entry.getValue(), menuNode));
        } else {
            log.info(prefix);
            parent.getLeaves().add(menuNode);
            // log.info(prefix + ": " + currentNode);
        }
        return menuNode;
    }


}
