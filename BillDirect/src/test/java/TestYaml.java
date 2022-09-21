import com.dic.app.telegram.bot.service.menu.MenuNode;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

import java.util.*;

@Slf4j
public class TestYaml {




    public static final String YAML = "meter  :\n" +
            "        cold   :\n" +
            "        hot    :\n" +
            "        heat   :\n" +
            "report :\n" +
            "        flow   :\n" +
            "        charge :\n" +
            "        payment:\n";

    @Test
    public void testYamlHierarchical() throws JsonProcessingException {
        ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory());
        JsonNode jsonNode = objectMapper.readTree(YAML);

        MenuNode menuMenuNode = new MenuNode("root", null);
        process("", jsonNode, menuMenuNode);
        System.out.println("check!");
    }

    private void process(String prefix, JsonNode currentNode, MenuNode menuMenuNode) {
        if (currentNode.isArray()) {
            log.info("array:{}", prefix);
            MenuNode menu = new MenuNode(prefix, menuMenuNode);
            menu.setData(prefix);
            menuMenuNode.getLeaves().add(menu);
            ArrayNode arrayNode = (ArrayNode) currentNode;
            Iterator<JsonNode> node = arrayNode.elements();
            int index = 1;
            while (node.hasNext()) {
                process(!prefix.isEmpty() ? prefix + "-" + index : String.valueOf(index), node.next(), menu);
                index += 1;
            }
        } else if (currentNode.isObject()) {
            //currentNode.fields().forEachRemaining(entry -> process(!prefix.isEmpty() ? prefix + "-" + entry.getKey() : entry.getKey(), entry.getValue()));
            log.info("object:{}", prefix);
            MenuNode menu = new MenuNode(prefix, menuMenuNode);
            menu.setData(prefix);
            menuMenuNode.getLeaves().add(menu);
            currentNode.fields().forEachRemaining(entry -> process(entry.getKey(), entry.getValue(), menu));
        } else {
            log.info(prefix);
            MenuNode menu = new MenuNode(prefix, menuMenuNode);
            menu.setData(prefix);
            menuMenuNode.getLeaves().add(menu);
            // log.info(prefix + ": " + currentNode);
        }
    }

}
