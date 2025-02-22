package com.github.nlayna.ksolar.model;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Getter;
import lombok.Setter;
import org.apache.solr.common.SolrInputDocument;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class SolrMessage {
    @Getter
    private String id;

    @Getter
    private String action;

    @Getter
    private Map<String, Object> resFields;

    @Getter
    @Setter
    private long kafkaTimestamp;

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static SolrMessage fromJson(String json) throws Exception{
        var node = MAPPER.readTree(json);

        if (!node.has("id") || !node.has("action")) {
            throw new IllegalArgumentException("JSON object does not contain an id or an action");
        }

        var message = new SolrMessage();
        message.id = node.get("id").asText();
        message.action = node.get("action").asText();

        Map<String, Object> fields = new HashMap<>();
        Iterator<Map.Entry<String, JsonNode>> it = node.fields();
        while (it.hasNext()) {
            Map.Entry<String, JsonNode> entry = it.next();
            String key = entry.getKey();
            if (!key.equals("id") && !key.equals("action")) {
                fields.put(key, MAPPER.convertValue(entry.getValue(), Object.class));

            }
        }
        message.resFields = fields;
        return message;
    }

    public Map<String, Object> toMap() {
        Map<String, Object> result = new HashMap<>(resFields);
        result.put("id", id);
        return result;
    }

    public SolrInputDocument toSolrInputDocument() {
        var doc = new SolrInputDocument();
        resFields.forEach((k, v) -> doc.addField(k, v.toString()));
        doc.addField("id", id);

        return doc;
    }
}
