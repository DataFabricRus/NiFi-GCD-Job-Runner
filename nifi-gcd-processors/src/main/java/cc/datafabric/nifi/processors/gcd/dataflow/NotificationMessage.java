package cc.datafabric.nifi.processors.gcd.dataflow;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class NotificationMessage implements Serializable {

    private String state;
    private Map<String, String> fields = new HashMap<>();

    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
    }

    public Map<String, String> getFields() {
        return fields;
    }

    public void setFields(Map<String, String> fields) {
        this.fields = fields;
    }

    public NotificationMessage addField(String title, String value) {
        fields.put(title, value);
        return this;
    }

}
