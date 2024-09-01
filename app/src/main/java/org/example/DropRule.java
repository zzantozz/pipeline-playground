package org.example;

import java.util.Map;

/**
 * Implements a "drop rule". If the account, field, and value match an incoming data point, the dropping debatcher will
 * send it to /dev/null instead of processing it any further.
 */
public class DropRule {
    private String account;
    private String fieldMatch;
    private String valueMatch;

    public String getAccount() {
        return account;
    }

    public void setAccount(String account) {
        this.account = account;
    }

    public String getFieldMatch() {
        return fieldMatch;
    }

    public void setFieldMatch(String fieldMatch) {
        this.fieldMatch = fieldMatch;
    }

    public String getValueMatch() {
        return valueMatch;
    }

    public void setValueMatch(String valueMatch) {
        this.valueMatch = valueMatch;
    }

    public boolean match(DataPoint dp) {
        for (Map.Entry<String, String> entry : dp.getAttributes().entrySet()) {
            if (entry.getKey().contains(fieldMatch) && entry.getValue().contains(valueMatch)) {
                return true;
            }
        }
        return false;
    }
}
