package org.example.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class TelemetryQuery {
    private static final Logger log = LoggerFactory.getLogger(TelemetryQuery.class);
    private String id;
    private String valuePath;
    private String operator;
    private Object operand;
    private Object cachedContext;
    private boolean cached = false;

    public TelemetryQuery() {
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getValuePath() {
        return valuePath;
    }

    public void setValuePath(String valuePath) {
        this.valuePath = valuePath;
    }

    public String getOperator() {
        return operator;
    }

    public void setOperator(String operator) {
        this.operator = operator;
    }

    public Object getOperand() {
        return operand;
    }

    public void setOperand(Object operand) {
        this.operand = operand;
    }

    public boolean appliesTo(DataPoint dp) {
        return getContext(dp) != null;
    }

    private Object getContext(DataPoint dp) {
        if (cached) {
            return cachedContext;
        }
        String[] tokens = valuePath.split("\\.");
        boolean first = true;
        Object context = null;
        for (String token : tokens) {
            if (first) {
                if ("name".equals(token)) {
                    context = dp.getName();
                } else if ("attributes".equals(token)) {
                    context = dp.getAttributes();
                } else {
                    log.warn("Query with non-matching root field: " + this);
                    return null;
                }
                first = false;
            } else if (context == null) {
                return null;
            } else {
                context = ((Map) context).get(token);
            }
        }
        cachedContext = context;
        cached = true;
        return cachedContext;
    }

    public boolean match(DataPoint dp) {
        final Object context = getContext(dp);
        try {
            if ("==".equals(operator)) {
                return operand.equals(context);
            } else if (">".equals(operator)) {
                if (context instanceof String) {
                    return ((String) context).compareTo((String) operand) > 0;
                } else if (context instanceof Integer) {
                    return (Integer) context > (Integer) operand;
                }
            } else if ("<".equals(operator)) {
                if (context instanceof String) {
                    return ((String) context).compareTo((String) operand) < 0;
                } else if (context instanceof Integer) {
                    return (Integer) context < (Integer) operand;
                }
            }
        } catch (ClassCastException e) {
            throw new RuntimeException("Failed to apply operator " + operator + " to values " + context + " and " +
                    operand + " - type of LHS is " + operator.getClass() + " and type of RHS is " +
                    operand.getClass(), e);
        }
        return false;
    }
}
