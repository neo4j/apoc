package apoc.trigger;

import apoc.SystemPropertyKeys;

import java.util.Map;

public class TriggerInfo {
    public String name;
    public String query;
    public Map<String, Object> selector;
    public Map<String, Object> params;
    public boolean installed = false;
    public boolean paused = false;

    public TriggerInfo(String name) {
        this.name = name;
    }

    public TriggerInfo(String name, String query) {
        this(name);
        this.query = query;
    }

    public TriggerInfo(String name, String query, Map<String, Object> selector, boolean installed, boolean paused) {
        this(name, query);
        this.selector = selector;
        this.installed = installed;
        this.paused = paused;
    }


    public TriggerInfo(String name, String query, Map<String, Object> selector, Map<String, Object> params, boolean installed, boolean paused) {
        this(name, query, selector, installed, paused);
        this.params = params;
    }

    public static TriggerInfo from(Map<String, Object> mapInfo, boolean installed, String name) {
        return new TriggerInfo(name,
                (String) mapInfo.get(SystemPropertyKeys.statement.name()),
                (Map<String, Object>) mapInfo.get(SystemPropertyKeys.selector.name()),
                (Map<String, Object>) mapInfo.get(SystemPropertyKeys.params.name()),
                installed,
                (boolean) mapInfo.getOrDefault(SystemPropertyKeys.paused.name(), true));
    }

    public static TriggerInfo from(Map<String, Object> mapInfo, boolean installed) {
        return from(mapInfo, installed, (String) mapInfo.get(SystemPropertyKeys.name.name()));
    }


    public static TriggerInfo entryToTriggerInfo(Map.Entry<String, Object> e) {
        String name = e.getKey();
        if (e.getValue() instanceof Map) {
            try {
                Map<String, Object> value = (Map<String, Object>) e.getValue();
                return TriggerInfo.from(value, false, name);
            } catch (Exception ex) {
                return new TriggerInfo(name, ex.getMessage());
            }
        }
        return new TriggerInfo(name);
    }
}
