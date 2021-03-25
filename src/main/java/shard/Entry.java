package shard;

public class Entry {

    private final Object key;
    private Object value;

    public Entry(Object k, Object v) {
        key = k;
        value = v;
    }

    public Object getKey() {
        return key;
    }

    public Object getValue() {
        return value;
    }

    public void setValue(Object v) {
        value = v;
    }

    @Override
    public String toString() {
        return String.format("(%s,%S)", key, value);
    }
}

