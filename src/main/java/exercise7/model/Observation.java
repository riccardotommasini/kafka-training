package exercise7.model;

public class Observation {

    private int value;
    private long timestamp;

    private ObservationType type;

    public Observation(int value, long timestamp, ObservationType type) {
        this.value = value;
        this.timestamp = timestamp;
        this.type = type;
    }

    public Observation() {
    }

    @Override
    public String toString() {
        return type.name() + "(" + timestamp + "," + value + ")";
    }

    public int getValue() {
        return value;
    }

    public void setValue(int value) {
        this.value = value;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public ObservationType getType() {
        return type;
    }

    public void setType(ObservationType type) {
        this.type = type;
    }
}
