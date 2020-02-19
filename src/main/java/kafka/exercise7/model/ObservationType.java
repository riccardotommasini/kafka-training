package kafka.exercise7.model;

public enum ObservationType {

    TEMPERATURE("TEMPERATURE"), HUMIDITY("HUMIDITY"), PRESSURE("PRESSURE");

    private final String name;

    ObservationType(String temperature) {
        this.name = temperature;
    }
}
