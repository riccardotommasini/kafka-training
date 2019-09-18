package exercise7.model;

public enum LocationType {

    ROOM("ROOM"), BUILDING("BUILDING"), PARK("PARK");

    private final String name;

    LocationType(String temperature) {
        this.name = temperature;
    }
}
