package kafka.advanced.exercise6.solution.exercise6a.model;

public class Room {

    private String location;

    public Room(String location) {
        this.location = location;
    }

    public Room() {
    }

    public String getLocation() {
        return location;
    }

    public void setLocation(String location) {
        this.location = location;
    }

    @Override
    public String toString() {
        return "TemperatureKey " +
                "'location' " + location;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Room that = (Room) o;

        return location.equals(that.getLocation());
    }

    @Override
    public int hashCode() {
        return location.hashCode();
    }
}
