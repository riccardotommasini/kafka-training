package exercise7.model;

public class Location {
    private String location;
    private LocationType type;

    public Location(String location, LocationType type) {
        this.location = location;
        this.type = type;
    }

    public Location() {
    }

    public String getLocation() {
        return location;
    }

    public void setLocation(String location) {
        this.location = location;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Location that = (Location) o;

        return location.equals(that.getLocation());
    }

    @Override
    public int hashCode() {
        return location.hashCode();
    }

    @Override
    public String toString() {
        return "Location - " + type + ":" + location;
    }

    public LocationType getType() {
        return type;
    }

    public void setType(LocationType type) {
        this.type = type;
    }

}
