package kafka.example;

import java.util.Objects;

public class DistanceData {
    private int vehicleId;
    private double distance;

    public DistanceData() {
    }

    public DistanceData(int vehicleId, double distance) {
        this.vehicleId = vehicleId;
        this.distance = distance;
    }

    public int getVehicleId() {
        return vehicleId;
    }

    public void setVehicleId(int vehicleId) {
        this.vehicleId = vehicleId;
    }

    public double getDistance() {
        return distance;
    }

    public void setDistance(double distance) {
        this.distance = distance;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof DistanceData that)) return false;
        return vehicleId == that.vehicleId && Double.compare(that.distance, distance) == 0;
    }

    @Override
    public int hashCode() {
        return Objects.hash(vehicleId, distance);
    }

    @Override
    public String toString() {
        return "DistanceData{" +
                "vehicleId=" + vehicleId +
                ", distance=" + distance +
                '}';
    }
}
