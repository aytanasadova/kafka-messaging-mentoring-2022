package kafka.example;

import java.util.HashMap;
import java.util.Map;

public class test {

    private static Map<Integer, CoordinateData> lastKnownCoordinatesOfVehicles=new HashMap<>();

    public static void main(String[] args) {

            System.out.println("lastKnownCoordinatesOfVehicles.containsKey(1) " +lastKnownCoordinatesOfVehicles.containsKey(1));

    }
}
