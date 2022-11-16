import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import kafka.example.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

@EmbeddedKafka(brokerProperties = {
    "transaction.state.log.min.isr=1",
    "transaction.state.log.replication.factor=1"
})
@SpringJUnitConfig(classes = TestContext.class)
public class VehicleTrackerTest {

    @Autowired
    private VehicleTrackerService service;

    @Autowired
    private VehicleDataProducer sentenceProducer;

    @Autowired
    private VehicleDataProducer vehicleCoordinateProducer;
    @Autowired
    private VehicleDistanceConsumer vehicleCoordinateConsumer;

    @BeforeEach
    public void setup() {
        service.run();
        vehicleCoordinateConsumer.start();
    }


    @Test
    public void shouldPublishCoordinates() throws InterruptedException, JsonProcessingException {

        List<VehicleCoordinates> vehicleCoordinatesList=new ArrayList<>();
        vehicleCoordinatesList.add(new VehicleCoordinates(1,38.8951,-77.0364));
        vehicleCoordinatesList.add(new VehicleCoordinates(1,38.8951,-77.0364));
        vehicleCoordinatesList.add(new VehicleCoordinates(2,38.8951,-77.0364));
        vehicleCoordinatesList.add(new VehicleCoordinates(2,38.8951,-77.0364));
        vehicleCoordinatesList.add(new VehicleCoordinates(3,38.8951,-77.0364));


        for (VehicleCoordinates vehicleCoordinates : vehicleCoordinatesList) {
            vehicleCoordinateProducer.send(new ObjectMapper().writeValueAsString(vehicleCoordinates));
        }

        Thread.sleep(5000);

        var result = vehicleCoordinateConsumer.getData();
//        assertThat(result).isEqualTo(ImmutableMap.of(
//                "learning", 1,
//                "messaging", 2,
//                "and", 1,
//                "brokers", 1
//        ));
    }



    @Test
    public void shouldCountWords() throws InterruptedException {
        sentenceProducer.send("learning messaging and messaging brokers");

        Thread.sleep(1000);

        var result = vehicleCoordinateConsumer.getData();
        assertThat(result).isEqualTo(ImmutableMap.of(
            "learning", 1,
            "messaging", 2,
            "and", 1,
            "brokers", 1
        ));
    }

    @AfterEach
    public void tearDown() {
        service.stop();
        vehicleCoordinateConsumer.stop();
    }

}
