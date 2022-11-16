import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import kafka.example.VehicleDataProducer;
import kafka.example.VehicleCoordinateConsumer;
import kafka.example.VehicleTrackerService;
import kafka.example.VehicleCoordinates;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

@EmbeddedKafka(brokerProperties = {
    "transaction.state.log.min.isr=1",
    "transaction.state.log.replication.factor=1"
})
@SpringJUnitConfig(classes = TestContext.class)
public class WordCounterTest {

    @Autowired
    private VehicleTrackerService service;

    @Autowired
    private VehicleDataProducer sentenceProducer;

    @Autowired
    private VehicleDataProducer vehicleCoordinateProducer;
    @Autowired
    private VehicleCoordinateConsumer wordCountConsumer;

    @BeforeEach
    public void setup() {
        service.run();
        wordCountConsumer.start();
    }


    @Test
    public void shouldPublishCoordinates() throws InterruptedException, JsonProcessingException {

        vehicleCoordinateProducer.send(new ObjectMapper().writeValueAsString(new VehicleCoordinates()));

        Thread.sleep(1000);

        var result = wordCountConsumer.getData();
        assertThat(result).isEqualTo(ImmutableMap.of(
                "learning", 1,
                "messaging", 2,
                "and", 1,
                "brokers", 1
        ));
    }



    @Test
    public void shouldCountWords() throws InterruptedException {
        sentenceProducer.send("learning messaging and messaging brokers");

        Thread.sleep(1000);

        var result = wordCountConsumer.getData();
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
        wordCountConsumer.stop();
    }

}
