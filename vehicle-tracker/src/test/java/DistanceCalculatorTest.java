import com.google.common.collect.ImmutableMap;
import kafka.example.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import java.util.List;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

@EmbeddedKafka(brokerProperties = {
    "transaction.state.log.min.isr=1",
    "transaction.state.log.replication.factor=1"
})
@SpringJUnitConfig(classes = TestContext.class)
public class DistanceCalculatorTest {

    @Autowired
    private DistanceCalculatorService service;

    @Autowired
    private CoordinateDataProducer coordinateDataProducer;
    @Autowired
    private DistanceMeasuresConsumer distanceMeasuresConsumer;

    @BeforeEach
    public void setup() {
        service.run();
        distanceMeasuresConsumer.start();
    }

    @Test
    public void shouldCalculateDistance() throws InterruptedException {

        coordinateDataProducer.send(new CoordinateData(1,123.4,324.5));
        coordinateDataProducer.send(new CoordinateData(1,125.6,400.5));
        coordinateDataProducer.send(new CoordinateData(2,130.4,324.5));
        coordinateDataProducer.send(new CoordinateData(2,128.6,440.5));
        coordinateDataProducer.send(new CoordinateData(2,193.4,329.5));

        Thread.sleep(2000);

        var resultsProducer = distanceMeasuresConsumer.getData();

        assertThat(resultsProducer).isEqualTo(List.of(
                new DistanceData(1, 76.031835437532356),
                new DistanceData(2,116.01396467667158),
                new DistanceData(2,128.53030770989386)
        ));

    }

    @AfterEach
    public void tearDown() {
        service.stop();
        distanceMeasuresConsumer.stop();
    }

}
