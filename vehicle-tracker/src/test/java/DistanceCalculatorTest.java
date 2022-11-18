import com.google.common.collect.ImmutableMap;
import kafka.example.CoordinateData;
import kafka.example.CoordinateDataProducer;
import kafka.example.DistanceCalculatorService;
import kafka.example.DistanceMeasuresConsumer;
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
    public void shouldCountWords() throws InterruptedException {

        coordinateDataProducer.send(new CoordinateData(1,123.4,324.5));
        coordinateDataProducer.send(new CoordinateData(1,125.6,400.5));
        coordinateDataProducer.send(new CoordinateData(2,130.4,324.5));
        coordinateDataProducer.send(new CoordinateData(2,128.6,440.5));
        coordinateDataProducer.send(new CoordinateData(4,193.4,329.5));

        Thread.sleep(2000);

        var resultsProducer = distanceMeasuresConsumer.getData();

        assertThat(resultsProducer).isEqualTo(ImmutableMap.of(
                1, 76.03183543753235,
                2,116.01396467667158
        ));

    }

    @AfterEach
    public void tearDown() {
        service.stop();
        distanceMeasuresConsumer.stop();
    }

}
