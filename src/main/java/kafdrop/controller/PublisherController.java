package kafdrop.controller;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import kafdrop.model.PublishPayloadVO;
import kafdrop.service.BuildInfo;
import kafdrop.service.KafkaHighLevelProducer;
import kafdrop.service.KafkaMonitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.info.BuildProperties;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;

import java.util.Properties;

@Controller
@RequestMapping("/publish")
public class PublisherController {
  private static final Logger LOG = LoggerFactory.getLogger(PublisherController.class);
  private final BuildProperties buildProperties;
  private final KafkaHighLevelProducer producer;
  private final KafkaMonitor kafkaMonitor;

  public PublisherController(KafkaMonitor kafkaMonitor,
                             ObjectProvider<BuildInfo> buildInfoProvider,
                             KafkaHighLevelProducer producer) {
    this.kafkaMonitor = kafkaMonitor;
    this.buildProperties = buildInfoProvider.stream()
      .map(BuildInfo::getBuildProperties)
      .findAny()
      .orElseGet(PublisherController::blankBuildProperties);
    this.producer = producer;
  }

  private static BuildProperties blankBuildProperties() {
    final var properties = new Properties();
    properties.setProperty("version", "3.x");
    properties.setProperty("time", String.valueOf(System.currentTimeMillis()));
    return new BuildProperties(properties);
  }

  @RequestMapping("/ui")
  public String consumerDetail(Model model) {
    model.addAttribute("topicNames", kafkaMonitor.getTopics().toArray());
    model.addAttribute("buildProperties", buildProperties);
    return "publish-payload";
  }

  @PostMapping("{topic}")
  public void publish(@PathVariable("topic") String producerTopics, @RequestBody String message) {
    LOG.info("Producer Topics: {}", producerTopics);
    try {
      producer.publish(producerTopics, message, null, "");
    } catch (Exception e) {
      LOG.error("Error Occurred: ", e);
    }
    LOG.info("Message Published Successfully");
  }

  @Operation(summary = "publishPayload", description = "publish payload")
  @ApiResponses(value = {
    @ApiResponse(responseCode = "200", description = "Success")
  })
  @PostMapping(produces = MediaType.APPLICATION_JSON_VALUE)
  public String publishTopic(PublishPayloadVO publishPayloadVO, Model model) {
    LOG.info("PublishPayloadVO: {}", publishPayloadVO);
    model.addAttribute("buildProperties", buildProperties);
    try {
      model.addAttribute("topicNames", kafkaMonitor.getTopics().toArray());
      model.addAttribute("topicName", publishPayloadVO.getName());
      model.addAttribute("payloadText", publishPayloadVO.getPayload());
      producer.publish(publishPayloadVO.getName(), publishPayloadVO.getPayload(),
        publishPayloadVO.getPublishOptionalHeaders(), publishPayloadVO.getOptionalHeaders());
      return "publish-payload";
    } catch (Exception e) {
      model.addAttribute("errorMessage", e.getMessage());
      LOG.error("Exception Occurred: ", e);
    } finally {
      LOG.debug("Model: {}", model);
    }
    return "publish-payload";
  }
}
