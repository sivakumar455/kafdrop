package kafdrop.model;

import lombok.Data;
import lombok.RequiredArgsConstructor;

@Data
@RequiredArgsConstructor
public final class PublishPayloadVO {
  String name;
  String publishOptionalHeaders;
  String optionalHeaders;
  String payload;
}
