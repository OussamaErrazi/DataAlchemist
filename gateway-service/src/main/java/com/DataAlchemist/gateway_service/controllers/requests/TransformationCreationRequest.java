package com.DataAlchemist.gateway_service.controllers.requests;


import lombok.*;

import java.util.List;

@Getter
@Setter
@ToString
@NoArgsConstructor
@AllArgsConstructor
public class TransformationCreationRequest {
    private Long transformationId;
    private List<String> pipeline;
    private String dataURL;
    private boolean isLocal;
    private String authToken;
}
