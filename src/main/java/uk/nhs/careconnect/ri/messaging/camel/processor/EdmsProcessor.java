package uk.nhs.careconnect.ri.messaging.camel.processor;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.rest.api.MethodOutcome;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import uk.nhs.careconnect.ri.messaging.HapiProperties;

public class EdmsProcessor implements Processor {

    FhirContext ctx;

    public EdmsProcessor(FhirContext ctx) {
        this.ctx = ctx;
    }

    @Override
    public void process(Exchange exchange) throws Exception {
        if (exchange.getIn().getBody() instanceof MethodOutcome) {
            MethodOutcome methodOutcome = (MethodOutcome)exchange.getIn().getBody();
            String[] locations = methodOutcome.getId().toString().split("/");
            if (locations.length>0) {
                String location = HapiProperties.getServerBase("edms") + "/Binary/" + locations[locations.length - 1];
                exchange.getIn().setHeader("Location", location);
            }
            if (methodOutcome.getCreated()) {
                exchange.getIn().setHeader(Exchange.HTTP_RESPONSE_CODE,"201");
            }
            exchange.getIn().setBody("");
        }
    }
}
