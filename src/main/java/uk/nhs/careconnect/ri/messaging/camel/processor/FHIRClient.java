package uk.nhs.careconnect.ri.messaging.camel.processor;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.rest.client.api.IGenericClient;
import ca.uhn.fhir.rest.server.exceptions.InternalErrorException;
import org.apache.camel.Exchange;
import org.apache.camel.ExchangePattern;
import org.apache.camel.Processor;
import org.apache.camel.ProducerTemplate;
import org.hl7.fhir.dstu3.model.Binary;
import org.hl7.fhir.dstu3.model.Bundle;
import org.hl7.fhir.dstu3.model.DocumentReference;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class FHIRClient implements Processor {

    /// LEAVE FOR NOW BUT CODE MOVED TO BundleCore

    // SCHEDULED FOR DELETION

    private FhirContext ctx;

    private String clientBase;

    private Bundle bundle;

    private Exchange exchange;

    IGenericClient client = null;

    public FHIRClient(FhirContext ctx, String clientBase) {
        this.ctx = ctx;
        this.clientBase = clientBase;

        client = this.ctx.newRestfulGenericClient(clientBase);
    }
    private static final Logger log = LoggerFactory.getLogger(FHIRClient.class);

    @Override
    public void process(Exchange exchange) throws Exception {
        this.exchange = exchange;
        IBaseResource resource = null;
        if (exchange.getIn().getBody() != null) {
            resource = ctx.newXmlParser().parseResource((String) exchange.getIn().getBody());
        }
        switch (exchange.getIn().getHeader(Exchange.HTTP_METHOD).toString()) {
            case "GET":
                log.info("GET");
                break;
            case "PUT":
                log.info("PUT");
                break;
            case "POST":
                log.info("POST");
                break;

        }



    }

}
