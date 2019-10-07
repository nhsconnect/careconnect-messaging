package uk.nhs.careconnect.ri.messaging.dstu2.providers;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.rest.annotation.*;
import ca.uhn.fhir.rest.api.MethodOutcome;
import ca.uhn.fhir.rest.api.server.RequestDetails;
import ca.uhn.fhir.rest.server.IResourceProvider;
import ca.uhn.fhir.rest.server.exceptions.BaseServerResponseException;
import ca.uhn.fhir.rest.server.exceptions.InternalErrorException;
import org.apache.camel.*;
import org.hl7.fhir.instance.model.Bundle;
import org.hl7.fhir.instance.model.IdType;
import org.hl7.fhir.instance.model.OperationOutcome;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import uk.nhs.careconnect.ri.messaging.support.CareConnectDSTU2toSTU3;
import uk.nhs.careconnect.ri.messaging.support.ProviderResponseLibrary;

import javax.servlet.http.HttpServletRequest;
import java.io.InputStream;

@Component
public class BundleResourceProviderDSTU2 implements IResourceProvider {

    @Autowired
    CamelContext context;

    @Qualifier("CTXDSTU2")
    @Autowired()
    FhirContext ctx;

    @Qualifier("CTXR3")
    @Autowired()
    FhirContext ctxR3;

    @Autowired
    CareConnectDSTU2toSTU3 converterSTU3;

    private static final Logger log = LoggerFactory.getLogger(BundleResourceProviderDSTU2.class);


    @Override
    public Class<Bundle> getResourceType() {
        return Bundle.class;
    }


    private Exchange buildBundlePost(Exchange exchange, String newXmlResource, String query, String method) {
        exchange.getIn().setBody(newXmlResource);
        exchange.getIn().setHeader(Exchange.HTTP_QUERY, query);
        exchange.getIn().setHeader(Exchange.HTTP_METHOD, method);
        exchange.getIn().setHeader(Exchange.HTTP_PATH, "Bundle");
        // exchange.getIn().setHeader("Prefer", "return=representation");
        exchange.getIn().setHeader(Exchange.CONTENT_TYPE, "application/fhir+xml");
        return exchange;
    }


    @Create
    public MethodOutcome create(HttpServletRequest httpRequest, @ResourceParam Bundle bundleDSTU2) throws Exception {


        ProducerTemplate template = context.createProducerTemplate();

        IBaseResource resource = null;
        org.hl7.fhir.dstu3.model.Bundle bundle = null;


        try {
            InputStream inputStream = null;

            org.hl7.fhir.dstu3.model.Resource resourceR3 = converterSTU3.convert(bundleDSTU2);

            String newXmlResource = ctxR3.newXmlParser().encodeResourceToString(resourceR3);

            if (resourceR3 instanceof org.hl7.fhir.dstu3.model.Bundle) {

                bundle = (org.hl7.fhir.dstu3.model.Bundle) resourceR3;

                if (bundle.hasMeta() && bundle.getMeta().hasProfile("https://fhir.nhs.uk/STU3/StructureDefinition/ITK-Message-Bundle-1")) {
                    throw new InternalErrorException("This server does not know how to process transfer of care message Bundle."
                            + " Please remove the transport layer and submit the payload only");
                }

                switch (bundle.getType()) {


                    case COLLECTION:
                    case MESSAGE:
                        // Sync to get response

                        // ASync This uses a queue direct:FHIRBundleCollection
                        // Sync Direct flow direct:FHIRBundleMessage
                        Exchange exchangeMessage = template.send("direct:FHIRBundleMessage", ExchangePattern.InOut, new Processor() {
                            public void process(Exchange exchange) throws Exception {
                                exchange = buildBundlePost(exchange, newXmlResource, null, "POST");

                            }
                        });
                        resource = ProviderResponseLibrary.processMessageBody(ctx, resource, exchangeMessage.getIn().getBody());
                        break;

                    case DOCUMENT:
                        // Send a copy for EPR processing - Consider moving to camel route

                        // Main Message send to EDMS
                        Exchange exchangeDocument = template.send("direct:FHIRBundleDocument", ExchangePattern.InOut, new Processor() {
                            public void process(Exchange exchange) throws Exception {
                                exchange = buildBundlePost(exchange, newXmlResource, null, "POST");

                            }
                        });
                        resource = ProviderResponseLibrary.processMessageBody(ctx, resource, exchangeDocument.getIn().getBody());

                    default:
                        // TODO
                }
            }
            if (resource instanceof Bundle) {
               // bundle = (org.hl7.fhir.dstu3.model.Bundle) resource;
            } else {

                ProviderResponseLibrary.createException(ctx, resource);
            }
        }
        catch (BaseServerResponseException baseException) {
            throw baseException;
        }
        catch (Exception ex) {
            log.error("XML Parse failed {}", ex.getMessage());
            throw new InternalErrorException(ex.getMessage());
        }
        log.trace("RETURNED Resource {}", resource.getClass().getSimpleName());


        MethodOutcome method = new MethodOutcome();

        ProviderResponseLibrary.setMethodOutcome(new OperationOutcome(), method);

        return method;
    }


    @Update
    public MethodOutcome updateBundle(HttpServletRequest theRequest, @ResourceParam Bundle bundle, @IdParam IdType bundleId, @ConditionalUrlParam String conditional, RequestDetails theRequestDetails) throws Exception {

        ProducerTemplate template = context.createProducerTemplate();

        IBaseResource resource = null;
        try {
            InputStream inputStream = null;
            String newXmlResource = ctx.newXmlParser().encodeResourceToString(bundle);

            if (bundle.hasMeta() && bundle.getMeta().hasProfile("https://fhir.nhs.uk/STU3/StructureDefinition/ITK-Message-Bundle-1")) {
                throw new InternalErrorException("This server does not know how to process transfer of care message Bundle."
                        + " Please remove the transport layer and submit the payload only");
            }

            switch (bundle.getType()) {
                case COLLECTION:
                case MESSAGE:

                    Exchange exchangeBundle = template.send("direct:FHIRBundleCollection", ExchangePattern.InOut, new Processor() {
                        public void process(Exchange exchange) throws Exception {
                            exchange = buildBundlePost(exchange, newXmlResource, conditional, "PUT");

                        }
                    });
                    // TODO need proper responses from the camel processor. KGM 18/Apr/2018
                    resource = ProviderResponseLibrary.processMessageBody(ctx, resource, exchangeBundle.getIn().getBody());
                    break;

                case DOCUMENT:
                    Exchange exchangeDocument = template.send("direct:FHIRBundleDocument", ExchangePattern.InOut, new Processor() {
                        public void process(Exchange exchange) throws Exception {
                            exchange = buildBundlePost(exchange, newXmlResource, conditional, "PUT");
                        }
                    });
                    // TODO need proper responses from the camel processor. KGM 18/Apr/2018

                    // This response is coming from an external FHIR Server, so uses inputstream
                    resource = ProviderResponseLibrary.processMessageBody(ctx, resource, exchangeDocument.getIn().getBody());


                default:
                    // TODO
            }
        }
        catch (BaseServerResponseException baseException) {
            throw baseException;
        }
        catch (Exception ex) {
            log.error("XML Parse failed {}", ex.getMessage());
            throw new InternalErrorException(ex.getMessage());
        }
        log.trace("RETURNED Resource {}", resource.getClass().getSimpleName());
        if (resource instanceof Bundle) {
            bundle = (Bundle) resource;
        } else {
            ProviderResponseLibrary.createException(ctx, resource);
        }


        MethodOutcome method = new MethodOutcome();

        ProviderResponseLibrary.setMethodOutcome(new OperationOutcome(), method);


        return method;
    }


}
