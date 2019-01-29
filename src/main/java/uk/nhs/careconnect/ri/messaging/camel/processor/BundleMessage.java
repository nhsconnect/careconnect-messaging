package uk.nhs.careconnect.ri.messaging.camel.processor;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.parser.IParser;
import ca.uhn.fhir.rest.server.exceptions.UnprocessableEntityException;
import org.apache.camel.CamelContext;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.hl7.fhir.dstu3.model.Bundle;
import org.hl7.fhir.dstu3.model.DocumentReference;
import org.hl7.fhir.dstu3.model.OperationOutcome;
import org.hl7.fhir.dstu3.model.Resource;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.nhs.careconnect.ri.messaging.support.OperationOutcomeException;
import uk.nhs.careconnect.ri.messaging.support.OperationOutcomeFactory;

import java.util.HashMap;
import java.util.Map;

public class BundleMessage implements Processor {

    public BundleMessage(FhirContext ctx, String hapiBase, String edmsBase) {

        this.ctx = ctx;
        this.hapiBase = hapiBase;
        this.edmsBase = edmsBase;
    }

    CamelContext context;

    FhirContext ctx;

    private String hapiBase;

    private String edmsBase;

    private Map<String, Resource> resourceMap;

    private Bundle bundle;

    private static final Logger log = LoggerFactory.getLogger(BundleMessage.class);

    @Override
    public void process(Exchange exchange) throws Exception {
        // Bundles should be in XML format. Previous step should enforce this.

        log.info("Starting Message Bundle Processing");
        this.context = exchange.getContext();

        resourceMap = new HashMap<>();

        String bundleString = exchange.getIn().getBody().toString();

        IParser parser = ctx.newXmlParser();
        IBaseResource iresource = null;
        try {
            iresource = parser.parseResource(bundleString);
        } catch (Exception ex) {
            log.info("Failed to parse: "+bundleString);
            throw ex;
        }
        if (iresource instanceof Bundle) {
            bundle = (Bundle) iresource;
        } else if (iresource instanceof OperationOutcome) {
            String responseCode = exchange.getIn().getHeader(Exchange.HTTP_RESPONSE_CODE).toString();
            log.error("Server Returned: Status Code = " +responseCode + " "+ ctx.newJsonParser().encodeResourceToString(iresource));
            OperationOutcomeFactory.convertToException((OperationOutcome) iresource);
        } else {
            throw new UnprocessableEntityException(bundleString);

        }
        BundleCore bundleCore = new BundleCore(ctx,context,bundle, hapiBase, edmsBase);
        try {


            // Process resources
            for (Bundle.BundleEntryComponent entry : bundle.getEntry()) {
                Resource resource = entry.getResource();

                // Look for existing resources. Ideally we should not need to add Patient, Practitioner, Organization, etc
                // These should be using well known identifiers and ideally will be present on our system.

                if (resource.getId() != null) {
                    Resource resourceSearch = bundleCore.searchAddResource(resource.getId());
                    if (resourceSearch instanceof OperationOutcome) {
                        setExchange(exchange,(OperationOutcome) resourceSearch);
                    }
                } else {
                    resource.setId(java.util.UUID.randomUUID().toString());
                    Resource resourceSearch = bundleCore.searchAddResource(resource.getId());
                    if (resourceSearch instanceof OperationOutcome) {
                        setExchange(exchange,(OperationOutcome) resourceSearch);
                    }
                }
                if (resource instanceof DocumentReference) {
                    log.debug("Document Reference Location " + resource.getId());
                    exchange.getIn().setHeader("Location",resource.getId());
                    exchange.getIn().setHeader("Content-Location",resource.getId());
                }

            }
            exchange.getIn().setBody(ctx.newXmlParser().encodeResourceToString(bundleCore.getUpdatedBundle()));
            //log.info(ctx.newXmlParser().encodeResourceToString(bundleCore.getBundle()));

        }
        catch (OperationOutcomeException ex) {
            log.error("BundleMessage Exception OperationExchange");
            exchange.getIn().setHeader(Exchange.HTTP_RESPONSE_CODE,"400");
            exchange.getIn().setBody(ctx.newXmlParser().encodeResourceToString(ex.getOutcome()));
        }
        catch (Exception ex) {
            // A number of the HAPI related function will return exceptions.
            // Convert to operational outcomes
            String errorMessage;
            if (ex.getMessage()!= null) {
                errorMessage = ex.getMessage();
            } else {
                errorMessage = "BundleMessage Exception "+ex.getClass().getSimpleName();
            }
            if (ex.getStackTrace().length >0) {
                errorMessage = errorMessage + " (Line: "+ex.getStackTrace()[0].getLineNumber() + " Method: " + ex.getStackTrace()[0].getMethodName() + " " + ex.getStackTrace()[0].getClassName() + ")";
            }
            log.error(errorMessage);
            OperationOutcome operationOutcome = null;
            if (bundleCore != null && bundleCore.getOperationOutcome() != null) {
                operationOutcome = bundleCore.getOperationOutcome();
            } else {
                operationOutcome=new OperationOutcome();
                OperationOutcome.IssueType issueType = OperationOutcomeFactory.getIssueType(ex);

                operationOutcome.addIssue()
                        .setSeverity(OperationOutcome.IssueSeverity.ERROR)
                        .setCode(issueType)
                        .setDiagnostics(errorMessage);
            }

            setExchange(exchange,operationOutcome);
        }
        log.debug("Finishing Message Bundle Processing");

    }

    private void setExchange(Exchange exchange, OperationOutcome operationOutcome) {
        exchange.getIn().setHeader(Exchange.HTTP_RESPONSE_CODE,"400");
        exchange.getIn().setBody(ctx.newXmlParser().encodeResourceToString(operationOutcome));
    }



}
