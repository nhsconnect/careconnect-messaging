package uk.nhs.careconnect.ri.messaging.camel;


import ca.uhn.fhir.context.FhirContext;
import ca.uhn.hl7v2.DefaultHapiContext;
import ca.uhn.hl7v2.HapiContext;
import org.apache.camel.Exchange;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.hl7.HL7DataFormat;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;
import uk.nhs.careconnect.ri.messaging.camel.interceptor.GatewayPostProcessor;
import uk.nhs.careconnect.ri.messaging.camel.interceptor.GatewayPreProcessor;
import uk.nhs.careconnect.ri.messaging.camel.processor.BundleMessage;
import uk.nhs.careconnect.ri.messaging.camel.processor.CompositionDocumentBundle;
import uk.nhs.careconnect.ri.messaging.camel.processor.FHIRClient;
import uk.nhs.careconnect.ri.messaging.camel.processor.HL7v2A05toFHIRBundle;

import java.io.InputStream;

@Component
public class CamelRoute extends RouteBuilder {

	@Autowired
	protected Environment env;

	public HapiContext hapiContext;

	@Value("${fhir.restserver.edmsBase}")
	private String edmsBase;

	@Value("${fhir.restserver.tkwBase}")
	private String tkwBase;
	
	@Value("${ccri.server.base}")
    private String messageBase;

	@Value("${ccri.edms.server.base}")
	private String edmsBaseFHIR;

	@Value("${ccri.epr.server.base}")
	private String eprBaseFHIR;

	
    @Override
    public void configure() 
    {

		GatewayPreProcessor camelProcessor = new GatewayPreProcessor();

		GatewayPostProcessor camelPostProcessor = new GatewayPostProcessor();



		FhirContext ctx = FhirContext.forDstu3();

		//FHIRClient eprClient = new FHIRClient(ctx,eprBase);

		BundleMessage bundleMessage = new BundleMessage(ctx, eprBaseFHIR, edmsBaseFHIR);
        CompositionDocumentBundle compositionDocumentBundle = new CompositionDocumentBundle(ctx, messageBase, edmsBaseFHIR);
        //DocumentReferenceDocumentBundle documentReferenceDocumentBundle = new DocumentReferenceDocumentBundle(ctx,hapiBase);
       // BinaryResource binaryResource = new BinaryResource(ctx, hapiBase);

		hapiContext = new DefaultHapiContext();

		hapiContext.getParserConfiguration().setValidating(false);
		HL7DataFormat hl7 = new HL7DataFormat();

		HL7v2A05toFHIRBundle hl7v2A05toFHIRBundle = new HL7v2A05toFHIRBundle(hapiContext);

		hl7.setHapiContext(hapiContext);


		from("direct:FHIRValidate")
				.routeId("FHIR Validation")
				.process(camelProcessor) // Add in correlation Id if not present
				.to("direct:TKWServer");


		// Complex processing

		// This bundle goes to the EDMS Server. See also Binary
		from("direct:FHIRBundleDocument")
				.routeId("Bundle Document")
				.process(camelProcessor) // Add in correlation Id if not present
				.enrich("direct:EDMSServer", compositionDocumentBundle)
				.choice()
					.when(header(Exchange.HTTP_RESPONSE_CODE).isEqualTo("200") ).enrich("direct:FHIRBundleMessage")
					.when(header(Exchange.HTTP_RESPONSE_CODE).isEqualTo("201") ).enrich("direct:FHIRBundleMessage")
				.end(); // Send a copy to EPR for main CCRI load

		from("direct:FHIRBundleMessage")
				.routeId("Bundle Message Processing")
				.process(bundleMessage); // Goes direct to EPR FHIR Server


	// Integration Server (TIE)

		from("direct:FHIREncounterDocument")
				.routeId("TIE Encounter")
				.to("direct:TIEServer");

		from("direct:FHIRCarePlanDocument")
				.routeId("TIE CarePlan")
				.to("direct:TIEServer");

		from("direct:FHIRPatientOperation")
				.routeId("TIE PatientOperation")
				.to("direct:TIEServer");


		from("direct:TKWServer")
			.routeId("TKW FHIR Server")
			.process(camelProcessor)
			.to("log:uk.nhs.careconnect.FHIRGateway.start?level=INFO&showHeaders=true&showExchangeId=true")
			.to(tkwBase)
			.process(camelPostProcessor)
			.to("log:uk.nhs.careconnect.FHIRGateway.complete?level=INFO&showHeaders=true&showExchangeId=true")
			.convertBodyTo(InputStream.class);
			
		// EPR Server




		from("direct:EDMSServer")
				.routeId("EDMS FHIR Server")
				.to("log:uk.nhs.careconnect.FHIRGateway.start?level=INFO&showHeaders=true&showExchangeId=true")
				.to(edmsBase)
				.process(camelPostProcessor)
				.to("log:uk.nhs.careconnect.FHIRGateway.complete?level=INFO&showHeaders=true&showExchangeId=true")
				.convertBodyTo(InputStream.class);

		/*
		from("direct:EPRServer")
            .routeId("EPR FHIR Server")
				.process(camelProcessor)
				.to("log:uk.nhs.careconnect.FHIRGateway.start?level=INFO&showHeaders=true&showExchangeId=true")
				.process(eprClient)
                //.to(eprBase)
				.process(camelPostProcessor)
                .to("log:uk.nhs.careconnect.FHIRGateway.complete?level=INFO&showHeaders=true&showExchangeId=true")
				.convertBodyTo(InputStream.class);
*/
    }
}
