package uk.nhs.careconnect.ri.messaging.camel;


import ca.uhn.fhir.context.FhirContext;
import ca.uhn.hl7v2.DefaultHapiContext;
import ca.uhn.hl7v2.HapiContext;
import org.apache.camel.CamelContext;
import org.apache.camel.Exchange;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.hl7.HL7DataFormat;
import org.apache.camel.impl.DefaultCamelContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;
import uk.nhs.careconnect.ri.messaging.HapiProperties;
import uk.nhs.careconnect.ri.messaging.camel.interceptor.GatewayPostProcessor;
import uk.nhs.careconnect.ri.messaging.camel.interceptor.GatewayPreProcessor;
import uk.nhs.careconnect.ri.messaging.camel.processor.BundleMessage;
import uk.nhs.careconnect.ri.messaging.camel.processor.CompositionDocumentBundle;
import uk.nhs.careconnect.ri.messaging.camel.processor.HL7v2A05toFHIRBundle;

import java.io.InputStream;

@Component
public class CamelRoute extends RouteBuilder {

	@Autowired
	protected Environment env;

	public HapiContext hapiContext;


	
    @Override
    public void configure() 
    {
		CamelContext context = new DefaultCamelContext();


		GatewayPreProcessor camelProcessor = new GatewayPreProcessor();

		GatewayPostProcessor camelPostProcessor = new GatewayPostProcessor();

		FhirContext ctx = FhirContext.forDstu3();

		BundleMessage bundleMessage = new BundleMessage(ctx, HapiProperties.getServerBase("epr"), HapiProperties.getServerBase("edms"));
        CompositionDocumentBundle compositionDocumentBundle = new CompositionDocumentBundle(ctx, HapiProperties.getServerBase(), HapiProperties.getServerBase("edms"));

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
			.to(HapiProperties.getCamelRoute("tkw"))
			.process(camelPostProcessor)
			.to("log:uk.nhs.careconnect.FHIRGateway.complete?level=INFO&showHeaders=true&showExchangeId=true")
			.convertBodyTo(InputStream.class);
			
		// EPR Server


		from("direct:EDMSServer")
				.routeId("EDMS FHIR Server")
				.to("log:uk.nhs.careconnect.FHIRGateway.start?level=INFO&showHeaders=true&showExchangeId=true")
				.to(HapiProperties.getCamelRoute("edms"))
				.process(camelPostProcessor)
				.to("log:uk.nhs.careconnect.FHIRGateway.complete?level=INFO&showHeaders=true&showExchangeId=true")
				.convertBodyTo(InputStream.class);


    }
}
