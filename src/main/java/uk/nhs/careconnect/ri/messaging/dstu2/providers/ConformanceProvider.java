package uk.nhs.careconnect.ri.messaging.dstu2.providers;

import ca.uhn.fhir.context.FhirVersionEnum;
import ca.uhn.fhir.model.primitive.DateTimeDt;
import ca.uhn.fhir.parser.DataFormatException;
import ca.uhn.fhir.rest.annotation.Metadata;
import ca.uhn.fhir.rest.server.RestfulServer;
import ca.uhn.fhir.rest.server.RestulfulServerConfiguration;

import org.hl7.fhir.instance.conf.ServerConformanceProvider;
import org.hl7.fhir.instance.model.Conformance;
import org.hl7.fhir.instance.model.Enumerations;
import org.hl7.fhir.instance.model.api.IPrimitiveType;
import org.springframework.web.context.WebApplicationContext;
import org.springframework.web.context.support.WebApplicationContextUtils;
import uk.nhs.careconnect.ri.messaging.HapiProperties;

import javax.servlet.http.HttpServletRequest;
import java.util.Date;


public class ConformanceProvider extends ServerConformanceProvider {
        private boolean myCache = true;
        private volatile Conformance capabilityStatement;

        private RestulfulServerConfiguration serverConfiguration;

        private RestfulServer restfulServer;

        private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(ConformanceProvider.class);


        public ConformanceProvider() {
            super();
        }

        @Override
        public void setRestfulServer(RestfulServer theRestfulServer) {

            serverConfiguration = theRestfulServer.createConfiguration();
            restfulServer = theRestfulServer;
            super.setRestfulServer(theRestfulServer);
        }

        @Override
        @Metadata
        public Conformance getServerConformance(HttpServletRequest theRequest) {

            WebApplicationContext ctx = WebApplicationContextUtils.getWebApplicationContext(theRequest.getServletContext());
            log.info("restful2 Server not null = {}", HapiProperties.getValidationFlag());



            if (capabilityStatement != null && myCache) {
                return capabilityStatement;
            }
            Conformance capabilityStatement = super.getServerConformance(theRequest);


            capabilityStatement.setPublisher("NHS Digital");
            capabilityStatement.setDate(new Date());
            capabilityStatement.setFhirVersion(FhirVersionEnum.DSTU2_HL7ORG.getFhirVersionString());
            // effort since the parser
            // needs to be modified to actually allow it

            capabilityStatement.getImplementation().setDescription(serverConfiguration.getImplementationDescription());


            capabilityStatement.getSoftware().setName(HapiProperties.getServerName());
            capabilityStatement.getSoftware().setVersion(HapiProperties.getSoftwareVersion());
            capabilityStatement.getImplementation().setDescription(HapiProperties.getSoftwareImplementationDesc());
            capabilityStatement.getImplementation().setUrl(HapiProperties.getSoftwareImplementationUrl());

            capabilityStatement.setStatus(Enumerations.ConformanceResourceStatus.ACTIVE);
            log.trace("restful Server not null");
            return capabilityStatement;
        }

        private DateTimeDt conformanceDate() {
            IPrimitiveType<Date> buildDate = serverConfiguration.getConformanceDate();
            if (buildDate != null) {
                try {
                    return new DateTimeDt(buildDate.getValue());
                } catch (DataFormatException e) {
                    // fall through
                }
            }
            return new DateTimeDt(new Date());
        }


    }

