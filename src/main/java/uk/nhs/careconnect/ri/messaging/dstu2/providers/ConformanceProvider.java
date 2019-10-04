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
            log.info("restful2 Server not null = " + ctx.getEnvironment().getProperty("ccri.validate_flag"));

            String CRUD_update = ctx.getEnvironment().getProperty("ccri.CRUD_update");
            String CRUD_delete = ctx.getEnvironment().getProperty("ccri.CRUD_delete");
            String CRUD_create = ctx.getEnvironment().getProperty("ccri.CRUD_create");
            String CRUD_read = ctx.getEnvironment().getProperty("ccri.CRUD_read");


            String oauth2authorize = ctx.getEnvironment().getProperty("ccri.oauth2.authorize");
            String oauth2token = ctx.getEnvironment().getProperty("ccri.oauth2.token");
            String oauth2register = ctx.getEnvironment().getProperty("ccri.oauth2.register");
            String oauth2 = ctx.getEnvironment().getProperty("ccri.oauth2");

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


            capabilityStatement.getSoftware().setName(System.getProperty("ccri.software.name"));
            capabilityStatement.getSoftware().setVersion(System.getProperty("ccri.software.version"));
            capabilityStatement.getImplementation().setDescription(System.getProperty("ccri.server"));
            capabilityStatement.getImplementation().setUrl(System.getProperty("ccri.server.base"));

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

