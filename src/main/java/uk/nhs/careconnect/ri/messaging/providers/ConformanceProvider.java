package uk.nhs.careconnect.ri.messaging.providers;

import ca.uhn.fhir.context.FhirVersionEnum;
import ca.uhn.fhir.parser.DataFormatException;
import ca.uhn.fhir.rest.annotation.Metadata;
import ca.uhn.fhir.rest.server.IResourceProvider;
import ca.uhn.fhir.rest.server.RestfulServer;
import ca.uhn.fhir.rest.server.RestulfulServerConfiguration;
import org.hl7.fhir.dstu3.hapi.rest.server.ServerCapabilityStatementProvider;
import org.hl7.fhir.dstu3.model.*;
import org.hl7.fhir.instance.model.api.IPrimitiveType;

import javax.servlet.http.HttpServletRequest;
import java.util.Date;


public class ConformanceProvider extends ServerCapabilityStatementProvider {

    private boolean myCache = true;
    private volatile CapabilityStatement capabilityStatement;

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
    public CapabilityStatement getServerConformance(HttpServletRequest theRequest) {
        if (capabilityStatement != null && myCache) {
            return capabilityStatement;
        }
        CapabilityStatement capabilityStatement = super.getServerConformance(theRequest);


        capabilityStatement.setPublisher("NHS Digital");
        capabilityStatement.setDateElement(conformanceDate());
        capabilityStatement.setFhirVersion(FhirVersionEnum.DSTU3.getFhirVersionString());
        capabilityStatement.setAcceptUnknown(CapabilityStatement.UnknownContentCode.EXTENSIONS); // TODO: make this configurable - this is a fairly big
        // effort since the parser
        // needs to be modified to actually allow it

        capabilityStatement.getImplementation().setDescription(serverConfiguration.getImplementationDescription());
        capabilityStatement.setKind(CapabilityStatement.CapabilityStatementKind.INSTANCE);


        capabilityStatement.getSoftware().setName(System.getProperty("ccri.software.name"));
        capabilityStatement.getSoftware().setVersion(System.getProperty("ccri.software.version"));
        capabilityStatement.getImplementation().setDescription(System.getProperty("ccri.server"));
        capabilityStatement.getImplementation().setUrl(System.getProperty("ccri.server.base"));
        // TODO KGM move to config
        capabilityStatement.getImplementationGuide().add(new UriType(System.getProperty("ccri.guide")));

        capabilityStatement.setStatus(Enumerations.PublicationStatus.ACTIVE);
        /*
        if (serverConfiguration != null) {
            for (ResourceBinding resourceBinding : serverConfiguration.getResourceBindings()) {
                log.info("resourceBinding.getResourceName() = "+resourceBinding.getResourceName());
                log.info("resourceBinding.getMethodBindings().size() = "+resourceBinding.getMethodBindings().size());
            }
        }
        */
        if (restfulServer != null) {
            log.trace("restful Server not null");
            for (CapabilityStatement.CapabilityStatementRestComponent nextRest : capabilityStatement.getRest()) {
                for (CapabilityStatement.CapabilityStatementRestResourceComponent restResourceComponent : nextRest.getResource()) {
                    log.trace("restResourceComponent.getType - " + restResourceComponent.getType());
                   for (IResourceProvider provider : restfulServer.getResourceProviders()) {

                        log.trace("Provider Resource - " + provider.getResourceType().getSimpleName());
                        if (restResourceComponent.getType().equals(provider.getResourceType().getSimpleName())
                                || (restResourceComponent.getType().contains("List") && provider.getResourceType().getSimpleName().contains("List")))
                            if (provider instanceof ICCResourceProvider) {
                                log.trace("ICCResourceProvider - " + provider.getClass());
                                ICCResourceProvider resourceProvider = (ICCResourceProvider) provider;

                                Extension extension = restResourceComponent.getExtensionFirstRep();
                                if (extension == null) {
                                    extension = restResourceComponent.addExtension();
                                }
                                extension.setUrl("http://hl7api.sourceforge.net/hapi-fhir/res/extdefs.html#resourceCount")
                                        .setValue(new DecimalType(resourceProvider.count()));
                            }
                    }
                }
            }
        }


        return capabilityStatement;
    }

    private DateTimeType conformanceDate() {
        IPrimitiveType<Date> buildDate = serverConfiguration.getConformanceDate();
        if (buildDate != null) {
            try {
                return new DateTimeType(buildDate.getValue());
            } catch (DataFormatException e) {
                // fall through
            }
        }
        return DateTimeType.now();
    }


}
