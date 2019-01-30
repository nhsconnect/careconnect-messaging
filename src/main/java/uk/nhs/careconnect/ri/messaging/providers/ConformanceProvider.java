package uk.nhs.careconnect.ri.messaging.providers;

import ca.uhn.fhir.context.FhirVersionEnum;
import ca.uhn.fhir.parser.DataFormatException;
import ca.uhn.fhir.rest.annotation.Metadata;
import ca.uhn.fhir.rest.server.IResourceProvider;
import ca.uhn.fhir.rest.server.RestfulServer;
import ca.uhn.fhir.rest.server.RestulfulServerConfiguration;
import org.hl7.fhir.dstu3.hapi.rest.server.ServerCapabilityStatementProvider;
import org.hl7.fhir.dstu3.model.*;
import org.hl7.fhir.dstu3.model.CapabilityStatement.ResourceInteractionComponent;
import org.hl7.fhir.instance.model.api.IPrimitiveType;
import org.springframework.web.context.WebApplicationContext;
import org.springframework.web.context.support.WebApplicationContextUtils;

import javax.servlet.http.HttpServletRequest;
import java.util.Date;
import java.util.List;


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
    	
    	WebApplicationContext ctx = WebApplicationContextUtils.getWebApplicationContext(theRequest.getServletContext());
    	log.info("restful2 Server not null = " + ctx.getEnvironment().getProperty("ccri.validate_flag"));
    	 
        String CRUD_update =  ctx.getEnvironment().getProperty("ccri.CRUD_update");
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

        if (capabilityStatement.getImplementationGuide().size() == 0) {
            capabilityStatement.getImplementationGuide().add(new UriType(System.getProperty("ccri.guide")));
            capabilityStatement.setPublisher("NHS Digital");
        }


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
            log.info("restful Server not null");
            for (CapabilityStatement.CapabilityStatementRestComponent nextRest : capabilityStatement.getRest()) {
                nextRest.setMode(CapabilityStatement.RestfulCapabilityMode.SERVER);

                // KGM only add if not already present
                if (nextRest.getSecurity().getService().size() == 0 && oauth2.equals("true")) {
                    if (oauth2token != null && oauth2register != null && oauth2authorize != null) {
                        nextRest.getSecurity()
                                .addService().addCoding()
                                .setSystem("http://hl7.org/fhir/restful-security-service")
                                .setDisplay("SMART-on-FHIR")
                                .setSystem("SMART-on-FHIR");
                        Extension securityExtension = nextRest.getSecurity().addExtension()
                                .setUrl("http://fhir-registry.smarthealthit.org/StructureDefinition/oauth-uris");

                        securityExtension.addExtension()
                                .setUrl("authorize")
                                .setValue(new UriType(oauth2authorize));

                        securityExtension.addExtension()
                                .setUrl("register")
                                .setValue(new UriType(oauth2register));

                        securityExtension.addExtension()
                                .setUrl("token")
                                .setValue(new UriType(oauth2token));
                    }
                }
            }
            log.trace("restful Server not null");
            for (CapabilityStatement.CapabilityStatementRestComponent nextRest : capabilityStatement.getRest()) {
                for (CapabilityStatement.CapabilityStatementRestResourceComponent restResourceComponent : nextRest.getResource()) {

                    if (restResourceComponent.getType().equals("OperationDefinition")) {
                        nextRest.getResource().remove(restResourceComponent);
                        break;
                    }
                    if (restResourceComponent.getType().equals("StructureDefinition")) {
                        nextRest.getResource().remove(restResourceComponent);
                        break;
                    }
                	// Start of CRUD operations
               	 List<ResourceInteractionComponent> l = restResourceComponent.getInteraction();
                    for(int i=0;i<l.size();i++)
                    	if(CRUD_read.equals("false"))
                    	if (restResourceComponent.getInteraction().get(i).getCode().toString()=="READ")
                    	{
                    		restResourceComponent.getInteraction().remove(i);
                    	}	
                    for(int i=0;i<l.size();i++)
                    	if(CRUD_update.equals("false"))
                    	if (restResourceComponent.getInteraction().get(i).getCode().toString()=="UPDATE")
                    	{
                    		restResourceComponent.getInteraction().remove(i);
                    	}	
                    for(int i=0;i<l.size();i++)
                    	if(CRUD_create.equals("false"))
                    	if (restResourceComponent.getInteraction().get(i).getCode().toString()=="CREATE")
                    	{
                    		restResourceComponent.getInteraction().remove(i);
                    	}	
                    for(int i=0;i<l.size();i++)
                    	if(CRUD_delete.equals("false"))
                    	if (restResourceComponent.getInteraction().get(i).getCode().toString()=="DELETE")
                    	{
                    		restResourceComponent.getInteraction().remove(i);
                    	}	
                    // End of CRUD operations
                    
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
