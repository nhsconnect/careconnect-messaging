package uk.nhs.careconnect.ri.messaging;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.context.FhirVersionEnum;
import ca.uhn.fhir.rest.api.EncodingEnum;
import ca.uhn.fhir.rest.server.FifoMemoryPagingProvider;
import ca.uhn.fhir.rest.server.HardcodedServerAddressStrategy;
import ca.uhn.fhir.rest.server.IResourceProvider;
import ca.uhn.fhir.rest.server.RestfulServer;
import ca.uhn.fhir.rest.server.interceptor.CorsInterceptor;
import ca.uhn.fhir.util.VersionUtil;
import org.springframework.context.ApplicationContext;
import org.springframework.web.cors.CorsConfiguration;
import uk.nhs.careconnect.ri.messaging.dstu2.providers.ConformanceProvider;
import uk.nhs.careconnect.ri.messaging.support.ServerInterceptor;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServletResponse;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.TimeZone;

@WebServlet(urlPatterns = { "/*" }, displayName = "FHIR Server")
public class CustomRestfulServerDSTU2 extends RestfulServer {

	private static final long serialVersionUID = 1L;
	private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(CustomRestfulServerDSTU2.class);

	private ApplicationContext applicationContext;

	CustomRestfulServerDSTU2(ApplicationContext context) {
		this.applicationContext = context;
	}


	

    @Override
	public void addHeadersToResponse(HttpServletResponse theHttpResponse) {
		theHttpResponse.addHeader("X-Powered-By", "HAPI FHIR " + VersionUtil.getVersion() + " RESTful Server (INTEROPen Care Connect STU3)");
	}

	@SuppressWarnings("unchecked")
	@Override
	protected void initialize() throws ServletException {
		super.initialize();
		TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
		String ccri_role =  HapiProperties.getServerRole();

		String resources =  this.applicationContext.getEnvironment().getProperty("ccri.Messaging_resources");
	   // List<String>  Messaging_resources = resources.split(",");
	    List<String> Messaging_resources = Arrays.asList(resources.split("\\s*,\\s*"));

		FhirVersionEnum fhirVersion = FhirVersionEnum.DSTU2_HL7ORG;
		setFhirContext(new FhirContext(fhirVersion));

		String serverBase = HapiProperties.getServerBase();
	     if (serverBase != null && !serverBase.isEmpty()) {
            setServerAddressStrategy(new HardcodedServerAddressStrategy(serverBase));
        }
	     List<String> permissions = null;
	     switch(ccri_role)
	        {
	            case "Messaging" :
	                permissions = Messaging_resources;
	                break;
	           
	        }

	     
	     Class<?> classType = null;
	        log.info("Resource count " + permissions.size());

	        List<IResourceProvider> permissionlist = new ArrayList<>();
	        for (String permission : permissions) {
	            try {
	                classType = Class.forName("uk.nhs.careconnect.ri.messaging.dstu2.providers." + permission + "ResourceProviderDSTU2");
	                log.info("class methods " + classType.getMethods()[4].getName() );
	            } catch (ClassNotFoundException  e) {
	                // TODO Auto-generated catch block
	                e.printStackTrace();
	            }
	            System.out.println(permission);
	            permissionlist.add((IResourceProvider) applicationContext.getBean(classType));
	        }



	        setResourceProviders(permissionlist);
	     
		// Replace built in conformance provider (CapabilityStatement)
		setServerConformanceProvider(new ConformanceProvider());

		setServerName(HapiProperties.getServerName());
		setServerVersion(HapiProperties.getSoftwareVersion());
		setImplementationDescription(HapiProperties.getServerName());


		CorsConfiguration config = new CorsConfiguration();
		config.addAllowedHeader("x-fhir-starter");
		config.addAllowedHeader("Origin");
		config.addAllowedHeader("Accept");
		config.addAllowedHeader("X-Requested-With");
		config.addAllowedHeader("Content-Type");

		config.addAllowedOrigin("*");

		config.addExposedHeader("Location");
		config.addExposedHeader("Content-Location");
		config.setAllowedMethods(Arrays.asList("GET", "POST", "PUT", "DELETE", "OPTIONS", "PATCH"));

		// Create the interceptor and register it
		CorsInterceptor interceptor = new CorsInterceptor(config);
		registerInterceptor(interceptor);

		ServerInterceptor loggingInterceptor = new ServerInterceptor(log);
		registerInterceptor(loggingInterceptor);



		FifoMemoryPagingProvider pp = new FifoMemoryPagingProvider(10);
		pp.setDefaultPageSize(10);
		pp.setMaximumPageSize(100);
		setPagingProvider(pp);

		setDefaultPrettyPrint(true);
		setDefaultResponseEncoding(EncodingEnum.JSON);

		FhirContext ctx = getFhirContext();
		// Remove as believe due to issues on docker ctx.setNarrativeGenerator(new DefaultThymeleafNarrativeGenerator());
	}




}
