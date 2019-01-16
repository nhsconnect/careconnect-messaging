package uk.nhs.careconnect.ri.messaging;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.context.FhirVersionEnum;
import ca.uhn.fhir.rest.api.EncodingEnum;
import ca.uhn.fhir.rest.server.FifoMemoryPagingProvider;
import ca.uhn.fhir.rest.server.HardcodedServerAddressStrategy;
import ca.uhn.fhir.rest.server.RestfulServer;
import ca.uhn.fhir.rest.server.interceptor.CorsInterceptor;
import ca.uhn.fhir.rest.server.interceptor.IServerInterceptor;
import ca.uhn.fhir.rest.server.interceptor.ResponseHighlighterInterceptor;
import ca.uhn.fhir.util.VersionUtil;

import org.springframework.beans.factory.annotation.Autowire;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.web.cors.CorsConfiguration;
import uk.nhs.careconnect.ri.messaging.providers.ConformanceProvider;
import uk.nhs.careconnect.ri.messaging.providers.PatientResourceProvider;
import uk.nhs.careconnect.ri.messaging.support.ServerInterceptor;
import uk.nhs.careconnect.ri.messaging.providers.BundleResourceProvider;



import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServletResponse;
import java.util.Arrays;
import java.util.TimeZone;

@WebServlet(urlPatterns = { "/*" }, displayName = "FHIR Server")
public class CcriMessagingHAPIConfig extends RestfulServer {

	private static final long serialVersionUID = 1L;
	private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(CcriMessagingHAPIConfig.class);

	private ApplicationContext applicationContext;

	CcriMessagingHAPIConfig(ApplicationContext context) {
		this.applicationContext = context;
	}

	@Value("${ccri.software.name}")
	private String softwareName;

	@Value("${ccri.software.version}")
	private String softwareVersion;

	@Value("${ccri.server}")
	private String server;

	@Value("${ccri.server.base}")
	private String serverBase;


    @Override
	public void addHeadersToResponse(HttpServletResponse theHttpResponse) {
		theHttpResponse.addHeader("X-Powered-By", "HAPI FHIR " + VersionUtil.getVersion() + " RESTful Server (INTEROPen Care Connect STU3)");
	}

	@SuppressWarnings("unchecked")
	@Override
	protected void initialize() throws ServletException {
		super.initialize();
		TimeZone.setDefault(TimeZone.getTimeZone("UTC"));


		FhirVersionEnum fhirVersion = FhirVersionEnum.DSTU3;
		setFhirContext(new FhirContext(fhirVersion));

	     if (serverBase != null && !serverBase.isEmpty()) {
            setServerAddressStrategy(new HardcodedServerAddressStrategy(serverBase));
        }

        if (applicationContext == null ) log.info("Context is null");

		setResourceProviders(Arrays.asList(

				applicationContext.getBean(BundleResourceProvider.class),
				applicationContext.getBean(PatientResourceProvider.class)
		));

		// Replace built in conformance provider (CapabilityStatement)
		setServerConformanceProvider(new ConformanceProvider());

		setServerName(softwareName);
		setServerVersion(softwareVersion);
		setImplementationDescription(server);


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

		ServerInterceptor gatewayInterceptor = new ServerInterceptor(log);
		registerInterceptor(gatewayInterceptor);

		FifoMemoryPagingProvider pp = new FifoMemoryPagingProvider(10);
		pp.setDefaultPageSize(10);
		pp.setMaximumPageSize(100);
		setPagingProvider(pp);

		setDefaultPrettyPrint(true);
		setDefaultResponseEncoding(EncodingEnum.JSON);

		FhirContext ctx = getFhirContext();
		// Remove as believe due to issues on docker ctx.setNarrativeGenerator(new DefaultThymeleafNarrativeGenerator());
	}

	/**
	 * This interceptor adds some pretty syntax highlighting in responses when a browser is detected
	 */
	@Bean(autowire = Autowire.BY_TYPE)
	public IServerInterceptor responseHighlighterInterceptor() {
		ResponseHighlighterInterceptor retVal = new ResponseHighlighterInterceptor();
		return retVal;
	}



}
