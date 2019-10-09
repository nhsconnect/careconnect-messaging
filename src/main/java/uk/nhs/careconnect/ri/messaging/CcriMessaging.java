package uk.nhs.careconnect.ri.messaging;

import ca.uhn.fhir.context.FhirContext;

import org.apache.camel.CamelContext;
import org.apache.camel.component.hl7.HL7MLLPCodec;
import org.apache.camel.impl.DefaultCamelContextNameStrategy;
import org.apache.camel.spring.boot.CamelContextConfiguration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.boot.web.servlet.ServletRegistrationBean;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.web.cors.CorsConfiguration;
import org.springframework.web.cors.CorsConfigurationSource;
import org.springframework.web.cors.UrlBasedCorsConfigurationSource;
import springfox.documentation.swagger2.annotations.EnableSwagger2;
import uk.nhs.careconnect.ri.messaging.support.CareConnectDSTU2toSTU3;
import uk.nhs.careconnect.ri.messaging.support.CorsFilter;

@SpringBootApplication
@EnableSwagger2
public class CcriMessaging {

    @Autowired
    ApplicationContext context;

    public static void main(String[] args) {
        System.setProperty("hawtio.authenticationEnabled", "false");
        System.setProperty("management.security.enabled","false");
        System.setProperty("management.contextPath","");
        SpringApplication.run(CcriMessaging.class, args);

    }

    @Bean
    public ServletRegistrationBean servletRegistrationBeanR3() {
        ServletRegistrationBean registration = new ServletRegistrationBean(new CustomRestfulServerR3(context), "/STU3/*");
        registration.setName("FhirServletR3");
        registration.setLoadOnStartup(1);
        return registration;
    }

    @Bean
    public ServletRegistrationBean servletRegistrationBeanDSTU2() {
        ServletRegistrationBean registration = new ServletRegistrationBean(new CustomRestfulServerDSTU2(context), "/DSTU2/*");
        registration.setName("FhirServletDSTU2");
        registration.setLoadOnStartup(2);
        return registration;
    }

    @Bean(name="CTXR3")
    public FhirContext getFhirContextR3() {
        return FhirContext.forDstu3();
    }

    @Bean(name="CTXDSTU2")
    public FhirContext getFhirContextDSTU2() {

        return FhirContext.forDstu2Hl7Org();
    }

    @Bean
    CorsConfigurationSource
    corsConfigurationSource() {
        UrlBasedCorsConfigurationSource source = new UrlBasedCorsConfigurationSource();
        source.registerCorsConfiguration("/**", new CorsConfiguration().applyPermitDefaultValues());
        return source;
    }

    @Bean
    public FilterRegistrationBean corsFilter() {

        UrlBasedCorsConfigurationSource source = new UrlBasedCorsConfigurationSource();
        CorsConfiguration config = new CorsConfiguration();
        config.setAllowCredentials(true);
        config.addAllowedOrigin("*");
        config.addAllowedHeader("*");
        config.addAllowedMethod("*");
        source.registerCorsConfiguration("/**", config);
        FilterRegistrationBean bean = new FilterRegistrationBean(new CorsFilter());
        bean.setOrder(0);
        return bean;
    }

    @Bean
    public CareConnectDSTU2toSTU3 careConnectDSTU2toSTU3() {
        return new CareConnectDSTU2toSTU3();
    }


    @Bean
    CamelContextConfiguration contextConfiguration() {
        return new CamelContextConfiguration() {

            @Override
            public void beforeApplicationStart(CamelContext camelContext) {

                camelContext.setNameStrategy(new DefaultCamelContextNameStrategy("CCRIMessaging"));

                final org.apache.camel.impl.SimpleRegistry registry = new org.apache.camel.impl.SimpleRegistry();
                final org.apache.camel.impl.CompositeRegistry compositeRegistry = new org.apache.camel.impl.CompositeRegistry();
                compositeRegistry.addRegistry(camelContext.getRegistry());
                compositeRegistry.addRegistry(registry);
                ((org.apache.camel.impl.DefaultCamelContext) camelContext).setRegistry(compositeRegistry);
                registry.put("hl7codec", new HL7MLLPCodec());
            }

            @Override
            public void afterApplicationStart(CamelContext camelContext) {
                // Empty method
            }
        };
    }


}
