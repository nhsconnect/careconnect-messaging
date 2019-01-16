package uk.nhs.careconnect.ri.messaging.dao;

import ca.uhn.fhir.rest.client.api.IGenericClient;
import ca.uhn.fhir.rest.param.TokenParam;
import org.hl7.fhir.dstu3.model.Bundle;

public interface IPatient {


    Bundle getPatient(IGenericClient client, TokenParam nhsNumber) throws Exception;


}
