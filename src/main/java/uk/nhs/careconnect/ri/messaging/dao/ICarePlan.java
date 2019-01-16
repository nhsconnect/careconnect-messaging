package uk.nhs.careconnect.ri.messaging.dao;

import ca.uhn.fhir.rest.client.api.IGenericClient;
import ca.uhn.fhir.rest.param.TokenParam;
import org.hl7.fhir.dstu3.model.Bundle;
import org.hl7.fhir.dstu3.model.IdType;

public interface ICarePlan {


    Bundle searchCarePlan(IGenericClient client, IdType patient, TokenParam carePlanType) throws Exception;


    Bundle getCarePlan(IGenericClient client, IdType carePlan) throws Exception;


}
