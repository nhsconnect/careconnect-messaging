package uk.nhs.careconnect.ri.messaging.dao;

import ca.uhn.fhir.rest.client.api.IGenericClient;
import ca.uhn.fhir.rest.param.TokenParam;
import org.hl7.fhir.dstu3.model.Bundle;
import org.hl7.fhir.dstu3.model.Patient;
import org.springframework.stereotype.Component;

@Component
public class PatientDao implements IPatient {

    @Override
    public Bundle getPatient(IGenericClient client, TokenParam nhsNumber) throws Exception {
        return client
                .search()
                .forResource(Patient.class)
                .where(Patient.IDENTIFIER.exactly().code(nhsNumber.getValue()))
                .returnBundle(Bundle.class)
                .execute();
    }
}
