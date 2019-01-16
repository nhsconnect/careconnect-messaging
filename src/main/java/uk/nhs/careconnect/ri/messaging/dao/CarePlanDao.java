package uk.nhs.careconnect.ri.messaging.dao;

import ca.uhn.fhir.model.api.Include;
import ca.uhn.fhir.rest.client.api.IGenericClient;
import ca.uhn.fhir.rest.param.TokenParam;
import org.hl7.fhir.dstu3.model.Bundle;
import org.hl7.fhir.dstu3.model.CarePlan;
import org.hl7.fhir.dstu3.model.IdType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class CarePlanDao implements ICarePlan {

    private static final Logger log = LoggerFactory.getLogger(CarePlanDao.class);

    @Override
    public Bundle searchCarePlan(IGenericClient client, IdType patient, TokenParam carePlanType) throws Exception {
        log.info(patient.getIdPart() + "  "+ carePlanType.getValue());
        return client
                .search()
                .forResource(CarePlan.class)
                .where(CarePlan.PATIENT.hasId(patient.getIdPart()))
                .and(CarePlan.CATEGORY.exactly().code(carePlanType.getValue()))
                .returnBundle(Bundle.class)
                .execute();
    }

    @Override
    public Bundle getCarePlan(IGenericClient client, IdType carePlan) throws Exception {
        log.info(carePlan.getIdPart() );
        Bundle bundle = client
                .search()
                .forResource(CarePlan.class)
                .where(CarePlan.RES_ID.exactly().code(carePlan.getIdPart()))

                .include(new Include("*"))
                .count(100) // be careful of this TODO
                .returnBundle(Bundle.class)
                .execute();
        return bundle;
    }
}
