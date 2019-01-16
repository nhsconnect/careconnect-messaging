package uk.nhs.careconnect.ri.messaging.dao;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.model.api.Include;
import ca.uhn.fhir.rest.client.api.IGenericClient;
import ca.uhn.fhir.rest.param.ReferenceParam;
import ca.uhn.fhir.rest.param.TokenParam;

import ca.uhn.fhir.rest.server.exceptions.UnprocessableEntityException;
import org.hl7.fhir.dstu3.model.*;

import org.hl7.fhir.utilities.xhtml.XhtmlParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import org.springframework.stereotype.Component;
import org.thymeleaf.TemplateEngine;




import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

@Component
public class CompositionDao implements IComposition {

    @Autowired
    private TemplateEngine templateEngine;


    FhirContext ctxFHIR = FhirContext.forDstu3();


    private XhtmlParser xhtmlParser = new XhtmlParser();

    private ClassLoader getContextClassLoader() {
        return Thread.currentThread().getContextClassLoader();
    }

    public static boolean isNumeric(String s) {
        return s != null && s.matches("[-+]?\\d*\\.?\\d+");
    }


    private static final Logger log = LoggerFactory.getLogger(CompositionDao.class);

    public static final String SNOMEDCT = "http://snomed.info/sct";

    DateFormat df = new SimpleDateFormat("HHmm_dd_MM_yyyy");

    Composition composition = null;

    private FhirBundleUtil fhirBundleUtil;

    @Override
    public List<Resource> search(FhirContext ctx, TokenParam resid, ReferenceParam patient) {

        List<Resource> resources = new ArrayList<>();



        return resources;
    }

    @Override
    public Composition read(FhirContext ctx, IdType theId) {

           return null;
    }

    /*

    CARE PLAN

     */

    @Override
    public Bundle buildCarePlanDocument(IGenericClient client, IdType carePlanId, TokenParam recordSection) throws Exception {
        fhirBundleUtil = new FhirBundleUtil(Bundle.BundleType.DOCUMENT);
        Bundle compositionBundle = new Bundle();

        // Main resource of a FHIR Bundle is a Composition
        composition = new Composition();
        composition.setId(UUID.randomUUID().toString());
        compositionBundle.addEntry().setResource(composition);

        // composition.getMeta().addProfile(CareConnectProfile.Composition_1);
        composition.setTitle("Care Plan Document");

        composition.setDate(new Date());
        composition.setStatus(Composition.CompositionStatus.FINAL);

        Organization leedsTH = getOrganization(client,"RR8");
        compositionBundle.addEntry().setResource(leedsTH);

        composition.addAttester()
                .setParty(new Reference("Organization/"+leedsTH.getIdElement().getIdPart()))
                .addMode(Composition.CompositionAttestationMode.OFFICIAL);




        composition.getType().addCoding()
                .setCode("736373009")
                .setDisplay("End of life care plan (record artifact)")
                .setSystem(SNOMEDCT);

        fhirBundleUtil.processBundleResources(compositionBundle);
        fhirBundleUtil.processReferences();



        Bundle carePlanBundle = getCarePlanBundle(client,carePlanId.getIdPart());
        CarePlan carePlan = null;
        for(Bundle.BundleEntryComponent entry : carePlanBundle.getEntry()) {
            Resource resource =  entry.getResource();
            if (carePlan == null && entry.getResource() instanceof CarePlan) {
                carePlan = (CarePlan) entry.getResource();
            }
        }

        for (Reference reference : carePlan.getAuthor()) {
            composition.addAuthor(reference);
        }

        String patientId = null;

        if (carePlan!=null) {

            patientId = carePlan.getSubject().getReferenceElement().getIdPart();
            log.debug(carePlan.getSubject().getReferenceElement().getIdPart());


            // This is a synthea patient
            Bundle patientBundle = getPatientBundle(client, patientId);

            fhirBundleUtil.processBundleResources(patientBundle);

            if (fhirBundleUtil.getPatient() == null) throw new Exception("404 Patient not found");
            composition.setSubject(new Reference("Patient/"+patientId));
            patientId = fhirBundleUtil.getPatient().getId();
        }

        if (fhirBundleUtil.getPatient() == null) throw new UnprocessableEntityException();

        fhirBundleUtil.processBundleResources(carePlanBundle);

        fhirBundleUtil.processReferences();

        FhirDocUtil fhirDoc = new FhirDocUtil(templateEngine);


        //  Do we only include a section if it has data?

        Composition.SectionComponent section = null;

        /*
        section = fhirDoc.getCareTeamSection(fhirBundleUtil.getFhirDocument());
        if (section.getEntry().size()>0) composition.addSection(section);
*/
        if (recordSection != null) {
            switch (recordSection.getValue()) {
                case "consent":
                    section = fhirDoc.getConsentSection(fhirBundleUtil.getFhirDocument());
                    composition.addSection(section);
                    break;
                case "advancedtreatmentpreferences":
                    section = fhirDoc.getAdvanceTreatmentPreferencesSection(fhirBundleUtil.getFhirDocument());
                    composition.addSection(section);
                    break;
                case "disability":
                    section = fhirDoc.getDisabilitySection(fhirBundleUtil.getFhirDocument());
                    composition.addSection(section);
                    break;
                case "preferences":
                    section = fhirDoc.getPreferencesSection(fhirBundleUtil.getFhirDocument());
                    composition.addSection(section);
                    break;
                case "prognosis":
                    section = fhirDoc.getPrognosis(fhirBundleUtil.getFhirDocument());
                    composition.addSection(section);
                    break;
            }
        } else {
            section = fhirDoc.getCareTeamSection(fhirBundleUtil.getFhirDocument());
            if (section.getEntry().size()>0) composition.addSection(section);

            section = fhirDoc.getConsentSection(fhirBundleUtil.getFhirDocument());
            composition.addSection(section);

            section = fhirDoc.getAdvanceTreatmentPreferencesSection(fhirBundleUtil.getFhirDocument());
            composition.addSection(section);

            section = fhirDoc.getDisabilitySection(fhirBundleUtil.getFhirDocument());
            composition.addSection(section);

            section = fhirDoc.getPreferencesSection(fhirBundleUtil.getFhirDocument());
            composition.addSection(section);

            section = fhirDoc.getPrognosis(fhirBundleUtil.getFhirDocument());
            composition.addSection(section);
        }

        return fhirBundleUtil.getFhirDocument();
    }

    @Override
    public Bundle buildEncounterDocument(IGenericClient client, IdType encounterId) throws Exception {


        fhirBundleUtil = new FhirBundleUtil(Bundle.BundleType.DOCUMENT);
        Bundle compositionBundle = new Bundle();

        // Main resource of a FHIR Bundle is a Composition
        composition = new Composition();
        composition.setId(UUID.randomUUID().toString());
        compositionBundle.addEntry().setResource(composition);

        // composition.getMeta().addProfile(CareConnectProfile.Composition_1);
        composition.setTitle("Encounter Document");
        composition.setDate(new Date());
        composition.setStatus(Composition.CompositionStatus.FINAL);

        Organization leedsTH = getOrganization(client,"RR8");
        compositionBundle.addEntry().setResource(leedsTH);

        composition.addAttester()
                .setParty(new Reference("Organization/"+leedsTH.getIdElement().getIdPart()))
                .addMode(Composition.CompositionAttestationMode.OFFICIAL);


        Device device = new Device();
        device.setId(UUID.randomUUID().toString());
        device.getType().addCoding()
                .setSystem("http://snomed.info/sct")
                .setCode("58153004")
                .setDisplay("Android");
        device.setOwner(new Reference("Organization/"+leedsTH.getIdElement().getIdPart()));
        compositionBundle.addEntry().setResource(device);

        composition.addAuthor(new Reference("Device/"+device.getIdElement().getIdPart()));

        composition.getType().addCoding()
                .setCode("371531000")
                .setDisplay("Report of clinical encounter")
                .setSystem(SNOMEDCT);

        fhirBundleUtil.processBundleResources(compositionBundle);
        fhirBundleUtil.processReferences();


        Bundle encounterBundle = getEncounterBundleRev(client,encounterId.getIdPart());
        Encounter encounter = null;
        for(Bundle.BundleEntryComponent entry : encounterBundle.getEntry()) {
            Resource resource =  entry.getResource();
            if (encounter == null && entry.getResource() instanceof Encounter) {
                encounter = (Encounter) entry.getResource();
            }
        }
        String patientId = null;

        if (encounter!=null) {

            patientId = encounter.getSubject().getReferenceElement().getIdPart();
            log.debug(encounter.getSubject().getReferenceElement().getIdPart());


            // This is a synthea patient
            Bundle patientBundle = getPatientBundle(client, patientId);

            fhirBundleUtil.processBundleResources(patientBundle);

            if (fhirBundleUtil.getPatient() == null) throw new Exception("404 Patient not found");
            composition.setSubject(new Reference("Patient/"+patientId));
            patientId = fhirBundleUtil.getPatient().getId();
        }

        if (fhirBundleUtil.getPatient() == null) throw new UnprocessableEntityException();

        fhirBundleUtil.processBundleResources(encounterBundle);

        fhirBundleUtil.processReferences();

        FhirDocUtil fhirDoc = new FhirDocUtil(templateEngine);

        composition.addSection(fhirDoc.getEncounterSection(fhirBundleUtil.getFhirDocument()));

        Composition.SectionComponent section = fhirDoc.getConditionSection(fhirBundleUtil.getFhirDocument());
        if (section.getEntry().size()>0) composition.addSection(section);

        section = fhirDoc.getMedicationStatementSection(fhirBundleUtil.getFhirDocument());
        if (section.getEntry().size()>0) composition.addSection(section);

        section = fhirDoc.getMedicationRequestSection(fhirBundleUtil.getFhirDocument());
        if (section.getEntry().size()>0) composition.addSection(section);

        section = fhirDoc.getAllergySection(fhirBundleUtil.getFhirDocument());
        if (section.getEntry().size()>0) composition.addSection(section);

        section = fhirDoc.getObservationSection(fhirBundleUtil.getFhirDocument());
        if (section.getEntry().size()>0) composition.addSection(section);

        section = fhirDoc.getProcedureSection(fhirBundleUtil.getFhirDocument());
        if (section.getEntry().size()>0) composition.addSection(section);

        return fhirBundleUtil.getFhirDocument();
    }

    @Override
    public Bundle readDocument(FhirContext ctx, IdType theId) {
        // Search for document bundle rather than composition (this contains a link to the Composition

        // {'entry.objectId': ObjectId("5a95166bbc5b249440975d8f"), 'entry.resourceType' : 'Composition'}
        Bundle bundle = new Bundle();
        bundle.setType(Bundle.BundleType.DOCUMENT);


        return bundle;
    }

    @Override
    public Bundle buildSummaryCareDocument(IGenericClient client, IdType patientIdType) throws Exception {

        log.info("SCR for patient = "+patientIdType.getIdPart());

        //this.client = client;
        String patientId = patientIdType.getIdPart();

        if (!isNumeric(patientId)) {
            return null;
        }

        // Create Bundle of type Document
        fhirBundleUtil = new FhirBundleUtil(Bundle.BundleType.DOCUMENT);
        // Main resource of a FHIR Bundle is a Composition

        Bundle compositionBundle = new Bundle();

        composition = new Composition();
        composition.setId(UUID.randomUUID().toString());
        compositionBundle.addEntry().setResource(composition);

        composition.setTitle("Patient Summary Care Record");
        composition.setDate(new Date());
        composition.setStatus(Composition.CompositionStatus.FINAL);

        Organization leedsTH = getOrganization(client,"RR8");
        compositionBundle.addEntry().setResource(leedsTH);

        composition.addAttester()
                .setParty(new Reference(leedsTH.getId()))
                .addMode(Composition.CompositionAttestationMode.OFFICIAL);

        Device device = new Device();
        device.setId(UUID.randomUUID().toString());
        device.getType().addCoding()
                .setSystem("http://snomed.info/sct")
                .setCode("58153004")
                .setDisplay("Android");
        device.setOwner(new Reference("Organization/"+leedsTH.getIdElement().getIdPart()));

        compositionBundle.addEntry().setResource(device);

        composition.addAuthor(new Reference("Device/"+device.getIdElement().getIdPart()));

        fhirBundleUtil.processBundleResources(compositionBundle);
        fhirBundleUtil.processReferences();

        // This is a synthea patient
        Bundle patientBundle = getPatientBundle(client, patientId);
        fhirBundleUtil.processBundleResources(patientBundle);

        if (fhirBundleUtil.getPatient() == null) throw new Exception("404 Patient not found");
        composition.setSubject(new Reference("Patient/"+patientId));

        FhirDocUtil fhirDoc = new FhirDocUtil(templateEngine);

        patientId = fhirBundleUtil.getPatient().getId();
        fhirDoc.generatePatientHtml(fhirBundleUtil.getPatient(),patientBundle);

        /* CONDITION */

        Bundle conditionBundle = getConditionBundle(client,patientId);
        fhirBundleUtil.processBundleResources(conditionBundle);
        composition.addSection(fhirDoc.getConditionSection(conditionBundle));

        /* MEDICATION STATEMENT */

        Bundle medicationStatementBundle = getMedicationStatementBundle(client,patientId);
        fhirBundleUtil.processBundleResources(medicationStatementBundle);
        composition.addSection(fhirDoc.getMedicationStatementSection(medicationStatementBundle));


        /* ALLERGY INTOLERANCE */

        Bundle allergyBundle = getAllergyBundle(client,patientId);
        fhirBundleUtil.processBundleResources(allergyBundle);
        composition.addSection(fhirDoc.getAllergySection(allergyBundle));

        /* ENCOUNTER */

        Bundle encounterBundle = getEncounterBundle(client,patientId);
        fhirBundleUtil.processBundleResources(encounterBundle);
        composition.addSection(fhirDoc.getEncounterSection(encounterBundle));

        fhirBundleUtil.processReferences();
        log.debug(ctxFHIR.newJsonParser().setPrettyPrint(true).encodeResourceToString(fhirBundleUtil.getFhirDocument()));

        return fhirBundleUtil.getFhirDocument();
    }
    private Bundle getPatientBundle(IGenericClient client, String patientId) {

        return client
                .search()
                .forResource(Patient.class)
                .where(Patient.RES_ID.exactly().code(patientId))
                .include(Patient.INCLUDE_GENERAL_PRACTITIONER)
                .include(Patient.INCLUDE_ORGANIZATION)
                .returnBundle(Bundle.class)
                .execute();
    }




    private Bundle getConditionBundle(IGenericClient client,String patientId) {

        return client
                .search()
                .forResource(Condition.class)
                .where(Condition.PATIENT.hasId(patientId))
                .and(Condition.CLINICAL_STATUS.exactly().code("active"))
                .returnBundle(Bundle.class)
                .execute();
    }

    private Bundle getConditionDisabilityBundle(IGenericClient client,String patientId) {

        return client
                .search()
                .forResource(Condition.class)
                .where(Condition.PATIENT.hasId(patientId))
                .and(Condition.CLINICAL_STATUS.exactly().code("active"))
                .returnBundle(Bundle.class)
                .execute();
    }

    private Bundle getEncounterBundleRev(IGenericClient client, String encounterId) {

        Bundle bundle = client
                .search()
                .forResource(Encounter.class)
                .where(Encounter.RES_ID.exactly().code(encounterId))
                .revInclude(new Include("*"))
                .include(new Include("*"))
                .count(100) // be careful of this TODO
                .returnBundle(Bundle.class)
                .execute();
        return bundle;
    }

    private Bundle getCarePlanBundle(IGenericClient client, String carePlanId) {

        Bundle bundle = client
                .search()
                .forResource(CarePlan.class)
                .where(CarePlan.RES_ID.exactly().code(carePlanId))
                .include(new Include("*"))
                //.revInclude(new Include("*"))
                .count(100) // be careful of this TODO
                .returnBundle(Bundle.class)
                .execute();
        return bundle;
    }

    private Bundle getEncounterBundle(IGenericClient client,String patientId) {

        return client
                .search()
                .forResource(Encounter.class)
                .where(Encounter.PATIENT.hasId(patientId))
                .count(3) // Last 3 entries same as GP Connect
                .returnBundle(Bundle.class)
                .execute();
    }

    private Organization getOrganization(IGenericClient client,String sdsCode) {
        Organization organization = null;
        Bundle bundle =  client
                .search()
                .forResource(Organization.class)
                .where(Organization.IDENTIFIER.exactly().code(sdsCode))

                .returnBundle(Bundle.class)
                .execute();
        if (bundle.getEntry().size()>0) {
            if (bundle.getEntry().get(0).getResource() instanceof Organization)
                organization = (Organization) bundle.getEntry().get(0).getResource();
        }
        return organization;
    }
    private Bundle getMedicationStatementBundle(IGenericClient client,String patientId) {

        return client
                .search()
                .forResource(MedicationStatement.class)
                .where(MedicationStatement.PATIENT.hasId(patientId))
                .and(MedicationStatement.STATUS.exactly().code("active"))
                .returnBundle(Bundle.class)
                .execute();
    }

    private Bundle getMedicationRequestBundle(IGenericClient client,String patientId) {

        return client
                .search()
                .forResource(MedicationStatement.class)
                .where(MedicationRequest.PATIENT.hasId(patientId))
                .and(MedicationRequest.STATUS.exactly().code("active"))
                .returnBundle(Bundle.class)
                .execute();
    }

    private Bundle getAllergyBundle(IGenericClient client,String patientId) {

        return client
                .search()
                .forResource(AllergyIntolerance.class)
                .where(AllergyIntolerance.PATIENT.hasId(patientId))
                .returnBundle(Bundle.class)
                .execute();
    }


}
