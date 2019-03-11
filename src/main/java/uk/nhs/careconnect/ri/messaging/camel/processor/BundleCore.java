package uk.nhs.careconnect.ri.messaging.camel.processor;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.rest.annotation.Operation;
import ca.uhn.fhir.rest.api.MethodOutcome;
import ca.uhn.fhir.rest.client.api.IGenericClient;
import ca.uhn.fhir.rest.server.exceptions.BaseServerResponseException;
import ca.uhn.fhir.rest.server.exceptions.InternalErrorException;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import ca.uhn.fhir.rest.server.exceptions.ResourceNotFoundException;
import org.apache.camel.*;
import org.apache.commons.io.IOUtils;
import org.hl7.fhir.dstu3.model.*;
import org.hl7.fhir.instance.model.api.IBase;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.nhs.careconnect.ri.messaging.support.OperationOutcomeException;
import uk.nhs.careconnect.ri.messaging.support.OperationOutcomeFactory;

import javax.print.Doc;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BundleCore {

    public BundleCore(FhirContext ctx, CamelContext camelContext, Bundle bundle, String eprBase, String edmsBase) {
        this.ctx = ctx;
        this.bundle = bundle;
        this.context = camelContext;
        this.clientODS = FhirContext.forDstu3().newRestfulGenericClient("https://directory.spineservices.nhs.uk/STU3");
        this.clientEPR = FhirContext.forDstu3().newRestfulGenericClient(eprBase);
        this.clientEDMS = FhirContext.forDstu3().newRestfulGenericClient(edmsBase);
        this.eprBase = eprBase;
        this.edmsBase = edmsBase;
    }

    CamelContext context;

    FhirContext ctx;

    IGenericClient clientODS;

    IGenericClient clientEPR;

    IGenericClient clientEDMS;

    private String eprBase;

    private String edmsBase;

    private ProducerTemplate template = null;

/*
    private FHIRMedicationStatementToFHIRMedicationRequestTransformer
            fhirMedicationStatementToFHIRMedicationRequestTransformer = new  FHIRMedicationStatementToFHIRMedicationRequestTransformer();
*/

    private Map<String, Resource> resourceMap = new HashMap<>();;

    private Bundle bundle;

    private OperationOutcome operationOutcome = null;

    private static final Logger log = LoggerFactory.getLogger(BundleCore.class);

    public Reference getReference(Resource resource) {
        Reference reference = new Reference();
        reference.setReference(resource.getId());
        return reference;
    }

    public OperationOutcome getOperationOutcome() {
        return operationOutcome;
    }

    public Resource setOperationOutcome(OperationOutcome operationOutcome) {
        this.operationOutcome = operationOutcome;
        return null;
    }

    public Bundle getUpdatedBundle() throws OperationOutcomeException {
        //
        Bundle updatedBundle = new Bundle();
        updatedBundle.setType(this.bundle.getType());
        updatedBundle.setIdentifier(this.bundle.getIdentifier());
        for (Bundle.BundleEntryComponent entry : bundle.getEntry()) {
            Resource iResource = resourceMap.get(entry.getResource().getId());
            if (iResource == null) {
                iResource = searchAddResource(entry.getResource().getId());
            }
            if (iResource != null) {
                updatedBundle.addEntry().setResource(iResource);
            } else {
                log.warn("Not found "+entry.getResource().getClass().getSimpleName() + " Reference " + entry.getResource().getId());
                updatedBundle.addEntry().setResource(entry.getResource());
            }
        }
        return updatedBundle;
    }



    public Bundle getBundle() {
        return bundle;
    }

    public Boolean checkCircularReference(Encounter encounter) {
        Boolean found = false;
        log.debug("Checking Encounter id="+encounter.getId());
        log.debug("Checking Encounter idElement="+encounter.getIdElement());
        if (encounter.hasDiagnosis()) {
            for (Bundle.BundleEntryComponent entry : bundle.getEntry()) {
                if (entry.getResource() instanceof Condition) {
                    Condition condition = (Condition) entry.getResource();
                    log.debug("Check condition = "+condition.getId());
                    if (condition.hasContext()) {
                        for (Encounter.DiagnosisComponent diagnosis : encounter.getDiagnosis()) {
                            log.debug("Check encounter.diagnosis = "+diagnosis.getCondition().getReference());
                            if (diagnosis.getCondition().getReference().equals(condition.getId())) {

                                if (condition.getContext().getReference().equals(encounter.getId())) {
                                    OperationOutcome outcome = new OperationOutcome();
                                    outcome.addIssue()
                                            .setCode(OperationOutcome.IssueType.BUSINESSRULE)
                                            .setSeverity(OperationOutcome.IssueSeverity.FATAL)
                                            .setDiagnostics("Encounter "+encounter.getId()+" has a circular diagnosis reference to Condition "+condition.getId())
                                            .setDetails(
                                                    new CodeableConcept().setText("Circular Reference")
                                            );
                                    setOperationOutcome(outcome);
                                    OperationOutcomeFactory.convertToException(outcome);
                                }
                            }
                        }
                    }
                }
            }
        }
        return found;
    }

    public Resource searchAddResource(String referenceId) throws OperationOutcomeException {
        if (this.template == null) {
            this.template = context.createProducerTemplate();
        }
        try {
            log.debug("searchAddResource " + referenceId);
            if (referenceId == null) {
                return null; //throw new InternalErrorException("Null Reference");
            }
            Resource resource = resourceMap.get(referenceId);
            // Don't process, if already processed.
            if (resource != null) {
                log.debug("Already Processed " + resource.getId());
                return resource;
            }

            if (referenceId.contains("demographics.spineservices.nhs.uk")) {
                //
                log.debug("NHS Number detected");
            }
            if (referenceId.contains("directory.spineservices.nhs.uk")) {
                if (referenceId.contains("Organization")) {
                    String sdsCode = referenceId.replace("https://directory.spineservices.nhs.uk/STU3/Organization/","");
                    Organization sdsOrganization = null;
                    try {
                        sdsOrganization = clientODS.read().resource(Organization.class).withId(sdsCode).execute();
                    } catch(Exception ex) {
                        throw new ResourceNotFoundException("https://directory.spineservices.nhs.uk/STU3/Organization/"+sdsCode);
                    }
                    if (sdsOrganization != null) {
                        resource = searchAddOrganisation(referenceId, sdsOrganization);
                    }
                }
                /*
                if (referenceId.contains("Practitioner")) {
                    String sdsCode = referenceId.replace("https://directory.spineservices.nhs.uk/STU3/Practitioner/","");
                    Practitioner sdsPractitioner = client.read().resource(Practitioner.class).withId(sdsCode).execute();

                    if (sdsPractitioner != null) {
                        resource = searchAddPractitioner(referenceId, sdsPractitioner);
                    }
                }
                */
            } else {

                for (Bundle.BundleEntryComponent entry : bundle.getEntry()) {
                    Resource iResource = null;
                    if ((entry.getFullUrl() != null && entry.getFullUrl().equals(referenceId)) || (iResource == null && entry.getResource() != null && entry.getResource().getId() != null && entry.getResource().getId().equals(referenceId))) {
                        iResource = entry.getResource();

                        if (iResource instanceof Patient) {
                            resource = searchAddPatient(referenceId, (Patient) iResource);
                        } else if (iResource instanceof Practitioner) {
                            resource = searchAddPractitioner(referenceId, (Practitioner) iResource);
                        } else if (iResource instanceof Encounter) {
                            resource = searchAddEncounter(referenceId, (Encounter) iResource);
                        } else if (iResource instanceof Organization) {
                            resource = searchAddOrganisation(referenceId, (Organization) iResource);
                        } else if (iResource instanceof Location) {
                            resource = searchAddLocation(referenceId, (Location) iResource);
                        } else if (iResource instanceof Observation) {
                            resource = searchAddObservation(referenceId, (Observation) iResource);
                        } else if (iResource instanceof AllergyIntolerance) {
                            resource = searchAddAllergyIntolerance(referenceId, (AllergyIntolerance) iResource);
                        } else if (iResource instanceof Condition) {
                            resource = searchAddCondition(referenceId, (Condition) iResource);
                        } else if (iResource instanceof Procedure) {
                            resource = searchAddProcedure(referenceId, (Procedure) iResource);
                      //  } else if (iResource instanceof Composition) {
                      //      resource = searchAddComposition(referenceId, (Composition) iResource);
                      //  } else if (iResource instanceof DiagnosticReport) {
                      //      resource = searchAddDiagnosticReport(referenceId, (DiagnosticReport) iResource);
                        } else if (iResource instanceof MedicationRequest) {
                            resource = searchAddMedicationRequest(referenceId, (MedicationRequest) iResource);
                        } else if (iResource instanceof MedicationStatement) {
                            resource = searchAddMedicationStatement(referenceId, (MedicationStatement) iResource);
                       } else if (iResource instanceof ListResource) {
                           resource = searchAddList(referenceId, (ListResource) iResource);
                        } else if (iResource instanceof Immunization) {
                            resource = searchAddImmunization(referenceId, (Immunization) iResource);

                        } else {

                            switch (iResource.getClass().getSimpleName()) {
                                case "Binary":
                                    resource = searchAddBinary(referenceId, (Binary) iResource);
                                    break;
                                case "CarePlan":
                                    resource = searchAddCarePlan(referenceId, (CarePlan) iResource);
                                    break;
                                case "CareTeam":
                                    resource = searchAddCareTeam(referenceId, (CareTeam) iResource);
                                    break;
                                case "ClinicalImpression":
                                    resource = searchAddClinicalImpression(referenceId, (ClinicalImpression) iResource);
                                    break;
                                case "Consent":
                                    resource = searchAddConsent(referenceId, (Consent) iResource);
                                    break;
                                case "DocumentReference":
                                    resource = searchAddDocumentReference(referenceId, (DocumentReference) iResource);
                                    break;
                                case "EpisodeOfCare":
                                    resource = searchAddEpisodeOfCare(referenceId, (EpisodeOfCare) iResource);
                                    break;
                                case "Flag":
                                    resource = searchAddFlag(referenceId, (Flag) iResource);
                                    break;
                                case "Goal":
                                    resource = searchAddGoal(referenceId, (Goal) iResource);
                                    break;
                                case "HealthcareService":
                                    resource = searchAddHealthcareService(referenceId, (HealthcareService) iResource);
                                    break;
                                case "MedicationAdministration":
                                    resource = searchAddMedicationAdministration(referenceId, (MedicationAdministration) iResource);
                                    break;
                                case "MedicationDispense":
                                    resource = searchAddMedicationDispense(referenceId, (MedicationDispense) iResource);
                                    break;
                                case "MedicationRequest":
                                    resource = searchAddMedicationRequest(referenceId, (MedicationRequest) iResource);
                                    break;
                                case "QuestionnaireResponse":
                                    resource = searchAddQuestionnaireResponse(referenceId, (QuestionnaireResponse) iResource);
                                    break;
                                case "Questionnaire":
                                    resource = searchAddQuestionnaire(referenceId, (Questionnaire) iResource);
                                    break;
                                case "RelatedPerson":
                                    resource = searchAddRelatedPerson(referenceId, (RelatedPerson) iResource);
                                    break;
                                case "ReferralRequest":
                                    resource = searchAddReferralRequest(referenceId, (ReferralRequest) iResource);
                                    break;
                              /*  case "RiskAssessment":
                                    resource = searchAddRiskAssessment(referenceId, (RiskAssessment) iResource);
                                    break; */
                                case "Medication":
                                    resource = searchAddMedication(referenceId, (Medication) iResource);
                                    break;
                                default:
                                    log.debug("Found in Bundle. Not processed (" + iResource.getClass());
                            }

                        }
                    }

                    //else if (iResource instanceof PractitionerRole) {
                    //    resource = searchAddReferralRequest(referenceId, (ReferralRequest) iResource);
                    //}
                }
            }
            if (resource == null) log.debug("Search Not Found " + referenceId);
            if (this.operationOutcome != null) return operationOutcome;
            return resource;
        } catch (Exception ex) {

            String errorMessage = "Exception while processing reference "+referenceId;


            if (ex.getStackTrace().length >0) {
                errorMessage = errorMessage + " (Line: "+ex.getStackTrace()[0].getLineNumber() + " Method: " + ex.getStackTrace()[0].getMethodName() + " " + ex.getStackTrace()[0].getClassName() + ")";
            }
            if (ex instanceof BaseServerResponseException &&  this.operationOutcome != null && this.operationOutcome.getIssueFirstRep() != null) {
                //log.error("HAPI Exception " +ex.getClass().getSimpleName() );

                errorMessage = errorMessage + " Diagnostics: " + this.operationOutcome.getIssueFirstRep().getDiagnostics();
            } else {
                if (ex.getMessage() != null) {
                    errorMessage = errorMessage + " getMessage: " +ex.getMessage();
                } else {
                    errorMessage = errorMessage + " ExceptionClassname: " +ex.getClass().getSimpleName();
                }

            }
            log.error(errorMessage);
            throw ex;
        }

        //return null;
    }

    /*
    public IBaseResource getResource(Exchange exchange) throws OperationOutcomeException
    {
        InputStream inputStream = (InputStream) exchange.getIn().getBody();
        String responseCode = exchange.getIn().getHeader(Exchange.HTTP_RESPONSE_CODE).toString();
        if (exchange.getIn().getBody() instanceof String ) {
            log.error((String) exchange.getIn().getBody());
            throw new InternalErrorException((String) exchange.getIn().getBody());
        }
        Reader reader = new InputStreamReader(inputStream);
        IBaseResource iresource = null;
        try {
            iresource = ctx.newJsonParser().parseResource(reader);
        } catch (Exception ex) {
            log.error("Resonse Code = " + responseCode + " (" +ex.getMessage() + ")");
            switch (responseCode) {
                case "404":
                    throw new ResourceNotFoundException(exchange.getIn().getHeader(Exchange.HTTP_METHOD).toString() + " " + exchange.getIn().getHeader(Exchange.HTTP_PATH).toString() + " " + exchange.getIn().getHeader(Exchange.HTTP_QUERY).toString());

                    default:
                        throw new InternalErrorException(ex.getMessage());
            }
        }
        if (iresource instanceof OperationOutcome) {
            log.error("Operation outcome with Resonse Code = " + responseCode);
            processOperationOutcome((OperationOutcome) iresource);
        }
        if (!responseCode.equals("200") && !responseCode.equals("201")) {
            log.error("Unexpected Error on "+exchange.getIn().getHeader(Exchange.HTTP_PATH).toString() + " " + exchange.getIn().getHeader(Exchange.HTTP_QUERY).toString());
        }
        return iresource;
    }
    */

    public IBaseResource queryResource(Identifier identifier, String resourceName) throws OperationOutcomeException {

        log.info("Search "+resourceName+"?identifier="+identifier.getSystem()+"|"+identifier.getValue());
        Class resourceType = null;

        switch (resourceName) {
            case "AllergyIntolerance":
                return clientEPR.search().forResource(AllergyIntolerance.class).where(AllergyIntolerance.IDENTIFIER.exactly().systemAndCode(identifier.getSystem(),identifier.getValue())).returnBundle(Bundle.class).execute();
            case "Appointment":
                return clientEPR.search().forResource(Appointment.class).where(Appointment.IDENTIFIER.exactly().systemAndCode(identifier.getSystem(),identifier.getValue())).returnBundle(Bundle.class).execute();
            case "CarePlan":
                return clientEPR.search().forResource(CarePlan.class).where(CarePlan.IDENTIFIER.exactly().systemAndCode(identifier.getSystem(),identifier.getValue())).returnBundle(Bundle.class).execute();
            case "ClinicalImpression":
                return clientEPR.search().forResource(ClinicalImpression.class).where(ClinicalImpression.IDENTIFIER.exactly().systemAndCode(identifier.getSystem(),identifier.getValue())).returnBundle(Bundle.class).execute();

                case "Condition":
                return clientEPR.search().forResource(Condition.class).where(Condition.IDENTIFIER.exactly().systemAndCode(identifier.getSystem(),identifier.getValue())).returnBundle(Bundle.class).execute();
            case "Consent":
                return clientEPR.search().forResource(Consent.class).where(Consent.IDENTIFIER.exactly().systemAndCode(identifier.getSystem(),identifier.getValue())).returnBundle(Bundle.class).execute();

            case "DocumentReference":
                return clientEPR.search().forResource(DocumentReference.class).where(DocumentReference.IDENTIFIER.exactly().systemAndCode(identifier.getSystem(),identifier.getValue())).returnBundle(Bundle.class).execute();
            case "Encounter":
                return clientEPR.search().forResource(Encounter.class).where(Encounter.IDENTIFIER.exactly().systemAndCode(identifier.getSystem(),identifier.getValue())).returnBundle(Bundle.class).execute();
            case "EpisodeOfCare":
                return clientEPR.search().forResource(EpisodeOfCare.class).where(EpisodeOfCare.IDENTIFIER.exactly().systemAndCode(identifier.getSystem(),identifier.getValue())).returnBundle(Bundle.class).execute();
            case "Flag":
                return clientEPR.search().forResource(Flag.class).where(Flag.IDENTIFIER.exactly().systemAndCode(identifier.getSystem(),identifier.getValue())).returnBundle(Bundle.class).execute();
            case "HealthcareService":
                return clientEPR.search().forResource(HealthcareService.class).where(HealthcareService.IDENTIFIER.exactly().systemAndCode(identifier.getSystem(),identifier.getValue())).returnBundle(Bundle.class).execute();
            case "Immunization":
                return clientEPR.search().forResource(Immunization.class).where(Immunization.IDENTIFIER.exactly().systemAndCode(identifier.getSystem(),identifier.getValue())).returnBundle(Bundle.class).execute();
            case "List":
                return clientEPR.search().forResource(ListResource.class).where(ListResource.IDENTIFIER.exactly().systemAndCode(identifier.getSystem(),identifier.getValue())).returnBundle(Bundle.class).execute();

            case "Location":
                return clientEPR.search().forResource(Location.class).where(Location.IDENTIFIER.exactly().systemAndCode(identifier.getSystem(),identifier.getValue())).returnBundle(Bundle.class).execute();
          //  case "Medication":
          //      return clientEPR.search().forResource(Medication.class).where(Medication.IDENTIFIER.exactly().systemAndCode(identifier.getSystem(),identifier.getValue())).returnBundle(Bundle.class).execute();
            case "MedicationDispense":
                return clientEPR.search().forResource(MedicationDispense.class).where(MedicationDispense.IDENTIFIER.exactly().systemAndCode(identifier.getSystem(),identifier.getValue())).returnBundle(Bundle.class).execute();
            case "MedicationRequest":
                return clientEPR.search().forResource(MedicationRequest.class).where(MedicationRequest.IDENTIFIER.exactly().systemAndCode(identifier.getSystem(),identifier.getValue())).returnBundle(Bundle.class).execute();
            case "MedicationAdministration":
                return clientEPR.search().forResource(MedicationAdministration.class).where(MedicationAdministration.IDENTIFIER.exactly().systemAndCode(identifier.getSystem(),identifier.getValue())).returnBundle(Bundle.class).execute();
            case "MedicationStatement":
                return clientEPR.search().forResource(MedicationStatement.class).where(MedicationStatement.IDENTIFIER.exactly().systemAndCode(identifier.getSystem(),identifier.getValue())).returnBundle(Bundle.class).execute();
            case "Observation":
                return clientEPR.search().forResource(Observation.class).where(Observation.IDENTIFIER.exactly().systemAndCode(identifier.getSystem(),identifier.getValue())).returnBundle(Bundle.class).execute();
            case "Organization":
                return clientEPR.search().forResource(Organization.class).where(Organization.IDENTIFIER.exactly().systemAndCode(identifier.getSystem(),identifier.getValue())).returnBundle(Bundle.class).execute();
            case "Practitioner":
                return clientEPR.search().forResource(Practitioner.class).where(Practitioner.IDENTIFIER.exactly().systemAndCode(identifier.getSystem(),identifier.getValue())).returnBundle(Bundle.class).execute();
            case "Procedure":
                return clientEPR.search().forResource(Procedure.class).where(Procedure.IDENTIFIER.exactly().systemAndCode(identifier.getSystem(),identifier.getValue())).returnBundle(Bundle.class).execute();
            case "QuestionnaireResponse":
                return clientEPR.search().forResource(QuestionnaireResponse.class).where(QuestionnaireResponse.IDENTIFIER.exactly().systemAndCode(identifier.getSystem(),identifier.getValue())).returnBundle(Bundle.class).execute();
            case "ReferralRequest":
                return clientEPR.search().forResource(ReferralRequest.class).where(ReferralRequest.IDENTIFIER.exactly().systemAndCode(identifier.getSystem(),identifier.getValue())).returnBundle(Bundle.class).execute();
            case "RelatedPerson":
                return clientEPR.search().forResource(RelatedPerson.class).where(RelatedPerson.IDENTIFIER.exactly().systemAndCode(identifier.getSystem(),identifier.getValue())).returnBundle(Bundle.class).execute();
            case "Schedule":
                return clientEPR.search().forResource(Schedule.class).where(Schedule.IDENTIFIER.exactly().systemAndCode(identifier.getSystem(),identifier.getValue())).returnBundle(Bundle.class).execute();
            case "Slot":
                return clientEPR.search().forResource(Slot.class).where(Slot.IDENTIFIER.exactly().systemAndCode(identifier.getSystem(),identifier.getValue())).returnBundle(Bundle.class).execute();

            case "Patient":
                return clientEPR.search().forResource(Patient.class).where(Patient.IDENTIFIER.exactly().systemAndCode(identifier.getSystem(),identifier.getValue())).returnBundle(Bundle.class).execute();

            case "Questionnaire":
                return clientEPR.search().forResource(Questionnaire.class).where(Questionnaire.IDENTIFIER.exactly().systemAndCode(identifier.getSystem(),identifier.getValue())).returnBundle(Bundle.class).execute();
            default:
                log.info("Not processed "+resourceName);
        }

        log.info("Query Resource " + resourceType.getSimpleName());

       /* Exchange exchange = template.send("direct:EPRServer", ExchangePattern.InOut, new Processor() {
            public void process(Exchange exchange) throws Exception {
                exchange.getIn().setHeader(Exchange.HTTP_QUERY, );
                exchange.getIn().setHeader(Exchange.HTTP_METHOD, "GET");
                exchange.getIn().setHeader(Exchange.HTTP_PATH, xhttpPath);
            }
        });
        return getResource(exchange); */
       return null;
    }

    public IBaseResource createResource( IBaseResource resource) throws OperationOutcomeException
    {

        log.info("Create "+resource.getClass().getSimpleName());
        MethodOutcome outcome =  clientEPR.create().resource(resource).execute();

        if (outcome.getCreated()) return outcome.getResource();

        processOperationOutcome((OperationOutcome) outcome.getOperationOutcome());

        return null;

    }

    public IBaseResource updateResource( IBaseResource resource) throws OperationOutcomeException
    {
        log.info("Update "+resource.getClass().getSimpleName()+"/"+resource.getIdElement().getIdPart());
        MethodOutcome outcome =  clientEPR.update().resource(resource).execute();

        if (outcome.getOperationOutcome() != null) processOperationOutcome((OperationOutcome) outcome.getOperationOutcome());

        if (outcome.getResource() instanceof OperationOutcome) processOperationOutcome((OperationOutcome) outcome.getResource());


        return outcome.getResource();

    }

/*
    public IBaseResource sendResource( String xhttpMethod, String xhttpPath, Object httpBody) throws OperationOutcomeException
    {
        String httpMethod= xhttpMethod;
        String httpPath = xhttpPath;

        Exchange exchange = template.send("direct:EPRServer", ExchangePattern.InOut, new Processor() {
            public void process(Exchange exchange) throws Exception {
                exchange.getIn().setHeader(Exchange.HTTP_QUERY, "");
                exchange.getIn().setHeader(Exchange.HTTP_METHOD, httpMethod);
                exchange.getIn().setHeader(Exchange.HTTP_PATH, httpPath);
                exchange.getIn().setHeader("Prefer","return=representation");
                exchange.getIn().setHeader(Exchange.CONTENT_TYPE, "application/fhir+json");
                exchange.getIn().setBody(httpBody);
            }
        });
        return getResource(exchange);
    }
    */

    private ListResource searchAddList(String listId, ListResource list) throws OperationOutcomeException {
        log.debug("List searchAdd " +listId);

        if (list == null) throw new InternalErrorException("Bundle processing error");

        ListResource eprListResource = (ListResource) resourceMap.get(listId);

        // Organization already processed, quit with Organization
        if (eprListResource != null) return eprListResource;

        // Prevent re-adding the same Practitioner
        if (list.getIdentifier().size() == 0) {
            list.addIdentifier()
                    .setSystem("urn:uuid")
                    .setValue(list.getId());
        }
        

        for (Identifier identifier : list.getIdentifier()) {
            // org.hl7.fhir.instance.model.api.
            IBaseResource iresource = queryResource(identifier, "List");
               
            if (iresource instanceof Bundle) {
                Bundle returnedBundle = (Bundle) iresource;
                if (returnedBundle.getEntry().size()>0) {
                    eprListResource = (ListResource) returnedBundle.getEntry().get(0).getResource();
                    log.debug("Found ListResource = " + eprListResource.getId());
                }
            }
        }

        if (list.hasEncounter() && checkNotInternalReference(list.getEncounter())) {
            Resource resource = searchAddResource(list.getEncounter().getReference());

            if (resource == null) referenceMissing(list, list.getEncounter().getReference());
            list.setEncounter(getReference(resource));
        }

        if (list.hasSubject() && checkNotInternalReference(list.getSubject())) {
            Resource resource = searchAddResource(list.getSubject().getReference());

            if (resource == null) referenceMissing(list, list.getSubject().getReference());
            list.setSubject(getReference(resource));
        }

        if (list.hasSource()) {
            Resource resource = searchAddResource(list.getSource().getReference());

            if (resource == null) referenceMissing(list, list.getSource().getReference());
            list.setSource(getReference(resource));
        }

        for (ListResource.ListEntryComponent listEntry : list.getEntry()) {
            if (listEntry.hasItem() && checkNotInternalReference(listEntry.getItem())) {
                Resource resource = searchAddResource(listEntry.getItem().getReference());
                if (resource == null) referenceMissing(list, listEntry.getItem().getReference());
                if (resource != null) listEntry.setItem(getReference(resource));
            }
        }


        IBaseResource iResource = null;

        // Location found do not add
        if (eprListResource != null) {

            setResourceMap(listId,eprListResource);
            list.setId(eprListResource.getId());
            iResource = updateResource(list);
        } else {
            iResource = createResource(list);
        }

        if (iResource instanceof ListResource) {
            eprListResource = (ListResource) iResource;
            setResourceMap(listId,eprListResource);

        } else if (iResource instanceof OperationOutcome)
        {
            processOperationOutcome((OperationOutcome) iResource);
        } else {
            throw new InternalErrorException("Unknown Error");
        }

        return eprListResource;
    }



    public Practitioner searchAddPractitioner(String practitionerId, Practitioner practitioner) throws OperationOutcomeException {

        log.debug("Practitioner searchAdd " +practitionerId);

        if (practitioner == null) throw new InternalErrorException("Bundle processing error");

        Practitioner eprPractitioner = (Practitioner) resourceMap.get(practitionerId);

        // Practitioner already processed, quit with Practitioner
        if (eprPractitioner != null) return eprPractitioner;


        // Prevent re-adding the same Practitioner
        if (practitioner.getIdentifier().size() == 0) {
            practitioner.addIdentifier()
                    .setSystem("urn:uuid")
                    .setValue(practitioner.getId());
        }

        for (Identifier identifier : practitioner.getIdentifier()) {
            IBaseResource iresource = queryResource(identifier,"Practitioner");
               
            if (iresource instanceof Bundle) {
                Bundle returnedBundle = (Bundle) iresource;
                if (returnedBundle.getEntry().size() > 0) {
                    eprPractitioner = (Practitioner) returnedBundle.getEntry().get(0).getResource();
                    log.debug("Found Practitioner = " + eprPractitioner.getId());
                }
            }
        }


        if (eprPractitioner != null) {
            setResourceMap(practitionerId,eprPractitioner);
            return eprPractitioner;
        }

        // Practitioner not found. Add to database


        IBaseResource iResource = null;
        //String jsonResource = ctx.newJsonParser().encodeResourceToString(practitioner);

        iResource = createResource(practitioner);

        if (iResource instanceof Practitioner) {
            eprPractitioner = (Practitioner) iResource;
            setResourceMap(practitionerId,eprPractitioner);

        } else if (iResource instanceof OperationOutcome)
        {
            processOperationOutcome((OperationOutcome) iResource);
        } else {
            throw new InternalErrorException("Unknown Error");
        }

        return eprPractitioner;
    }

    public Organization searchAddOrganisation(String organisationId, Organization organisation) throws OperationOutcomeException {
        log.debug("Orgnisation searchAdd " +organisationId);

        if (organisation == null) throw new InternalErrorException("Bundle processing error");

        Organization eprOrganization = (Organization) resourceMap.get(organisationId);

        // Organization already processed, quit with Organization
        if (eprOrganization != null) return eprOrganization;


        // Prevent re-adding the same Organisation
        if (organisation.getIdentifier().size() == 0) {
            organisation.addIdentifier()
                    .setSystem("urn:uuid")
                    .setValue(organisation.getId());
        }

        for (Identifier identifier : organisation.getIdentifier()) {
            IBaseResource iresource = queryResource(identifier,"Organization");
                
            if (iresource instanceof Bundle) {
                Bundle returnedBundle = (Bundle) iresource;
                if (returnedBundle.getEntry().size()>0) {
                    eprOrganization = (Organization) returnedBundle.getEntry().get(0).getResource();
                    log.debug("Found Organization = " + eprOrganization.getId());
                }
            }
        }
        // Organization found do not add
        if (eprOrganization != null) {
            setResourceMap(organisationId,eprOrganization);
            return eprOrganization;
        }

        // Organization not found. Add to database

        if (organisation.getPartOf().getReference() != null) {
            Resource resource = searchAddResource(organisation.getPartOf().getReference());
            log.debug("Found PartOfOrganization = "+resource.getId());

            if (resource == null) referenceMissing(organisation, organisation.getPartOf().getReference());
            organisation.setPartOf(getReference(resource));
        }

        IBaseResource iResource = null;

        iResource = createResource(organisation);

        if (iResource instanceof Organization) {
            eprOrganization = (Organization) iResource;
            setResourceMap(organisationId,eprOrganization);
        } else if (iResource instanceof OperationOutcome)
        {
            processOperationOutcome((OperationOutcome) iResource);
        } else {
            throw new InternalErrorException("Unknown Error");
        }


        return eprOrganization;
    }

    public HealthcareService searchAddHealthcareService(String serviceId, HealthcareService service) throws OperationOutcomeException {
        log.debug("HealthcareService searchAdd " +serviceId);

        if (service== null) throw new InternalErrorException("Bundle processing error");

        HealthcareService eprService = (HealthcareService) resourceMap.get(serviceId);

        // HealthcareService already processed, quit with HealthcareService
        if (eprService != null) return eprService;



        // Prevent re-adding the same HealthcareService
        if (service.getIdentifier().size() == 0) {
            service.addIdentifier()
                    .setSystem("urn:uuid")
                    .setValue(service.getId());
        }

        log.debug("Looking up HealthcareService Service " +serviceId);
        for (Identifier identifier : service.getIdentifier()) {
            IBaseResource iresource = queryResource(identifier,"HealthcareService");
                
            if (iresource instanceof Bundle) {
                Bundle returnedBundle = (Bundle) iresource;
                if (returnedBundle.getEntry().size()>0) {
                    eprService = (HealthcareService) returnedBundle.getEntry().get(0).getResource();
                    log.debug("Found HealthcareService = " + eprService.getId());
                }
            }
        }
        log.debug("Adding HealthcareService Service " +serviceId);
        // HealthcareService found do not add
        if (eprService != null) {
            setResourceMap(serviceId,eprService);
            return eprService;
        }

        // HealthcareService not found. Add to database

        if (service.getProvidedBy().getReference() != null) {
            Resource resource = searchAddResource(service.getProvidedBy().getReference());

            log.debug("Found PartOf HealthcareService = "+resource.getId());
            if (resource == null) referenceMissing(service, service.getProvidedBy().getReference());
            service.setProvidedBy(getReference(resource));
        }

        List<Reference> locations = new ArrayList<>();
        for (Reference reference : service.getLocation()) {
            if (reference.getReference() != null) {
                Resource resource = searchAddResource(reference.getReference());

                log.debug("Found Location Reference HealthcareService = " + resource.getId());
                if (resource == null) referenceMissing(service, reference.getReference());
                locations.add(getReference(resource));
            }
        }
        service.setLocation(locations);


        IBaseResource iResource = null;
        //String jsonResource = ctx.newJsonParser().encodeResourceToString();
        iResource = createResource(service);

        if (iResource instanceof HealthcareService) {
            eprService = (HealthcareService) iResource;
            setResourceMap(serviceId,eprService);
            return eprService;
        } else if (iResource instanceof OperationOutcome)
        {
            processOperationOutcome((OperationOutcome) iResource);
        } else {
            throw new InternalErrorException("Unknown Error");
        }


        return null;
    }

    public Location searchAddLocation(String locationId, Location location) throws OperationOutcomeException {
        log.debug("Location searchAdd " +locationId);

        if (location == null) throw new InternalErrorException("Bundle processing error");

        Location eprLocation = (Location) resourceMap.get(locationId);

        // Organization already processed, quit with Organization
        if (eprLocation != null) return eprLocation;

        // Prevent re-adding the same Practitioner
        if (location.getIdentifier().size() == 0) {
            location.addIdentifier()
                    .setSystem("urn:uuid")
                    .setValue(location.getId());
        }


        for (Identifier identifier : location.getIdentifier()) {
            IBaseResource iresource = queryResource(identifier, "Location");
              if (iresource instanceof Bundle) {
                Bundle returnedBundle = (Bundle) iresource;
                if (returnedBundle.getEntry().size()>0) {
                    eprLocation = (Location) returnedBundle.getEntry().get(0).getResource();
                    log.debug("Found Location = " + eprLocation.getId());
                }
            }
        }


        // Location not found. Add to database

        if (location.getManagingOrganization().getReference() != null) {
            Resource resource = searchAddResource(location.getManagingOrganization().getReference());

            if (resource == null) referenceMissing(location, location.getManagingOrganization().getReference());
            location.setManagingOrganization(getReference(resource));
        }

        IBaseResource iResource = null;


        String xhttpMethod = "POST";
        String xhttpPath = "Location";
        // Location found do not add
        if (eprLocation != null) {

            setResourceMap(locationId,eprLocation);


            location.setId(eprLocation.getId());
            iResource = updateResource(location);
        } else {


            iResource = createResource(location);
        }
        if (iResource instanceof Location) {
            eprLocation = (Location) iResource;
            setResourceMap(locationId,eprLocation);
        } else if (iResource instanceof OperationOutcome)
        {
            processOperationOutcome((OperationOutcome) iResource);
        } else {
            throw new InternalErrorException("Unknown Error");
        }

        return eprLocation;
    }

    public AllergyIntolerance searchAddAllergyIntolerance(String allergyIntoleranceId, AllergyIntolerance allergyIntolerance) throws OperationOutcomeException {
        log.debug("AllergyIntolerance searchAdd " +allergyIntoleranceId);

        if (allergyIntolerance == null) throw new InternalErrorException("Bundle processing error");

        AllergyIntolerance eprAllergyIntolerance = (AllergyIntolerance) resourceMap.get(allergyIntoleranceId);

        // Organization already processed, quit with Organization
        if (eprAllergyIntolerance != null) return eprAllergyIntolerance;

        // Prevent re-adding the same Practitioner
        if (allergyIntolerance.getIdentifier().size() == 0) {
            allergyIntolerance.addIdentifier()
                    .setSystem("urn:uuid")
                    .setValue(allergyIntolerance.getId());
        }



        for (Identifier identifier : allergyIntolerance.getIdentifier()) {
            IBaseResource iresource = queryResource(identifier, "AllergyIntolerance");
              
            if (iresource instanceof Bundle) {
                Bundle returnedBundle = (Bundle) iresource;
                if (returnedBundle.getEntry().size()>0) {
                    eprAllergyIntolerance = (AllergyIntolerance) returnedBundle.getEntry().get(0).getResource();
                    log.debug("Found AllergyIntolerance = " + eprAllergyIntolerance.getId());
                }
            }
        }

        if (allergyIntolerance.getAsserter().getReference() != null && checkNotInternalReference(allergyIntolerance.getAsserter())) {

            Resource resource = searchAddResource(allergyIntolerance.getAsserter().getReference());

            log.debug("Found Practitioner = " + resource.getId());
            if (resource == null) referenceMissing(allergyIntolerance, allergyIntolerance.getAsserter().getReference());
            allergyIntolerance.setAsserter(getReference(resource));

        }
        if (allergyIntolerance.getPatient() != null && checkNotInternalReference(allergyIntolerance.getPatient())) {
            Resource resource = searchAddResource(allergyIntolerance.getPatient().getReference());

            if (resource == null) referenceMissing(allergyIntolerance, allergyIntolerance.getPatient().getReference());
            allergyIntolerance.setPatient(getReference(resource));
        }

        IBaseResource iResource = null;

        // Location found do not add
        if (eprAllergyIntolerance != null) {

            setResourceMap(allergyIntoleranceId,eprAllergyIntolerance);

            allergyIntolerance.setId(eprAllergyIntolerance.getId());
            iResource = updateResource(allergyIntolerance);
        } else {

            iResource = createResource(allergyIntolerance);
        }

        if (iResource instanceof AllergyIntolerance) {
            eprAllergyIntolerance = (AllergyIntolerance) iResource;
            setResourceMap(allergyIntoleranceId,eprAllergyIntolerance);

        } else if (iResource instanceof OperationOutcome)
        {
            processOperationOutcome((OperationOutcome) iResource);
        } else {
            throw new InternalErrorException("Unknown Error");
        }

        return eprAllergyIntolerance;
    }

    public CarePlan searchAddCarePlan(String carePlanId, CarePlan carePlan) throws OperationOutcomeException {
        log.debug("CarePlan searchAdd " +carePlanId);

        if (carePlan == null) throw new InternalErrorException("Bundle processing error");

        CarePlan eprCarePlan = (CarePlan) resourceMap.get(carePlanId);

        // Organization already processed, quit with Organization
        if (eprCarePlan != null) return eprCarePlan;

        // Prevent re-adding the same Practitioner
        if (carePlan.getIdentifier().size() == 0) {
            carePlan.addIdentifier()
                    .setSystem("urn:uuid")
                    .setValue(carePlan.getId());
        }


        for (Identifier identifier : carePlan.getIdentifier()) {
            IBaseResource iresource = queryResource(identifier, "CarePlan");
            
            if (iresource instanceof Bundle) {
                Bundle returnedBundle = (Bundle) iresource;
                if (returnedBundle.getEntry().size()>0) {
                    eprCarePlan = (CarePlan) returnedBundle.getEntry().get(0).getResource();
                    log.debug("Found CarePlan = " + eprCarePlan.getId());
                }
            }
        }

        if (carePlan.getContext().getReference() != null) {
            Resource resource = searchAddResource(carePlan.getContext().getReference());

            if (resource == null) referenceMissing(carePlan, carePlan.getContext().getReference());
            carePlan.setContext(getReference(resource));
        }
        if (carePlan.getSubject() != null) {
            Resource resource = searchAddResource(carePlan.getSubject().getReference());

            if (resource == null) referenceMissing(carePlan, carePlan.getSubject().getReference());
            carePlan.setSubject(getReference(resource));
        }
        List<Reference> references = new ArrayList<>();
        for (Reference reference : carePlan.getAddresses()) {
            Resource resource = searchAddResource(reference.getReference());

            if (resource!=null) references.add(getReference(resource));
        }
        carePlan.setAddresses(references);

        references = new ArrayList<>();
        for (Reference reference : carePlan.getAuthor()) {
            Resource resource = searchAddResource(reference.getReference());

            if (resource!=null) references.add(getReference(resource));
        }
        carePlan.setAuthor(references);

        List<Reference> referenceTeam = new ArrayList<>();
        for (Reference reference : carePlan.getCareTeam()) {
            Resource resource = searchAddResource(reference.getReference());
            if (resource!=null) referenceTeam.add(getReference(resource));
        }
        carePlan.setCareTeam(referenceTeam);

        List<Reference> referenceSupporting = new ArrayList<>();
        for (Reference reference : carePlan.getSupportingInfo()) {
            Resource resource = searchAddResource(reference.getReference());
            if (resource == null) referenceMissing(carePlan, reference.getReference());
            if (resource!=null) referenceSupporting.add(getReference(resource));
        }
        carePlan.setSupportingInfo(referenceSupporting);

        IBaseResource iResource = null;

        // Location found do not add
        if (eprCarePlan != null) {
            setResourceMap(carePlanId,eprCarePlan);
            carePlan.setId(eprCarePlan.getId());
            iResource = updateResource(carePlan);
        } else {
            iResource = createResource(carePlan);
        }




        if (iResource instanceof CarePlan) {
            eprCarePlan = (CarePlan) iResource;
            setResourceMap(carePlanId,eprCarePlan);

        } else if (iResource instanceof OperationOutcome)
        {
            processOperationOutcome((OperationOutcome) iResource);
        } else {
            throw new InternalErrorException("Unknown Error");
        }

        return eprCarePlan;
    }

    public CareTeam searchAddCareTeam(String careTeamId, CareTeam careTeam) throws OperationOutcomeException {
        log.debug("CareTeam searchAdd " +careTeamId);

        if (careTeam == null) throw new InternalErrorException("Bundle processing error");

        CareTeam eprCareTeam = (CareTeam) resourceMap.get(careTeamId);

        // Organization already processed, quit with Organization
        if (eprCareTeam != null) return eprCareTeam;

        // Prevent re-adding the same Practitioner
        if (careTeam.getIdentifier().size() == 0) {
            careTeam.addIdentifier()
                    .setSystem("urn:uuid")
                    .setValue(careTeam.getId());
        }


        for (Identifier identifier : careTeam.getIdentifier()) {
            IBaseResource iresource = queryResource(identifier,"CareTeam");
            
            if (iresource instanceof Bundle) {
                Bundle returnedBundle = (Bundle) iresource;
                if (returnedBundle.getEntry().size()>0) {
                    eprCareTeam = (CareTeam) returnedBundle.getEntry().get(0).getResource();
                    log.debug("Found CareTeam = " + eprCareTeam.getId());
                }
            }
        }

        if (careTeam.getContext().getReference() != null) {
            Resource resource = searchAddResource(careTeam.getContext().getReference());

            if (resource == null) referenceMissing(careTeam, careTeam.getContext().getReference());
            careTeam.setContext(getReference(resource));
        }
        if (careTeam.getParticipant() != null) {
            for (CareTeam.CareTeamParticipantComponent participant : careTeam.getParticipant()) {
                if (participant.hasMember()) {
                    Resource resource = searchAddResource(participant.getMember().getReference());
                    if (resource == null) referenceMissing(careTeam, participant.getMember().getReference());
                    participant.setMember(getReference(resource));
                }
            }
        }
        if (careTeam.hasManagingOrganization()) {
            List<Reference> orgs = new ArrayList<>();
            for (Reference reference : careTeam.getManagingOrganization()) {
                Resource resource = searchAddResource(reference.getReference());
                if (resource == null) referenceMissing(careTeam, reference.getReference());
                orgs.add(getReference(resource));
            }
            careTeam.setManagingOrganization(orgs);
        }
        if (careTeam.getSubject() != null) {
            Resource resource = searchAddResource(careTeam.getSubject().getReference());

            if (resource == null) referenceMissing(careTeam, careTeam.getSubject().getReference());
            careTeam.setSubject(getReference(resource));
        }
        List<Reference> references = new ArrayList<>();
        for (Reference reference : careTeam.getReasonReference()) {
            Resource resource = searchAddResource(reference.getReference());

            if (resource!=null) references.add(getReference(resource));
        }
        careTeam.setReasonReference(references);

        IBaseResource iResource = null;

        // Location found do not add
        if (eprCareTeam != null) {

            setResourceMap(careTeamId,eprCareTeam);
            // Want id value, no path or resource

            careTeam.setId(eprCareTeam.getId());
            iResource = updateResource(careTeam);
        } else {
            iResource = createResource(careTeam);
        }




        if (iResource instanceof CareTeam) {
            eprCareTeam = (CareTeam) iResource;
            setResourceMap(careTeamId,eprCareTeam);

        } else if (iResource instanceof OperationOutcome)
        {
            processOperationOutcome((OperationOutcome) iResource);
        } else {
            throw new InternalErrorException("Unknown Error");
        }

        return eprCareTeam;
    }


    public QuestionnaireResponse searchAddQuestionnaireResponse(String formId, QuestionnaireResponse form) throws OperationOutcomeException {
        log.debug("QuestionnaireResponse searchAdd " +formId);

        if (form == null) throw new InternalErrorException("Bundle processing error");

        QuestionnaireResponse eprQuestionnaireResponse = (QuestionnaireResponse) resourceMap.get(formId);

        // Organization already processed, quit with Organization
        if (eprQuestionnaireResponse != null) return eprQuestionnaireResponse;

        // Prevent re-adding the same Practitioner
        if (!form.hasIdentifier()) {
            form.getIdentifier()
                    .setSystem("urn:uuid")
                    .setValue(form.getId());
        }


        Identifier identifier = form.getIdentifier();
        
        IBaseResource iresource = queryResource(identifier,"QuestionnaireResponse");
        
        if (iresource instanceof Bundle) {
            Bundle returnedBundle = (Bundle) iresource;
            if (returnedBundle.getEntry().size()>0) {
                eprQuestionnaireResponse = (QuestionnaireResponse) returnedBundle.getEntry().get(0).getResource();
                log.debug("Found QuestionnaireResponse = " + eprQuestionnaireResponse.getId());
            }
        }


        if (form.getContext().getReference() != null) {
            Resource resource = searchAddResource(form.getContext().getReference());

            if (resource == null) referenceMissing(form, form.getContext().getReference());
            form.setContext(getReference(resource));
        }
        if (form.getSubject() != null) {
            Resource resource = searchAddResource(form.getSubject().getReference());

            if (resource == null) referenceMissing(form, form.getSubject().getReference());
            form.setSubject(getReference(resource));
        }
        if (form.hasSource()) {
            Resource resource = searchAddResource(form.getSource().getReference());

            if (resource == null) referenceMissing(form, form.getSource().getReference());
            form.setSource(getReference(resource));
        }
        /*

        Just use URL - Assumption is the Questionnaire will be on a seperate system

        if (form.hasQuestionnaire() && form.getQuestionnaire().getReference() != null) {
            Resource resource = searchAddResource(form.getQuestionnaire().getReference());

            if (resource == null) referenceMissing(form, form.getQuestionnaire().getReference());
            form.setQuestionnaire(getReference(resource));
        }
        */
        if (form.hasAuthor()) {
            Resource resource = searchAddResource(form.getAuthor().getReference());

            if (resource == null) referenceMissing(form, form.getAuthor().getReference());
            form.setAuthor(getReference(resource));
        }
        if (form.hasItem()) {
            for (QuestionnaireResponse.QuestionnaireResponseItemComponent itemComponent :form.getItem()) {
                questionnaireItem(itemComponent,form);
            }
        }


        IBaseResource iResource = null;

        // Location found do not add
        if (eprQuestionnaireResponse != null) {

            setResourceMap(formId,eprQuestionnaireResponse);
            // Want id value, no path or resource
            form.setId(eprQuestionnaireResponse.getId());
            iResource = updateResource(form);
        } else {
            iResource = createResource(form);
        }




        if (iResource instanceof QuestionnaireResponse) {
            eprQuestionnaireResponse = (QuestionnaireResponse) iResource;
            setResourceMap(formId,eprQuestionnaireResponse);

        } else if (iResource instanceof OperationOutcome)
        {
            processOperationOutcome((OperationOutcome) iResource);
        } else {
            throw new InternalErrorException("Unknown Error");
        }

        return eprQuestionnaireResponse;
    }


    public QuestionnaireResponse questionnaireItem(QuestionnaireResponse.QuestionnaireResponseItemComponent itemComponent, QuestionnaireResponse form) throws OperationOutcomeException {

            if (itemComponent.hasAnswer()) {
                for (QuestionnaireResponse.QuestionnaireResponseItemAnswerComponent answerComponent : itemComponent.getAnswer()) {
                    if (answerComponent.hasValueReference()) {

                            Resource resource = searchAddResource(answerComponent.getValueReference().getReference());
                            if (resource instanceof OperationOutcome)
                            {
                                processOperationOutcome((OperationOutcome) resource);
                            }
                            if (resource == null) referenceMissing(form, answerComponent.getValueReference().getReference());
                            answerComponent.setValue(getReference(resource));

                    }
                }
            }
            if (itemComponent.hasItem()) {
                for (QuestionnaireResponse.QuestionnaireResponseItemComponent subItem : itemComponent.getItem()) {
                    questionnaireItem(subItem,form);
                }
            }
            return null;
    }


    public Observation searchAddObservation(String observationId, Observation observation) throws OperationOutcomeException {
        log.debug("Observation searchAdd " +observationId);

        if (observation == null) throw new InternalErrorException("Bundle processing error");

        Observation eprObservation = (Observation) resourceMap.get(observationId);

        // Organization already processed, quit with Organization
        if (eprObservation != null) return eprObservation;

        // Prevent re-adding the same Practitioner
        if (observation.getIdentifier().size() == 0) {
            observation.addIdentifier()
                    .setSystem("urn:uuid")
                    .setValue(observation.getId());
        }

      

      

        for (Identifier identifier : observation.getIdentifier()) {
            IBaseResource iresource = queryResource(identifier,"Observation");
            
            if (iresource instanceof Bundle) {
                Bundle returnedBundle = (Bundle) iresource;
                if (returnedBundle.getEntry().size()>0) {
                    eprObservation = (Observation) returnedBundle.getEntry().get(0).getResource();
                    log.debug("Found Observation = " + eprObservation.getId());
                }
            }
        }


        // Location not found. Add to database

        List<Reference> performers = new ArrayList<>();
        for (Reference reference : observation.getPerformer()) {
            Resource resource = searchAddResource(reference.getReference());
            if (resource!=null) performers.add(getReference(resource));
        }
        observation.setPerformer(performers);

        if (observation.hasSubject()) {
            Resource resource = searchAddResource(observation.getSubject().getReference());

            if (resource == null) referenceMissing(observation, observation.getSubject().getReference());
            observation.setSubject(getReference(resource));
        }
        if (observation.hasContext()) {
            Resource resource = searchAddResource(observation.getContext().getReference());

            if (resource == null) referenceMissing(observation, observation.getContext().getReference());
            observation.setContext(getReference(resource));
        }
        if (observation.hasRelated()) {
            for (Observation.ObservationRelatedComponent relatedComponent : observation.getRelated()) {
                if (relatedComponent.hasTarget()) {
                    relatedComponent.setTarget(getReference(searchAddResource(relatedComponent.getTarget().getReference())));
                }
            }
        }

        IBaseResource iResource = null;

        // Location found do not add
        if (eprObservation != null) {

            setResourceMap(observationId,eprObservation);
            // Want id value, no path or resource

            observation.setId(eprObservation.getId());
            iResource = updateResource(observation);
        } else {
            iResource = createResource(observation);
        }


        if (iResource instanceof Observation) {
            eprObservation = (Observation) iResource;
            setResourceMap(observationId,eprObservation);

        } else if (iResource instanceof OperationOutcome)
        {
            processOperationOutcome((OperationOutcome) iResource);
        } else {
            throw new InternalErrorException("Unknown Error");
        }

        return eprObservation;
    }




    public DiagnosticReport searchAddDiagnosticReport(String diagnosticReportId, DiagnosticReport diagnosticReport) throws OperationOutcomeException {
        log.debug("DiagnosticReport searchAdd " +diagnosticReportId);

        if (diagnosticReport == null) throw new InternalErrorException("Bundle processing error");

        DiagnosticReport eprDiagnosticReport = (DiagnosticReport) resourceMap.get(diagnosticReportId);

        // Organization already processed, quit with Organization
        if (eprDiagnosticReport != null) return eprDiagnosticReport;

        // Prevent re-adding the same Practitioner
        if (diagnosticReport.getIdentifier().size() == 0) {
            diagnosticReport.addIdentifier()
                    .setSystem("urn:uuid")
                    .setValue(diagnosticReport.getId());
        }



        for (Identifier identifier : diagnosticReport.getIdentifier()) {
            
            IBaseResource iresource = queryResource(identifier,"DiagnosticReport");
            
            if (iresource instanceof Bundle) {
                
                Bundle returnedBundle = (Bundle) iresource;
                if (returnedBundle.getEntry().size()>0) {
                    eprDiagnosticReport = (DiagnosticReport) returnedBundle.getEntry().get(0).getResource();
                    log.debug("Found DiagnosticReport = " + eprDiagnosticReport.getId());
                }
            }
        }


        // Location not found. Add to database


        for (DiagnosticReport.DiagnosticReportPerformerComponent performer : diagnosticReport.getPerformer())  {
            Resource resource = searchAddResource(performer.getActor().getReference());

            if (resource == null) referenceMissing(diagnosticReport, performer.getActor().getReference());
            performer.setActor(getReference(resource));
        }


        if (diagnosticReport.hasSubject()) {
            Resource resource = searchAddResource(diagnosticReport.getSubject().getReference());

            if (resource == null) referenceMissing(diagnosticReport, diagnosticReport.getSubject().getReference());
            diagnosticReport.setSubject(getReference(resource));
        }
        if (diagnosticReport.hasContext()) {
            Resource resource = searchAddResource(diagnosticReport.getContext().getReference());
            if (resource == null) referenceMissing(diagnosticReport, diagnosticReport.getContext().getReference());
            diagnosticReport.setContext(getReference(resource));
        }

        List<Reference> results = new ArrayList<>();
        for (Reference reference : diagnosticReport.getResult()) {
            Resource resource = searchAddResource(reference.getReference());
            if (resource == null) referenceMissing(diagnosticReport, reference.getReference());
            if (resource!=null) results.add(getReference(resource));
        }
        diagnosticReport.setResult(results);

        IBaseResource iResource = null;


        // Location found do not add
        if (eprDiagnosticReport != null) {
            setResourceMap(diagnosticReportId,eprDiagnosticReport);
            // Want id value, no path or resource

            diagnosticReport.setId(eprDiagnosticReport.getId());
            iResource = updateResource(diagnosticReport);
        } else {
            iResource = createResource(diagnosticReport);
        }



        if (iResource instanceof DiagnosticReport) {
            eprDiagnosticReport = (DiagnosticReport) iResource;
            setResourceMap(diagnosticReportId,eprDiagnosticReport);

        } else if (iResource instanceof OperationOutcome)
        {
            processOperationOutcome((OperationOutcome) iResource);
        } else {
            throw new InternalErrorException("Unknown Error");
        }

        return eprDiagnosticReport;
    }

    // KGM 22/Jan/2018 Added Immunization processing

    public Immunization searchAddImmunization(String immunisationId, Immunization immunisation) throws OperationOutcomeException {
        log.debug("Immunization searchAdd " +immunisationId);

        if (immunisation == null) throw new InternalErrorException("Bundle processing error");

        Immunization eprImmunization = (Immunization) resourceMap.get(immunisationId);

        // Organization already processed, quit with Organization
        if (eprImmunization != null) return eprImmunization;

        // Prevent re-adding the same Practitioner
        if (immunisation.getIdentifier().size() == 0) {
            // Use a custom identifier
            immunisation.addIdentifier()
                    .setSystem("urn:uuid")
                    .setValue(immunisation.getDate().toString()+"-"+immunisation.getVaccineCode().getCodingFirstRep().getCode());
        }


        for (Identifier identifier : immunisation.getIdentifier()) {
            
            IBaseResource iresource = queryResource(identifier,"Immunization");
            
            if (iresource instanceof Bundle) {
                Bundle returnedBundle = (Bundle) iresource;
                if (returnedBundle.getEntry().size()>0) {
                    eprImmunization = (Immunization) returnedBundle.getEntry().get(0).getResource();
                    log.debug("Found Immunization = " + eprImmunization.getId());
                }
            }
        }


        // Location not found. Add to database


        for (Immunization.ImmunizationPractitionerComponent reference : immunisation.getPractitioner()) {
            Resource resource = searchAddResource(reference.getActor().getReference());

            if (resource!=null) reference.setActor(getReference(resource));
        }

        if (immunisation.hasPatient()) {
            Resource resource = searchAddResource(immunisation.getPatient().getReference());
            if (resource == null) referenceMissing(immunisation, immunisation.getPatient().getReference());
            immunisation.setPatient(getReference(resource));
        }
        if (immunisation.hasEncounter()) {
            Resource resource = searchAddResource(immunisation.getEncounter().getReference());
            if (resource == null) referenceMissing(immunisation, immunisation.getEncounter().getReference());
            immunisation.setEncounter(getReference(resource));
        }

        IBaseResource iResource = null;


        // Location found do not add
        if (eprImmunization != null) {

            setResourceMap(immunisationId,eprImmunization);
            // Want id value, no path or resource
            immunisation.setId(eprImmunization.getId());
            iResource = updateResource(immunisation);
        } else {
            iResource = createResource(immunisation);
        }



        if (iResource instanceof Immunization) {
            eprImmunization = (Immunization) iResource;
            setResourceMap(immunisationId,eprImmunization);

        } else if (iResource instanceof OperationOutcome)
        {
            processOperationOutcome((OperationOutcome) iResource);
        } else {
            throw new InternalErrorException("Unknown Error");
        }

        return eprImmunization;
    }



     public MedicationRequest searchAddMedicationRequest(String medicationRequestId, MedicationRequest medicationRequest) throws OperationOutcomeException {
        log.debug("MedicationRequest searchAdd " +medicationRequestId);

        if (medicationRequest == null) throw new InternalErrorException("Bundle processing error");

        MedicationRequest eprMedicationRequest = (MedicationRequest) resourceMap.get(medicationRequestId);

        // Organization already processed, quit with Organization
        if (eprMedicationRequest != null) return eprMedicationRequest;

        // Prevent re-adding the same Practitioner
        if (medicationRequest.getIdentifier().size() == 0) {
            medicationRequest.addIdentifier()
                    .setSystem("urn:uuid")
                    .setValue(medicationRequest.getId());
        }



         for (Identifier identifier : medicationRequest.getIdentifier()) {
            IBaseResource iresource = queryResource(identifier,"MedicationRequest");
               
            if (iresource instanceof Bundle) {
                Bundle returnedBundle = (Bundle) iresource;
                if (returnedBundle.getEntry().size()>0) {
                    eprMedicationRequest = (MedicationRequest) returnedBundle.getEntry().get(0).getResource();
                    log.debug("Found MedicationRequest = " + eprMedicationRequest.getId());
                }
            }
        }


        // Location not found. Add to database



        if (medicationRequest.hasSubject()) {
            Resource resource = searchAddResource(medicationRequest.getSubject().getReference());
            if (resource == null) referenceMissing(medicationRequest, medicationRequest.getSubject().getReference());
            medicationRequest.setSubject(getReference(resource));
        }
        if (medicationRequest.hasContext()) {
            Resource resource = searchAddResource(medicationRequest.getContext().getReference());
            if (resource == null) referenceMissing(medicationRequest, medicationRequest.getContext().getReference());
            medicationRequest.setContext(getReference(resource));
        }

        if (medicationRequest.hasRequester()) {
            Resource resource = searchAddResource(medicationRequest.getRequester().getAgent().getReference());
            if (resource == null) referenceMissing(medicationRequest, medicationRequest.getRequester().getAgent().getReference());
            medicationRequest.getRequester().setAgent(getReference(resource));
        }

        if (medicationRequest.hasMedicationReference()) {
            Resource resource = null;
            String reference = "";
            try {
                reference = medicationRequest.getMedicationReference().getReference();
                resource = searchAddResource(reference);
                if (resource == null) referenceMissing(medicationRequest, medicationRequest.getMedicationReference().getReference());
                medicationRequest.setMedication(getReference(resource));
            } catch (Exception exMed) {}

        }

        IBaseResource iResource = null;


        // Location found do not add
        if (eprMedicationRequest != null) {

            setResourceMap(medicationRequestId,eprMedicationRequest);
            // Want id value, no path or resource

            medicationRequest.setId(eprMedicationRequest.getId());
            iResource = updateResource(medicationRequest);
        } else {
            iResource = createResource(medicationRequest);
        }


        if (iResource instanceof MedicationRequest) {
            eprMedicationRequest = (MedicationRequest) iResource;
            setResourceMap(medicationRequestId,eprMedicationRequest);

        } else if (iResource instanceof OperationOutcome)
        {
            processOperationOutcome((OperationOutcome) iResource);
        } else {
            throw new InternalErrorException("Unknown Error");
        }

        return eprMedicationRequest;
    }

    public Medication searchAddMedication(String medicationId, Medication medication) throws OperationOutcomeException {
        log.debug("Medication searchAdd " +medicationId);

        if (medication == null) throw new InternalErrorException("Bundle processing error");

        org.hl7.fhir.dstu3.model.Medication eprMedication = (org.hl7.fhir.dstu3.model.Medication) resourceMap.get(medicationId);

        // Organization already processed, quit with Organization
        if (eprMedication != null) return eprMedication;


        InputStream inputStream = null;

        for (Coding code : medication.getCode().getCoding()) {


            IBaseResource iresource = clientEPR.search().forResource(Medication.class).where(Medication.CODE.exactly().systemAndCode(code.getSystem(),code.getCode())).returnBundle(Bundle.class).execute();

            if (iresource instanceof OperationOutcome) {
                processOperationOutcome((OperationOutcome) iresource);
            } else
            if (iresource instanceof Bundle) {
                Bundle returnedBundle = (Bundle) iresource;
                if (returnedBundle.getEntry().size()>0) {
                    eprMedication = (org.hl7.fhir.dstu3.model.Medication) returnedBundle.getEntry().get(0).getResource();
                    log.debug("Found Medication = " + eprMedication.getId());
                }
            }
        }


        // Location not found. Add to database





        IBaseResource iResource = null;

        // Location found do not add
        if (eprMedication != null) {

            setResourceMap(medicationId, eprMedication);
            // Want id value, no path or resource
            medication.setId(eprMedication.getId());
            iResource = updateResource(medication);
        } else {
            iResource = createResource(medication);
        }



        if (iResource instanceof org.hl7.fhir.dstu3.model.Medication) {
            eprMedication = (org.hl7.fhir.dstu3.model.Medication) iResource;
            setResourceMap(medicationId,eprMedication);

        } else if (iResource instanceof OperationOutcome)
        {
            processOperationOutcome((OperationOutcome) iResource);
        } else {
            throw new InternalErrorException("Unknown Error");
        }

        return eprMedication;
    }



    public RiskAssessment searchAddRiskAssessment(String riskAssessmentId, RiskAssessment riskAssessment) throws OperationOutcomeException {
        log.debug("RiskAssessment searchAdd " +riskAssessmentId);

        if (riskAssessment == null) throw new InternalErrorException("Bundle processing error");

        RiskAssessment eprRiskAssessment = (RiskAssessment) resourceMap.get(riskAssessmentId);

        // Organization already processed, quit with Organization
        if (eprRiskAssessment != null) return eprRiskAssessment;

        // Prevent re-adding the same Practitioner
        if (!riskAssessment.hasIdentifier()) {
            riskAssessment.getIdentifier()
                    .setSystem("urn:uuid")
                    .setValue(riskAssessment.getId());
        }


        if (riskAssessment.hasIdentifier()) {
            Identifier identifier = riskAssessment.getIdentifier();
            IBaseResource iresource = queryResource(identifier,"RiskAssessment");
              
            if (iresource instanceof Bundle) {
                Bundle returnedBundle = (Bundle) iresource;
                if (returnedBundle.getEntry().size()>0) {
                    eprRiskAssessment = (RiskAssessment) returnedBundle.getEntry().get(0).getResource();
                    log.debug("Found RiskAssessment = " + eprRiskAssessment.getId());
                }
            }
        }


        // Location not found. Add to database



        if (riskAssessment.hasSubject()) {
            Resource resource = searchAddResource(riskAssessment.getSubject().getReference());
            if (resource == null) referenceMissing(riskAssessment, riskAssessment.getSubject().getReference());
            riskAssessment.setSubject(getReference(resource));
        }
        if (riskAssessment.hasContext()) {
            Resource resource = searchAddResource(riskAssessment.getContext().getReference());
            if (resource == null) referenceMissing(riskAssessment, riskAssessment.getContext().getReference());
            riskAssessment.setContext(getReference(resource));
        }
        if (riskAssessment.hasCondition()) {
            Resource resource = searchAddResource(riskAssessment.getCondition().getReference());
            if (resource == null) referenceMissing(riskAssessment, riskAssessment.getCondition().getReference());
            riskAssessment.setCondition(getReference(resource));
        }

        IBaseResource iResource = null;


        // Location found do not add
        if (eprRiskAssessment != null) {

            setResourceMap(riskAssessmentId,eprRiskAssessment);
            // Want id value, no path or resource
            riskAssessment.setId(eprRiskAssessment.getId());
            iResource = updateResource(riskAssessment);
        } else {
            iResource =createResource(riskAssessment);
        }




        if (iResource instanceof RiskAssessment) {
            eprRiskAssessment = (RiskAssessment) iResource;
            setResourceMap(riskAssessmentId,eprRiskAssessment);

        } else if (iResource instanceof OperationOutcome)
        {
            processOperationOutcome((OperationOutcome) iResource);
        } else {
            throw new InternalErrorException("Unknown Error");
        }

        return eprRiskAssessment;
    }


    public ClinicalImpression searchAddClinicalImpression(String impressionId, ClinicalImpression impression) throws OperationOutcomeException {
        log.debug("ClinicalImpression searchAdd " +impressionId);

        if (impression == null) throw new InternalErrorException("Bundle processing error");

        ClinicalImpression eprClinicalImpression = (ClinicalImpression) resourceMap.get(impressionId);

        // Organization already processed, quit with Organization
        if (eprClinicalImpression != null) return eprClinicalImpression;

        // Prevent re-adding the same Practitioner
        if (!impression.hasIdentifier()) {
            impression.addIdentifier()
                    .setSystem("urn:uuid")
                    .setValue(impression.getId());
        }


        for (Identifier identifier : impression.getIdentifier()) {
            IBaseResource iresource = queryResource(identifier,"ClinicalImpression");
               
            if (iresource instanceof Bundle) {
                Bundle returnedBundle = (Bundle) iresource;
                if (returnedBundle.getEntry().size()>0) {
                    eprClinicalImpression = (ClinicalImpression) returnedBundle.getEntry().get(0).getResource();
                    log.debug("Found ClinicalImpression = " + eprClinicalImpression.getId());
                }
            }
        }


        // Location not found. Add to database



        if (impression.hasSubject()) {
            Resource resource = searchAddResource(impression.getSubject().getReference());
            if (resource == null) referenceMissing(impression, impression.getSubject().getReference());
            impression.setSubject(getReference(resource));
        }
        if (impression.hasContext()) {
            Resource resource = searchAddResource(impression.getContext().getReference());
            if (resource == null) referenceMissing(impression, impression.getContext().getReference());
            impression.setContext(getReference(resource));
        }
        if (impression.hasAssessor()) {
            Resource resource = searchAddResource(impression.getAssessor().getReference());
            if (resource == null) referenceMissing(impression, impression.getAssessor().getReference());
            impression.setAssessor(getReference(resource));
        }


        IBaseResource iResource = null;

        String xhttpMethod = "POST";
        String xhttpPath = "ClinicalImpression";
        // Location found do not add
        if (eprClinicalImpression != null) {
            xhttpMethod="PUT";
            setResourceMap(impressionId,eprClinicalImpression);
            // Want id value, no path or resource
            xhttpPath = "ClinicalImpression/"+eprClinicalImpression.getIdElement().getIdPart();
            impression.setId(eprClinicalImpression.getId());
            iResource = updateResource(impression);
        } else {
            iResource = createResource(impression);
        }

        if (iResource instanceof ClinicalImpression) {
            eprClinicalImpression = (ClinicalImpression) iResource;
            setResourceMap(impressionId,eprClinicalImpression);

        } else if (iResource instanceof OperationOutcome)
        {
            processOperationOutcome((OperationOutcome) iResource);
        } else {
            throw new InternalErrorException("Unknown Error");
        }

        return eprClinicalImpression;
    }

    public Consent searchAddConsent(String consentId, Consent consent) throws OperationOutcomeException {
        log.debug("Consent searchAdd " +consentId);

        if (consent == null) throw new InternalErrorException("Bundle processing error");

        Consent eprConsent = (Consent) resourceMap.get(consentId);

        // Organization already processed, quit with Organization
        if (eprConsent != null) return eprConsent;

        // Prevent re-adding the same Practitioner
        if (!consent.hasIdentifier()) {
            consent.getIdentifier()
                    .setSystem("urn:uuid")
                    .setValue(consent.getId());
        }



        if (consent.hasIdentifier()) {
            Identifier identifier = consent.getIdentifier();
            IBaseResource iresource = queryResource(identifier,"Consent");
                
            if (iresource instanceof Bundle) {
                Bundle returnedBundle = (Bundle) iresource;
                if (returnedBundle.getEntry().size()>0) {
                    eprConsent = (Consent) returnedBundle.getEntry().get(0).getResource();
                    log.debug("Found Consent = " + eprConsent.getId());
                }
            }
        }


        // Location not found. Add to database



        if (consent.hasPatient()) {
            Resource resource = searchAddResource(consent.getPatient().getReference());
            if (resource == null) referenceMissing(consent, consent.getPatient().getReference());
            consent.setPatient(getReference(resource));
        }
        if (consent.hasOrganization()) {
            List<Reference> organisations = new ArrayList<>();
            for (Reference reference : consent.getOrganization()) {
                Resource resource = searchAddResource(reference.getReference());
                if (resource == null) referenceMissing(consent, reference.getReference());
                organisations.add(getReference(resource));
            }
            consent.setOrganization(organisations);

        }
        if (consent.hasConsentingParty()) {
            List<Reference> parties = new ArrayList<>();
            for (Reference reference : consent.getConsentingParty()) {
                Resource resource = searchAddResource(reference.getReference());
                if (resource == null) referenceMissing(consent, reference.getReference());
                parties.add(getReference(resource));
            }
            consent.setConsentingParty(parties);
        }

        if (consent.hasActor()) {

            for (Consent.ConsentActorComponent consentActorComponent : consent.getActor()) {
                if (consentActorComponent.hasReference()) {
                    Resource resource = searchAddResource(consentActorComponent.getReference().getReference());
                    if (resource == null) referenceMissing(consent, consentActorComponent.getReference().getReference());
                    consentActorComponent.setReference(getReference(resource));
                }
            }
        }

        IBaseResource iResource = null;

        String xhttpMethod = "POST";
        String xhttpPath = "Consent";
        // Location found do not add
        if (eprConsent != null) {
            xhttpMethod="PUT";
            setResourceMap(consentId,eprConsent);
            // Want id value, no path or resource
            xhttpPath = "Consent/"+eprConsent.getIdElement().getIdPart();
            consent.setId(eprConsent.getId());
            iResource = updateResource(consent);

        } else {
            iResource = createResource(consent);
        }

        if (iResource instanceof Consent) {
            eprConsent = (Consent) iResource;
            setResourceMap(consentId,eprConsent);

        } else if (iResource instanceof OperationOutcome)
        {
            processOperationOutcome((OperationOutcome) iResource);
        } else {
            throw new InternalErrorException("Unknown Error");
        }

        return eprConsent;
    }

    public Flag searchAddFlag(String flagId, Flag flag) throws OperationOutcomeException {
        log.debug("Flag searchAdd " +flagId);

        if (flag == null) throw new InternalErrorException("Bundle processing error");

        Flag eprFlag = (Flag) resourceMap.get(flagId);

        // Organization already processed, quit with Organization
        if (eprFlag != null) return eprFlag;

        // Prevent re-adding the same Practitioner
        if (flag.getIdentifier().size() == 0) {
            flag.addIdentifier()
                    .setSystem("urn:uuid")
                    .setValue(flag.getId());
        }



        for (Identifier identifier : flag.getIdentifier()) {
            IBaseResource iresource = queryResource(identifier, "Flag");
                
            if (iresource instanceof Bundle) {
                Bundle returnedBundle = (Bundle) iresource;
                if (returnedBundle.getEntry().size()>0) {
                    eprFlag = (Flag) returnedBundle.getEntry().get(0).getResource();
                    log.debug("Found Flag = " + eprFlag.getId());
                }
            }
        }


        // Location not found. Add to database



        if (flag.hasSubject()) {
            Resource resource = searchAddResource(flag.getSubject().getReference());
            if (resource == null) referenceMissing(flag, flag.getSubject().getReference());
            flag.setSubject(getReference(resource));
        }

        if (flag.hasAuthor()) {
            Resource resource = searchAddResource(flag.getAuthor().getReference());
            if (resource == null) referenceMissing(flag, flag.getAuthor().getReference());
            flag.setAuthor(getReference(resource));
        }

        IBaseResource iResource = null;

        // Location found do not add
        if (eprFlag != null) {

            setResourceMap(flagId,eprFlag);
            // Want id value, no path or resource
            flag.setId(eprFlag.getId());
            iResource = updateResource(flag);
        } else {
            iResource = createResource(flag);
        }


        if (iResource instanceof Flag) {
            eprFlag = (Flag) iResource;
            setResourceMap(flagId,eprFlag);

        } else if (iResource instanceof OperationOutcome)
        {
            processOperationOutcome((OperationOutcome) iResource);
        } else {
            throw new InternalErrorException("Unknown Error");
        }

        return eprFlag;
    }


    public Goal searchAddGoal(String goalId, Goal goal) throws OperationOutcomeException {
        log.debug("Goal searchAdd " +goalId);

        if (goal == null) throw new InternalErrorException("Bundle processing error");

        Goal eprGoal = (Goal) resourceMap.get(goalId);

        // Organization already processed, quit with Organization
        if (eprGoal != null) return eprGoal;

        // Prevent re-adding the same Practitioner
        if (goal.getIdentifier().size() == 0) {
            goal.addIdentifier()
                    .setSystem("urn:uuid")
                    .setValue(goal.getId());
        }


        for (Identifier identifier : goal.getIdentifier()) {
            IBaseResource iresource = queryResource(identifier,"Goal");
               
            if (iresource instanceof Bundle) {
                Bundle returnedBundle = (Bundle) iresource;
                if (returnedBundle.getEntry().size()>0) {
                    eprGoal = (Goal) returnedBundle.getEntry().get(0).getResource();
                    log.debug("Found Goal = " + eprGoal.getId());
                }
            }
        }


        // Location not found. Add to database



        if (goal.hasSubject()) {
            Resource resource = searchAddResource(goal.getSubject().getReference());
            if (resource == null) referenceMissing(goal, goal.getSubject().getReference());
            goal.setSubject(getReference(resource));
        }


        IBaseResource iResource = null;

        String xhttpMethod = "POST";
        String xhttpPath = "Goal";
        // Location found do not add
        if (eprGoal != null) {
            xhttpMethod="PUT";
            setResourceMap(goalId,eprGoal);
            // Want id value, no path or resource
            xhttpPath = "Goal/"+eprGoal.getIdElement().getIdPart();
            goal.setId(eprGoal.getId());
            iResource = updateResource(goal);
        } else {
            iResource = createResource(goal);
        }


        if (iResource instanceof Goal) {
            eprGoal = (Goal) iResource;
            setResourceMap(goalId,eprGoal);

        } else if (iResource instanceof OperationOutcome)
        {
            processOperationOutcome((OperationOutcome) iResource);
        } else {
            throw new InternalErrorException("Unknown Error");
        }

        return eprGoal;
    }


    public MedicationDispense searchAddMedicationDispense(String medicationDispenseId, MedicationDispense medicationDispense) throws OperationOutcomeException {
        log.debug("MedicationDispense searchAdd " +medicationDispenseId);

        if (medicationDispense == null) throw new InternalErrorException("Bundle processing error");

        MedicationDispense eprMedicationDispense = (MedicationDispense) resourceMap.get(medicationDispenseId);

        // Organization already processed, quit with Organization
        if (eprMedicationDispense != null) return eprMedicationDispense;

        // Prevent re-adding the same Practitioner
        if (medicationDispense.getIdentifier().size() == 0) {
            medicationDispense.addIdentifier()
                    .setSystem("urn:uuid")
                    .setValue(medicationDispense.getId());
        }



        for (Identifier identifier : medicationDispense.getIdentifier()) {
            IBaseResource iresource = queryResource(identifier,"MedicationDispense");
           
            if (iresource instanceof Bundle) {
                Bundle returnedBundle = (Bundle) iresource;
                if (returnedBundle.getEntry().size()>0) {
                    eprMedicationDispense = (MedicationDispense) returnedBundle.getEntry().get(0).getResource();
                    log.debug("Found MedicationDispense = " + eprMedicationDispense.getId());
                }
            }
        }


        // Location not found. Add to database



        if (medicationDispense.hasSubject()) {
            Resource resource = searchAddResource(medicationDispense.getSubject().getReference());
            if (resource == null) referenceMissing(medicationDispense, medicationDispense.getSubject().getReference());
            medicationDispense.setSubject(getReference(resource));
        }
        if (medicationDispense.hasContext()) {
            Resource resource = searchAddResource(medicationDispense.getContext().getReference());
            if (resource == null) referenceMissing(medicationDispense, medicationDispense.getContext().getReference());
            medicationDispense.setContext(getReference(resource));
        }

        if (medicationDispense.hasAuthorizingPrescription()) {
            List<Reference> pres = new ArrayList<>();
            for (Reference reference : medicationDispense.getAuthorizingPrescription()) {
                Resource resource = searchAddResource(reference.getReference());
                if (resource == null) referenceMissing(medicationDispense, reference.getReference());
                pres.add(getReference(resource));
            }
            medicationDispense.setAuthorizingPrescription(pres);
        }

        if (medicationDispense.hasPerformer()) {
            if (medicationDispense.getPerformerFirstRep().hasActor()) {
                Resource resource = searchAddResource(medicationDispense.getPerformerFirstRep().getActor().getReference());
                if (resource == null) referenceMissing(medicationDispense, medicationDispense.getPerformerFirstRep().getActor().getReference());
                medicationDispense.getPerformerFirstRep().setActor(getReference(resource));
            }
            if (medicationDispense.getPerformerFirstRep().hasOnBehalfOf()) {
                Resource resource = searchAddResource(medicationDispense.getPerformerFirstRep().getOnBehalfOf().getReference());
                if (resource == null) referenceMissing(medicationDispense, medicationDispense.getPerformerFirstRep().getOnBehalfOf().getReference());
                medicationDispense.getPerformerFirstRep().setOnBehalfOf(getReference(resource));
            }
        }

        if (medicationDispense.hasReceiver()) {
            List<Reference> recv = new ArrayList<>();
            for (Reference reference : medicationDispense.getReceiver()) {
                Resource resource = searchAddResource(reference.getReference());
                if (resource == null) referenceMissing(medicationDispense, reference.getReference());
                recv.add(getReference(resource));
            }
            medicationDispense.setReceiver(recv);
        }

        if (medicationDispense.hasMedicationReference()) {
            Resource resource = null;
            String reference = "";
           try {
               reference = medicationDispense.getMedicationReference().getReference();
               resource = searchAddResource(reference);
               if (resource == null) referenceMissing(medicationDispense, medicationDispense.getMedicationReference().getReference());
               medicationDispense.setMedication(getReference(resource));
           } catch (Exception ex) {}

        }

        IBaseResource iResource = null;

        String xhttpMethod = "POST";
        String xhttpPath = "MedicationDispense";
        // Location found do not add
        if (eprMedicationDispense != null) {
            xhttpMethod="PUT";
            setResourceMap(medicationDispenseId,eprMedicationDispense);
            // Want id value, no path or resource
            xhttpPath = "MedicationDispense/"+eprMedicationDispense.getIdElement().getIdPart();
            medicationDispense.setId(eprMedicationDispense.getId());
            iResource = updateResource(medicationDispense);
        } else {
            iResource = createResource(medicationDispense);
        }



        if (iResource instanceof MedicationDispense) {
            eprMedicationDispense = (MedicationDispense) iResource;
            setResourceMap(medicationDispenseId,eprMedicationDispense);

        } else if (iResource instanceof OperationOutcome)
        {
            processOperationOutcome((OperationOutcome) iResource);
        } else {
            throw new InternalErrorException("Unknown Error");
        }

        return eprMedicationDispense;
    }

    public MedicationAdministration searchAddMedicationAdministration(String medicationAdministrationId, MedicationAdministration medicationAdministration) throws OperationOutcomeException {
        log.debug("MedicationAdministration searchAdd " +medicationAdministrationId);

        if (medicationAdministration == null) throw new InternalErrorException("Bundle processing error");

        MedicationAdministration eprMedicationAdministration = (MedicationAdministration) resourceMap.get(medicationAdministrationId);

        // Organization already processed, quit with Organization
        if (eprMedicationAdministration != null) return eprMedicationAdministration;

        // Prevent re-adding the same Practitioner
        if (medicationAdministration.getIdentifier().size() == 0) {
            medicationAdministration.addIdentifier()
                    .setSystem("urn:uuid")
                    .setValue(medicationAdministration.getId());
        }



        for (Identifier identifier : medicationAdministration.getIdentifier()) {
            IBaseResource iresource = queryResource(identifier,"MedicationAdministration");
            
            if (iresource instanceof Bundle) {
                Bundle returnedBundle = (Bundle) iresource;
                if (returnedBundle.getEntry().size()>0) {
                    eprMedicationAdministration = (MedicationAdministration) returnedBundle.getEntry().get(0).getResource();
                    log.debug("Found MedicationAdministration = " + eprMedicationAdministration.getId());
                }
            }
        }


        // Location not found. Add to database



        if (medicationAdministration.hasSubject()) {
            Resource resource = searchAddResource(medicationAdministration.getSubject().getReference());
            if (resource == null) referenceMissing(medicationAdministration, medicationAdministration.getSubject().getReference());
            medicationAdministration.setSubject(getReference(resource));
        }
        if (medicationAdministration.hasContext()) {
            Resource resource = searchAddResource(medicationAdministration.getContext().getReference());
            if (resource == null) referenceMissing(medicationAdministration, medicationAdministration.getContext().getReference());
            medicationAdministration.setContext(getReference(resource));
        }

        if (medicationAdministration.hasPrescription()) {

                Resource resource = searchAddResource(medicationAdministration.getPrescription().getReference());
                if (resource == null) referenceMissing(medicationAdministration, medicationAdministration.getPrescription().getReference());
                medicationAdministration.setPrescription(getReference(resource));
        }

        if (medicationAdministration.hasPerformer()) {
            if (medicationAdministration.getPerformerFirstRep().hasActor()) {
                Resource resource = searchAddResource(medicationAdministration.getPerformerFirstRep().getActor().getReference());
                if (resource == null) referenceMissing(medicationAdministration, medicationAdministration.getPerformerFirstRep().getActor().getReference());
                medicationAdministration.getPerformerFirstRep().setActor(getReference(resource));
            }
            if (medicationAdministration.getPerformerFirstRep().hasOnBehalfOf()) {
                Resource resource = searchAddResource(medicationAdministration.getPerformerFirstRep().getOnBehalfOf().getReference());
                if (resource == null) referenceMissing(medicationAdministration, medicationAdministration.getPerformerFirstRep().getOnBehalfOf().getReference());
                medicationAdministration.getPerformerFirstRep().setOnBehalfOf(getReference(resource));
            }
        }


        if (medicationAdministration.hasMedicationReference()) {
            Resource resource = null;
            String reference = "";
            try {
                reference = medicationAdministration.getMedicationReference().getReference();
                resource = searchAddResource(reference);
                if (resource == null) referenceMissing(medicationAdministration, medicationAdministration.getMedicationReference().getReference());
                medicationAdministration.setMedication(getReference(resource));
            } catch (Exception ex) {}

        }

        IBaseResource iResource = null;


        // Location found do not add
        if (eprMedicationAdministration != null) {

            setResourceMap(medicationAdministrationId,eprMedicationAdministration);
            // Want id value, no path or resource
            medicationAdministration.setId(eprMedicationAdministration.getId());
            iResource = updateResource(medicationAdministration);
        } else {
            iResource = createResource(medicationAdministration);
        }

        if (iResource instanceof MedicationAdministration) {
            eprMedicationAdministration = (MedicationAdministration) iResource;
            setResourceMap(medicationAdministrationId,eprMedicationAdministration);

        } else if (iResource instanceof OperationOutcome)
        {
            processOperationOutcome((OperationOutcome) iResource);
        } else {
            throw new InternalErrorException("Unknown Error");
        }

        return eprMedicationAdministration;
    }


    public MedicationStatement searchAddMedicationStatement(String medicationStatementId, MedicationStatement medicationStatement) throws OperationOutcomeException {
        log.debug("MedicationStatement searchAdd " +medicationStatementId);

        if (medicationStatement == null) throw new InternalErrorException("Bundle processing error");

        MedicationStatement eprMedicationStatement = (MedicationStatement) resourceMap.get(medicationStatementId);

        // Organization already processed, quit with Organization
        if (eprMedicationStatement != null) return eprMedicationStatement;

        // Prevent re-adding the same Practitioner
        if (medicationStatement.getIdentifier().size() == 0) {
            medicationStatement.addIdentifier()
                    .setSystem("urn:uuid")
                    .setValue(medicationStatement.getId());
        }



        for (Identifier identifier : medicationStatement.getIdentifier()) {
            IBaseResource iresource = queryResource(identifier, "MedicationStatement");
               
            if (iresource instanceof Bundle) {
                Bundle returnedBundle = (Bundle) iresource;
                if (returnedBundle.getEntry().size()>0) {
                    eprMedicationStatement = (MedicationStatement) returnedBundle.getEntry().get(0).getResource();
                    log.debug("Found MedicationStatement = " + eprMedicationStatement.getId());
                }
            }
        }


        // Location not found. Add to database

        if (medicationStatement.hasSubject()) {
            Resource resource = searchAddResource(medicationStatement.getSubject().getReference());
            if (resource == null) referenceMissing(medicationStatement, medicationStatement.getSubject().getReference());
            medicationStatement.setSubject(getReference(resource));
        }
        if (medicationStatement.hasContext()) {
            Resource resource = searchAddResource(medicationStatement.getContext().getReference());
            if (resource == null) referenceMissing(medicationStatement, medicationStatement.getContext().getReference());
            medicationStatement.setContext(getReference(resource));
        }


        if (medicationStatement.hasMedicationReference()) {
            Resource resource = null;
            String reference = "";
            try {
                reference = medicationStatement.getMedicationReference().getReference();
                resource = searchAddResource(reference);
                if (resource == null) referenceMissing(medicationStatement, medicationStatement.getMedicationReference().getReference());
                medicationStatement.setMedication(getReference(resource));
            } catch (Exception ex) {}
        }
        List<Reference> based = new ArrayList<>();
        if (medicationStatement.hasBasedOn()) {
            for (Reference ref : medicationStatement.getBasedOn()) {

                    Resource resource = searchAddResource(ref.getReference());
                    if (resource == null) referenceMissing(medicationStatement, ref.getReference());
                    based.add(getReference(resource));
            }
        }
        medicationStatement.setBasedOn(based);

        List<Reference> derived = new ArrayList<>();
        if (medicationStatement.hasDerivedFrom()) {
            for (Reference ref : medicationStatement.getDerivedFrom()) {

                Resource resource = searchAddResource(ref.getReference());
                if (resource == null) referenceMissing(medicationStatement, ref.getReference());
                derived.add(getReference(resource));
            }
        }
        medicationStatement.setDerivedFrom(derived);

        List<Reference> reason = new ArrayList<>();
        if (medicationStatement.hasReasonReference()) {
            for (Reference ref : medicationStatement.getReasonReference()) {

                Resource resource = searchAddResource(ref.getReference());
                if (resource == null) referenceMissing(medicationStatement, ref.getReference());
                reason.add(getReference(resource));
            }
        }
        medicationStatement.setReasonReference(reason);

        List<Reference> parts = new ArrayList<>();
        if (medicationStatement.hasPartOf()) {
            for (Reference ref : medicationStatement.getPartOf()) {

                Resource resource = searchAddResource(ref.getReference());
                if (resource == null) referenceMissing(medicationStatement, ref.getReference());
                parts.add(getReference(resource));
            }
        }
        medicationStatement.setPartOf(parts);

        IBaseResource iResource = null;

        String xhttpMethod = "POST";
        String xhttpPath = "MedicationStatement";
        // Location found do not add
        if (eprMedicationStatement != null) {
            xhttpMethod="PUT";

            setResourceMap(medicationStatementId,eprMedicationStatement);
            // Want id value, no path or resource
            xhttpPath = "MedicationStatement/"+eprMedicationStatement.getIdElement().getIdPart();
            medicationStatement.setId(eprMedicationStatement.getId());
            iResource = updateResource(medicationStatement);
        } else {
            iResource = createResource(medicationStatement);
        }

        if (iResource instanceof MedicationStatement) {
            eprMedicationStatement = (MedicationStatement) iResource;
            setResourceMap(medicationStatementId,eprMedicationStatement);

        } else if (iResource instanceof OperationOutcome)
        {
            processOperationOutcome((OperationOutcome) iResource);
        } else {
            throw new InternalErrorException("Unknown Error");
        }

        return eprMedicationStatement;
    }



    public Condition searchAddCondition(String conditionId, Condition condition) throws OperationOutcomeException {
        log.debug("Condition searchAdd " +conditionId);

        if (condition == null) throw new InternalErrorException("Bundle processing error");

        Condition eprCondition = (Condition) resourceMap.get(conditionId);

        // Condition already processed, quit with Organization
        if (eprCondition != null) return eprCondition;

        // Prevent re-adding the same Practitioner
        if (condition.getIdentifier().size() == 0) {
            condition.addIdentifier()
                    .setSystem("urn:uuid")
                    .setValue(condition.getId());
        }


        for (Identifier identifier : condition.getIdentifier()) {
            IBaseResource iresource = queryResource(identifier,"Condition");
         
            if (iresource instanceof Bundle) {
                Bundle returnedBundle = (Bundle) iresource;
                if (returnedBundle.getEntry().size()>0) {
                    eprCondition = (Condition) returnedBundle.getEntry().get(0).getResource();
                    log.debug("Found Condition = " + eprCondition.getId());
                }
            }
        }


        if (checkNotInternalReference(condition.getAsserter())) {
            Resource resource = searchAddResource(condition.getAsserter().getReference());
            if (resource == null) referenceMissing(condition, condition.getAsserter().getReference());
            log.debug("Found Resource = " + resource.getId());
            condition.setAsserter(getReference(resource));
        }
        if (checkNotInternalReference(condition.getSubject())) {
            Resource resource = searchAddResource(condition.getSubject().getReference());
            if (resource == null) referenceMissing(condition, condition.getSubject().getReference());
            condition.setSubject(getReference(resource));
        }
        if (checkNotInternalReference(condition.getContext())) {
            Resource resource = searchAddResource(condition.getContext().getReference());
            if (resource == null) referenceMissing(condition, condition.getContext().getReference());
            condition.setContext(getReference(resource));
        }


        IBaseResource iResource = null;
        String xhttpMethod = "POST";
        String xhttpPath = "Condition";
        // Location found do not add
        if (eprCondition != null) {

            xhttpMethod="PUT";
            setResourceMap(conditionId,eprCondition);
            // Want id value, no path or resource
            xhttpPath = "Condition/"+eprCondition.getIdElement().getIdPart();
            condition.setId(eprCondition.getId());
            iResource = updateResource(condition);
        } else {
            iResource = createResource(condition);
        }

        if (iResource instanceof Condition) {
            eprCondition = (Condition) iResource;
            setResourceMap(conditionId,eprCondition);
        } else if (iResource instanceof OperationOutcome)
        {
            processOperationOutcome((OperationOutcome) iResource);
        } else {
            throw new InternalErrorException("Unknown Error");
        }

        return eprCondition;
    }

    /*
    public Composition searchAddComposition(String compositionId, Composition composition) {
        log.debug("Composition searchAdd " +compositionId);

        if (composition == null) throw new InternalErrorException("Bundle processing error");

        Composition eprComposition = (Composition) resourceMap.get(compositionId);

        // Organization already processed, quit with Organization
        if (eprComposition != null) return eprComposition;

        // Prevent re-adding the same Practitioner
        if (composition.getIdentifier() == null) {
            composition.getIdentifier()
                    .setSystem("urn:uuid")
                    .setValue(composition.getId());
        } else {
            if (composition.getIdentifier().getSystem() == null) {
                composition.getIdentifier()
                        .setSystem("urn:uuid");
            }
        }

        ProducerTemplate template = context.createProducerTemplate();

        InputStream inputStream = null;

        String identifierUrl = "identifier=" + composition.getIdentifier().getSystem() + "|" + composition.getIdentifier().getValue();
        Exchange exchange = template.send("direct:EPRServer", ExchangePattern.InOut, new Processor() {
            public void process(Exchange exchange) throws Exception {
                exchange.getIn().setHeader(Exchange.HTTP_QUERY, identifierUrl);
                exchange.getIn().setHeader(Exchange.HTTP_METHOD, "GET");
                exchange.getIn().setHeader(Exchange.HTTP_PATH, "Composition");
            }
        });
        inputStream = (InputStream) exchange.getIn().getBody();
        Reader reader = new InputStreamReader(inputStream);
        IBaseResource iresource = null;
        try {
            iresource = ctx.newJsonParser().parseResource(reader);
        } catch(Exception ex) {
            log.error("JSON Parse failed " + ex.getMessage());
            throw new InternalErrorException(ex.getMessage());
        }
        if (iresource instanceof OperationOutcome) {
            processOperationOutcome((OperationOutcome) iresource);
        } else
        if (iresource instanceof Bundle) {
            Bundle returnedBundle = (Bundle) iresource;
            if (returnedBundle.getEntry().size()>0) {
                eprComposition = (Composition) returnedBundle.getEntry().get(0).getResource();
                log.debug("Found Composition = " + eprComposition.getId());
            }
        }

        // Location not found. Add to database
        List<Reference> authors = new ArrayList<>();
        for (Reference reference : composition.getAuthor()) {
            Resource resource = searchAddResource(reference.getReference());
            if (resource != null) {
                log.debug("Found Resource = " + resource.getId());
                authors.add(getReference(resource));
            }

        }
        composition.setAuthor(authors);

        if (composition.getSubject() != null) {
            Resource resource = searchAddResource(composition.getSubject().getReference());
            if (resource != null) {
                log.debug("Patient resource = "+resource.getId());
            }
            composition.setSubject(getReference(resource));
        }
        if (composition.getEncounter().getReference() != null) {
            Resource resource = searchAddResource(composition.getEncounter().getReference());
            if (resource == null) referenceMissing(composition, composition.getEncounter().getReference());
            composition.setEncounter(getReference(resource));
        }
        if (composition.hasAttester()) {
            for (Composition.CompositionAttesterComponent attester : composition.getAttester()){
                Resource resource = searchAddResource(attester.getParty().getReference());
                if (resource == null) referenceMissing(composition, attester.getParty().getReference());
                attester.setParty(getReference(resource));
            }
        }
        if (composition.hasCustodian()) {
            Resource resource = searchAddResource(composition.getCustodian().getReference());
            if (resource == null) referenceMissing(composition, composition.getCustodian().getReference());
            composition.setCustodian(getReference(resource));
        }
        for (Composition.SectionComponent section: composition.getSection()) {
            List<Reference> references = new ArrayList<>();
            for (Reference reference : section.getEntry()) {
                Resource resource = searchAddResource(reference.getReference());

                if (resource!=null) references.add(getReference(resource));
            }
            section.setEntry(references);
        }

        IBaseResource iResource = null;
        String xhttpMethod = "POST";
        String xhttpPath = "Composition";
        // Location found do not add
        if (eprComposition != null) {
            xhttpMethod="PUT";
            setResourceMap(compositionId,eprComposition);
            // Want id value, no path or resource
            xhttpPath = "Composition/"+eprComposition.getIdElement().getIdPart();
            composition.setId(eprComposition.getId());
        }
        String httpBody = ctx.newJsonParser().encodeResourceToString(composition);
        String httpMethod= xhttpMethod;
        String httpPath = xhttpPath;
        try {
            exchange = template.send("direct:EPRServer", ExchangePattern.InOut, new Processor() {
                public void process(Exchange exchange) throws Exception {
                    exchange.getIn().setHeader(Exchange.HTTP_QUERY, "");
                    exchange.getIn().setHeader(Exchange.HTTP_METHOD, httpMethod);
                    exchange.getIn().setHeader(Exchange.HTTP_PATH, httpPath);
                    exchange.getIn().setHeader("Prefer","return=representation");
                    exchange.getIn().setHeader(Exchange.CONTENT_TYPE, "application/fhir+json");
                    exchange.getIn().setBody(httpBody);
                }
            });
            inputStream = (InputStream) exchange.getIn().getBody();

            reader = new InputStreamReader(inputStream);
            iResource = ctx.newJsonParser().parseResource(reader);
        } catch(Exception ex) {
            log.error("JSON Parse failed " + ex.getMessage());
            throw new InternalErrorException(ex.getMessage());
        }
        if (iResource instanceof Composition) {
            eprComposition = (Composition) iResource;

            setResourceMap(eprComposition.getId(),eprComposition);

        } else if (iResource instanceof OperationOutcome)
        {
            processOperationOutcome((OperationOutcome) iResource);
        } else {
            throw new InternalErrorException("Unknown Error");
        }

        return eprComposition;
    }
    */

    public Binary searchAddBinary(String binaryId, Binary binary) throws OperationOutcomeException {

        ProducerTemplate template = context.createProducerTemplate();
        String jsonResource = ctx.newXmlParser().encodeResourceToString(binary);
        try {
            Exchange edmsExchange = template.send("direct:EDMSServer", ExchangePattern.InOut, new Processor() {
                public void process(Exchange exchange) throws Exception {
                    exchange.getIn().setHeader(Exchange.HTTP_QUERY, "");
                    exchange.getIn().setHeader(Exchange.HTTP_METHOD, "POST");
                    exchange.getIn().setHeader(Exchange.HTTP_PATH, "Binary");
                    exchange.getIn().setHeader(Exchange.CONTENT_TYPE, "application/fhir+xml");
                    exchange.getIn().setBody(jsonResource);
                }
            });

            if (edmsExchange.getIn().getHeader(Exchange.HTTP_RESPONSE_CODE) != null && (edmsExchange.getIn().getHeader(Exchange.HTTP_RESPONSE_CODE).toString().equals("201"))) {
                // Now update the document links
                String[] path = edmsExchange.getIn().getHeader("Location").toString().split("/");
                String resourceId = path[path.length - 1];
                log.info("Binary resource Id = " + resourceId);

                binary.setId(resourceId);
            }
        } catch(Exception ex) {
            log.error("JSON Parse failed " + ex.getMessage());
            throw new InternalErrorException(ex.getMessage());
        }
        return binary;
    }

    public DocumentReference searchAddDocumentReference(String documentReferenceId, DocumentReference documentReference) throws OperationOutcomeException {
        log.debug("DocumentReference searchAdd " +documentReferenceId);

        if (documentReference == null) throw new InternalErrorException("Bundle processing error");

        DocumentReference eprDocumentReference = (DocumentReference) resourceMap.get(documentReferenceId);

        // Organization already processed, quit with Organization
        if (eprDocumentReference != null) return eprDocumentReference;

        // Prevent re-adding the same Document

        if (documentReference.getIdentifier().size() == 0) {
            documentReference.addIdentifier()
                    .setSystem("urn:uuid")
                    .setValue(documentReference.getId());
        }


        for (Identifier identifier : documentReference.getIdentifier()) {

            IBaseResource iresource = queryResource(identifier,"DocumentReference");
            
            if (iresource instanceof Bundle) {
                Bundle returnedBundle = (Bundle) iresource;
                if (returnedBundle.getEntry().size()>0) {
                    eprDocumentReference = (DocumentReference) returnedBundle.getEntry().get(0).getResource();
                    log.debug("Found DocumentReference = " + eprDocumentReference.getId());
                }
            }
        }

        // Location not found. Add to database
        List<Reference> authors = new ArrayList<>();
        for (Reference reference : documentReference.getAuthor()) {
            Resource resource = searchAddResource(reference.getReference());
            if (resource != null) {
                log.debug("Found Resource = " + resource.getId());
                authors.add(getReference(resource));
            }

        }
        documentReference.setAuthor(authors);

        if (documentReference.getSubject() != null) {
            Resource resource = searchAddResource(documentReference.getSubject().getReference());
            if (resource != null) {
                log.debug("Patient resource = "+resource.getId());
            }
            documentReference.setSubject(getReference(resource));
        }

        if (documentReference.getCustodian() != null) {
            Resource resource = searchAddResource(documentReference.getCustodian().getReference());
            if (resource != null) {
                log.debug("Organization resource = "+resource.getId());
                documentReference.setCustodian(getReference(resource));
            }

        }

        if (documentReference.hasContent()) {
            for (DocumentReference.DocumentReferenceContentComponent contentComponent : documentReference.getContent()) {
                if (contentComponent.hasAttachment()) {
                    if (contentComponent.getAttachment().getUrl().contains("urn:uuid")) {
                        Resource resource = searchAddResource(contentComponent.getAttachment().getUrl());
                        if (resource == null) {
                            referenceMissing(documentReference, contentComponent.getAttachment().getUrl());
                        }
                        contentComponent.getAttachment().setUrl(edmsBase + "/Binary/" + resource.getId());
                    }

                }
            }
        }

        IBaseResource iResource = null;
        String xhttpMethod = "POST";
        String xhttpPath = "DocumentReference";
        // Location found do not add
        if (eprDocumentReference != null) {
            xhttpMethod="PUT";
            setResourceMap(documentReferenceId,eprDocumentReference);
            // Want id value, no path or resource
            xhttpPath = "DocumentReference/"+eprDocumentReference.getIdElement().getIdPart();
            documentReference.setId(eprDocumentReference.getId());
            iResource = updateResource(documentReference);
        } else {
            iResource = createResource(documentReference);
        }

        if (iResource instanceof DocumentReference) {
            eprDocumentReference = (DocumentReference) iResource;

            setResourceMap(eprDocumentReference.getId(),eprDocumentReference);

        } else if (iResource instanceof OperationOutcome)
        {
            processOperationOutcome((OperationOutcome) iResource);
        } else {
            throw new InternalErrorException("Unknown Error");
        }

        return eprDocumentReference;
    }


    public Procedure searchAddProcedure(String procedureId, Procedure procedure) throws OperationOutcomeException {
        log.debug("Procedure searchAdd " +procedureId);

        if (procedure == null) throw new InternalErrorException("Bundle processing error");

        Procedure eprProcedure = (Procedure) resourceMap.get(procedureId);

        // Organization already processed, quit with Organization
        if (eprProcedure != null) return eprProcedure;

        // Prevent re-adding the same Practitioner
        if (procedure.getIdentifier().size() == 0) {
            procedure.addIdentifier()
                    .setSystem("urn:uuid")
                    .setValue(procedure.getId());
        }


        for (Identifier identifier : procedure.getIdentifier()) {
            IBaseResource iresource = queryResource(identifier,"Procedure");
           
            if (iresource instanceof Bundle) {
                Bundle returnedBundle = (Bundle) iresource;
                if (returnedBundle.getEntry().size()>0) {
                    eprProcedure = (Procedure) returnedBundle.getEntry().get(0).getResource();
                    log.debug("Found Procedure = " + eprProcedure.getId());
                }
            }
        }

        // Location not found. Add to database

        for (Procedure.ProcedurePerformerComponent performer : procedure.getPerformer()) {
            Reference reference = performer.getActor();
            Resource resource = searchAddResource(reference.getReference());
            if (resource == null) referenceMissing(procedure, reference.getReference());
            log.debug("Found Resource = " + resource.getId());
            performer.setActor(getReference(resource));

        }
        if (procedure.getSubject() != null) {
            Resource resource = searchAddResource(procedure.getSubject().getReference());
            if (resource == null) referenceMissing(procedure, procedure.getSubject().getReference());
            procedure.setSubject(getReference(resource));
        }
        if (procedure.getLocation().getReference() != null) {
            Resource resource = searchAddResource(procedure.getLocation().getReference());
            if (resource == null) referenceMissing(procedure, procedure.getLocation().getReference());
            procedure.setLocation(getReference(resource));
        }
        if (procedure.hasContext()) {
            Resource resource = searchAddResource(procedure.getContext().getReference());
            if (resource != null) { procedure.setContext(getReference(resource)); }
            else { procedure.setContext(null); }
        }
        List<Reference> reasons = new ArrayList<>();
        for (Reference reference : procedure.getReasonReference()) {
            Resource resource = searchAddResource(reference.getReference());
            if (resource != null) reasons.add(getReference(resource));
        }
        procedure.setReasonReference(reasons);

        IBaseResource iResource = null;

        // Location found do not add
        if (eprProcedure != null) {

            procedure.setId(eprProcedure.getId());
            iResource = updateResource(procedure);
        } else {
            iResource = createResource(procedure);
        }

        if (iResource instanceof Procedure) {
            eprProcedure = (Procedure) iResource;
            setResourceMap(eprProcedure.getId(),eprProcedure);


        } else if (iResource instanceof OperationOutcome)
        {
            processOperationOutcome((OperationOutcome) iResource);
        } else {
            throw new InternalErrorException("Unknown Error");
        }

        return eprProcedure;
    }

    public ReferralRequest searchAddReferralRequest(String referralRequestId, ReferralRequest referralRequest) throws OperationOutcomeException {
        log.debug("ReferralRequest searchAdd " +referralRequestId);

        if (referralRequest == null) throw new InternalErrorException("Bundle processing error");

        ReferralRequest eprReferralRequest = (ReferralRequest) resourceMap.get(referralRequestId);

        // Organization already processed, quit with Organization
        if (eprReferralRequest != null) return eprReferralRequest;

        // Prevent re-adding the same Practitioner
        if (referralRequest.getIdentifier().size() == 0) {
            referralRequest.addIdentifier()
                    .setSystem("urn:uuid")
                    .setValue(referralRequest.getId());
        }


        for (Identifier identifier : referralRequest.getIdentifier()) {
            IBaseResource iresource = queryResource(identifier, "ReferralRequest");
            
            if (iresource instanceof Bundle) {
                Bundle returnedBundle = (Bundle) iresource;
                if (returnedBundle.getEntry().size()>0) {
                    eprReferralRequest = (ReferralRequest) returnedBundle.getEntry().get(0).getResource();
                    log.debug("Found ReferralRequest = " + eprReferralRequest.getId());
                }
            }
        }

        // Location not found. Add to database


        if (referralRequest.hasSubject()) {
            Resource resource = searchAddResource(referralRequest.getSubject().getReference());
            if (resource == null) referenceMissing(referralRequest, referralRequest.getSubject().getReference());
            referralRequest.setSubject(getReference(resource));
        }

        if (referralRequest.hasContext() && referralRequest.getContext().getReference() != null) {
            Resource resource = searchAddResource(referralRequest.getContext().getReference());
            if (resource == null) referenceMissing(referralRequest, referralRequest.getContext().getReference());
            referralRequest.setContext(getReference(resource));
        }

        if (referralRequest.getRequester().hasAgent()) {
            Resource resource = searchAddResource(referralRequest.getRequester().getAgent().getReference());
            if (resource == null) referenceMissing(referralRequest, referralRequest.getRequester().getAgent().getReference());
            referralRequest.getRequester().setAgent(getReference(resource));
        }

        if (referralRequest.getRequester().hasOnBehalfOf()) {
            Resource resource = searchAddResource(referralRequest.getRequester().getOnBehalfOf().getReference());
            if (resource == null) referenceMissing(referralRequest, referralRequest.getRequester().getOnBehalfOf().getReference());
            referralRequest.getRequester().setOnBehalfOf(getReference(resource));
        }

        List<Reference> recipients = new ArrayList<>();
        for (Reference reference : referralRequest.getRecipient()) {
            Resource resource = searchAddResource(reference.getReference());
            if (resource == null) referenceMissing(referralRequest, reference.getReference());
            if (resource != null) recipients.add(getReference(resource));
        }
        referralRequest.setRecipient(recipients);

        List<Reference> supportingInfo = new ArrayList<>();
        for (Reference reference : referralRequest.getSupportingInfo()) {
            Resource resource = searchAddResource(reference.getReference());
            if (resource == null) referenceMissing(referralRequest, reference.getReference());
            if (resource != null) recipients.add(getReference(resource));
        }
        referralRequest.setSupportingInfo(supportingInfo);


        List<Reference> reasons = new ArrayList<>();
        for (Reference reference : referralRequest.getReasonReference()) {
            if (reference.hasReference()) {
                Resource resource = searchAddResource(reference.getReference());
                if (resource == null) referenceMissing(referralRequest, reference.getReference());
                if (resource != null) reasons.add(getReference(resource));
            }
        }
        referralRequest.setReasonReference(reasons);

        IBaseResource iResource = null;

        // Location found do not add
        if (eprReferralRequest != null) {

            setResourceMap(referralRequestId,eprReferralRequest);

            referralRequest.setId(eprReferralRequest.getId());
            iResource = updateResource(referralRequest);
        } else {
            iResource = createResource(referralRequest);
        }

        if (iResource instanceof ReferralRequest) {
            eprReferralRequest = (ReferralRequest) iResource;
            setResourceMap(eprReferralRequest.getId(),eprReferralRequest);


        } else if (iResource instanceof OperationOutcome)
        {
            processOperationOutcome((OperationOutcome) iResource);
        } else {
            throw new InternalErrorException("Unknown Error");
        }

        return eprReferralRequest;
    }

    public Encounter searchAddEncounter(String encounterId, Encounter encounter) throws OperationOutcomeException {
        log.debug("Encounter searchAdd " +encounterId);

        if (encounter == null) throw new InternalErrorException("Bundle processing error");

        // To prevent infinite loop
        checkCircularReference(encounter);

        Encounter eprEncounter = (Encounter) resourceMap.get(encounterId);

        // Organization already processed, quit with Organization
        if (eprEncounter != null) return eprEncounter;

        // Prevent re-adding the same Practitioner
        if (encounter.getIdentifier().size() == 0) {
            encounter.addIdentifier()
                    .setSystem("urn:uuid")
                    .setValue(encounter.getId());
        }


        for (Identifier identifier : encounter.getIdentifier()) {
            IBaseResource iresource = queryResource(identifier,"Encounter");
            
            if (iresource instanceof Bundle) {
                Bundle returnedBundle = (Bundle) iresource;
                if (returnedBundle.getEntry().size()>0) {
                    eprEncounter = (Encounter) returnedBundle.getEntry().get(0).getResource();
                    log.debug("Found Encounter = " + eprEncounter.getId());
                }
            }
        }

        // Location not found. Add to database

        // Create new list add in supported resources and discard unsupported
        List<Encounter.EncounterParticipantComponent> performertemp = new ArrayList<>();
        for (Encounter.EncounterParticipantComponent performer : encounter.getParticipant()) {
            Reference reference = performer.getIndividual();
            Resource resource = searchAddResource(reference.getReference());

            if (resource != null) {
                log.debug("Found Resource = " + resource.getId());
                performer.setIndividual(getReference(resource));
                performertemp.add(performer);
            } else {
                log.debug("Not processed "+reference.getReference());
            }
        }
        encounter.setParticipant(performertemp);

        if (encounter.getSubject() != null) {
            Resource resource = searchAddResource(encounter.getSubject().getReference());
            if (resource == null) referenceMissing(encounter, "patient: "+encounter.getSubject().getReference());
            encounter.setSubject(getReference(resource));
        }

        List<Reference> episodes = new ArrayList<>();
        for (Reference reference : encounter.getEpisodeOfCare()) {
            Resource resource = searchAddResource(reference.getReference());
            if (resource == null) referenceMissing(encounter, "episode: "+encounter.getSubject().getReference());
            episodes.add(getReference(resource));
        }
        encounter.setEpisodeOfCare(episodes);

        for (Encounter.DiagnosisComponent component : encounter.getDiagnosis()) {
            if (component.getCondition().hasReference()) {

                    Resource resource = searchAddResource(component.getCondition().getReference());
                    if (resource == null) referenceMissing(encounter, "diagnosis: "+component.getCondition().getReference());
                    component.setCondition(getReference(resource));
                }
        }

        for (Encounter.EncounterLocationComponent component : encounter.getLocation()) {
            if (component.getLocation().hasReference()) {
                Resource resource = searchAddResource(component.getLocation().getReference());
                if (resource == null) referenceMissing(encounter, "location: "+component.getLocation().getReference());
                component.setLocation(getReference(resource));
            }
        }
        if (encounter.hasHospitalization()) {
            if (encounter.getHospitalization().hasDestination() && encounter.getHospitalization().getDestination().hasReference()) {
                Resource resource = searchAddResource(encounter.getHospitalization().getDestination().getReference());
                if (resource == null) referenceMissing(encounter, "hospitalDestination: "+encounter.getHospitalization().getDestination().getReference());
                encounter.getHospitalization().setDestination(getReference(resource));
            }
        }
        if (encounter.hasPartOf()) {
            Resource resource = searchAddResource(encounter.getPartOf().getReference());
            if (resource == null) {
                // Ideally would be an error but not currently supporting ServiceProvider
                referenceMissingWarn(encounter, "PartOf: "+encounter.getPartOf().getReference());
                encounter.setPartOf(null);
            } else {
                encounter.setPartOf(getReference(resource));
            }
        }
        if (encounter.hasServiceProvider()) {
            Resource resource = searchAddResource(encounter.getServiceProvider().getReference());
            if (resource == null) {
                // Ideally would be an error but not currently supporting ServiceProvider
                referenceMissingWarn(encounter, "serviceProvider: "+encounter.getServiceProvider().getReference());
                encounter.setServiceProvider(null);
            } else {
                encounter.setServiceProvider(getReference(resource));
            }
        }
        if (encounter.hasClass_()) {
            if (encounter.getClass_().getSystem() == null) {
                encounter.getClass_().setSystem("http://hl7.org/fhir/v3/ActCode");

                switch (encounter.getClass_().getCode()) {
                    case "inpatient":
                        encounter.getClass_().setCode("ACUTE");
                        break;
                    case "outpatient":
                        encounter.getClass_().setCode("SS");
                        break;
                    case "ambulatory":
                        encounter.getClass_().setCode("AMB");
                        break;
                    case "emergency":
                        encounter.getClass_().setCode("EMER");
                        break;
                }
            }
        }

        IBaseResource iResource = null;

        // Location found do not add
        if (eprEncounter != null) {

            setResourceMap(encounterId,eprEncounter);
            // Want id value, no path or resource

            encounter.setId(eprEncounter.getId());
            iResource = updateResource(encounter);
        } else {
            iResource = createResource(encounter);
        }

        if (iResource instanceof Encounter) {
            eprEncounter = (Encounter) iResource;
            setResourceMap(eprEncounter.getId(),eprEncounter);
        } else if (iResource instanceof OperationOutcome)
        {
            processOperationOutcome((OperationOutcome) iResource);
        } else {
            throw new InternalErrorException("Unknown Error");
        }

        return eprEncounter;
    }

    public EpisodeOfCare searchAddEpisodeOfCare(String episodeOfCareId, EpisodeOfCare episodeOfCare) throws OperationOutcomeException {
        log.debug("EpisodeOfCare searchAdd " +episodeOfCareId);

        if (episodeOfCare == null) throw new InternalErrorException("Bundle processing error");


        EpisodeOfCare eprEpisodeOfCare = (EpisodeOfCare) resourceMap.get(episodeOfCareId);

        // Organization already processed, quit with Organization
        if (eprEpisodeOfCare != null) return eprEpisodeOfCare;

        // Prevent re-adding the same Practitioner
        if (episodeOfCare.getIdentifier().size() == 0) {
            episodeOfCare.addIdentifier()
                    .setSystem("urn:uuid")
                    .setValue(episodeOfCare.getId());
        }


        for (Identifier identifier : episodeOfCare.getIdentifier()) {
            IBaseResource iresource = queryResource(identifier,"EpisodeOfCare");
              
            if (iresource instanceof Bundle) {
                Bundle returnedBundle = (Bundle) iresource;
                if (returnedBundle.getEntry().size()>0) {
                    eprEpisodeOfCare = (EpisodeOfCare) returnedBundle.getEntry().get(0).getResource();
                    log.debug("Found EpisodeOfCare = " + eprEpisodeOfCare.getId());
                }
            }
        }


        if (episodeOfCare.getPatient() != null) {
            Resource resource = searchAddResource(episodeOfCare.getPatient().getReference());
            if (resource == null) referenceMissing(episodeOfCare, episodeOfCare.getPatient().getReference());
            episodeOfCare.setPatient(getReference(resource));
        }

        for (EpisodeOfCare.DiagnosisComponent component : episodeOfCare.getDiagnosis()) {
            if (component.getCondition().getReference() != null) {

                Resource resource = searchAddResource(component.getCondition().getReference());
                if (resource == null) referenceMissing(episodeOfCare, component.getCondition().getReference());
                component.setCondition(getReference(resource));
            }
        }


        if (episodeOfCare.hasManagingOrganization()) {
            Resource resource = searchAddResource(episodeOfCare.getManagingOrganization().getReference());
            if (resource == null) {
                // Ideally would be an error but not currently supporting ServiceProvider
                referenceMissingWarn(episodeOfCare, episodeOfCare.getManagingOrganization().getReference());
                episodeOfCare.setManagingOrganization(null);
            } else {
                episodeOfCare.setManagingOrganization(getReference(resource));
            }
        }

        if (episodeOfCare.hasCareManager()) {
            Resource resource = searchAddResource(episodeOfCare.getCareManager().getReference());
            if (resource == null) {
                referenceMissingWarn(episodeOfCare, episodeOfCare.getCareManager().getReference());
                episodeOfCare.setCareManager(null);
            } else {
                episodeOfCare.setCareManager(getReference(resource));
            }
        }



        IBaseResource iResource = null;
        // Location found do not add
        if (eprEpisodeOfCare != null) {

            setResourceMap(episodeOfCareId,eprEpisodeOfCare);
            // Want id value, no path or resource
            episodeOfCare.setId(eprEpisodeOfCare.getId());
            iResource = updateResource(episodeOfCare);
        } else {
            iResource = createResource(episodeOfCare);
        }

        if (iResource instanceof EpisodeOfCare) {
            eprEpisodeOfCare = (EpisodeOfCare) iResource;
            setResourceMap(eprEpisodeOfCare.getId(),eprEpisodeOfCare);
        } else if (iResource instanceof OperationOutcome)
        {
            processOperationOutcome((OperationOutcome) iResource);
        } else {
            throw new InternalErrorException("Unknown Error");
        }

        return eprEpisodeOfCare;
    }



    public Patient searchAddPatient(String patientId, Patient patient) throws OperationOutcomeException {

            log.info("Patient searchAdd " + patientId);

            if (patient == null) throw new InternalErrorException("Bundle processing error");

            Patient eprPatient = (Patient) resourceMap.get(patientId);

            // Patient already processed, quit with Patient
            if (eprPatient != null) return eprPatient;


            for (Identifier identifier : patient.getIdentifier()) {
                
                IBaseResource iresource = queryResource( identifier, "Patient");

                if (iresource instanceof Bundle) {
                    Bundle returnedBundle = (Bundle) iresource;
                    if (returnedBundle.getEntry().size() > 0) {
                        eprPatient = (Patient) returnedBundle.getEntry().get(0).getResource();
                        log.debug("Found Patient = " + eprPatient.getId());
                        // KGM 31/Jan/2018 Missing break on finding patient
                        break;
                    }
                }
            }
            // Patient found do not add
            if (eprPatient != null) {
                setResourceMap(patientId, eprPatient);

               // return eprPatient;
            }

            // Location not found. Add to database

            if (patient.getManagingOrganization().getReference() != null) {
                Resource resource = searchAddResource(patient.getManagingOrganization().getReference());

                if (resource == null) referenceMissing(patient, patient.getManagingOrganization().getReference());
                log.debug("Found ManagingOrganization = " + resource.getId());
                patient.setManagingOrganization(getReference(resource));
            }
            for (Reference reference : patient.getGeneralPractitioner()) {
                Resource resource = searchAddResource(reference.getReference());
                if (resource == null) referenceMissing(patient, reference.getReference());
                log.debug("Found Patient Practitioner = " + reference.getId());
                // This resets the first gp only (should only be one gp)
                patient.getGeneralPractitioner().get(0).setReference(getReference(resource).getReference());
            }

            IBaseResource iResource = null;

        if (eprPatient != null) {

            patient.setId(eprPatient.getId());
            iResource = updateResource(patient);
        } else {
            iResource = createResource(patient);
        }


            if (iResource instanceof Patient) {
                eprPatient = (Patient) iResource;
                setResourceMap(patientId, eprPatient);
            } else if (iResource instanceof OperationOutcome) {
                processOperationOutcome((OperationOutcome) iResource);
            } else {
                throw new InternalErrorException("Unknown Error");
            }

        return eprPatient;
    }

    public Questionnaire searchAddQuestionnaire(String formId, Questionnaire form) throws OperationOutcomeException {

        log.debug("Questionnaire searchAdd " + formId);

        if (form == null) throw new InternalErrorException("Bundle processing error");

        Questionnaire eprQuestionnaire = (Questionnaire) resourceMap.get(formId);

        // Questionnaire already processed, quit with Questionnaire
        if (eprQuestionnaire != null) return eprQuestionnaire;


        for (Identifier identifier : form.getIdentifier()) {
            IBaseResource iresource = queryResource( identifier, "Questionnaire");
            
            if (iresource instanceof Bundle) {
                Bundle returnedBundle = (Bundle) iresource;
                if (returnedBundle.getEntry().size() > 0) {
                    eprQuestionnaire = (Questionnaire) returnedBundle.getEntry().get(0).getResource();
                    log.debug("Found Questionnaire = " + eprQuestionnaire.getId());
                    // KGM 31/Jan/2018 Missing break on finding form
                    break;
                }
            }
        }
        // Questionnaire found do not add
        if (eprQuestionnaire != null) {
            setResourceMap(formId, eprQuestionnaire);

           // return eprQuestionnaire;
        }

        IBaseResource iResource = null;



        // Location found do not add
        if (eprQuestionnaire != null) {

            form.setId(eprQuestionnaire.getId());
            iResource = updateResource(form);
        } else {
            iResource = createResource(form);
        }

        if (iResource instanceof Questionnaire) {
            eprQuestionnaire = (Questionnaire) iResource;
            setResourceMap(formId, eprQuestionnaire);
        } else if (iResource instanceof OperationOutcome) {
            processOperationOutcome((OperationOutcome) iResource);
        } else {
            throw new InternalErrorException("Unknown Error");
        }

        return eprQuestionnaire;
    }

    public RelatedPerson searchAddRelatedPerson(String personId, RelatedPerson person) throws OperationOutcomeException {

        log.debug("RelatedPerson searchAdd " + personId);

        if (person == null) throw new InternalErrorException("Bundle processing error");

        RelatedPerson eprRelatedPerson = (RelatedPerson) resourceMap.get(personId);

        // RelatedPerson already processed, quit with RelatedPerson
        if (eprRelatedPerson != null) return eprRelatedPerson;


        for (Identifier identifier : person.getIdentifier()) {
            IBaseResource iresource = queryResource(identifier,"RelatedPerson");
             if (iresource instanceof Bundle) {
                Bundle returnedBundle = (Bundle) iresource;
                if (returnedBundle.getEntry().size() > 0) {
                    eprRelatedPerson = (RelatedPerson) returnedBundle.getEntry().get(0).getResource();
                    log.debug("Found RelatedPerson = " + eprRelatedPerson.getId());
                    // KGM 31/Jan/2018 Missing break on finding person
                    break;
                }
            }
        }
        // RelatedPerson found do not add
        if (eprRelatedPerson != null) {
            setResourceMap(personId, eprRelatedPerson);

            return eprRelatedPerson;
        }
        IBaseResource iResource = null;
        if (person.getPatient() != null) {
            Resource resource = searchAddResource(person.getPatient().getReference());
            if (resource == null) referenceMissing(person, person.getPatient().getReference());
            person.setPatient(getReference(resource));
            person.setId(resource.getId());
            iResource = updateResource(person);
        } else {
            iResource = createResource(person);
        }

        if (iResource instanceof RelatedPerson) {
            eprRelatedPerson = (RelatedPerson) iResource;
            setResourceMap(personId, eprRelatedPerson);
        } else if (iResource instanceof OperationOutcome) {
            processOperationOutcome((OperationOutcome) iResource);
        } else {
            throw new InternalErrorException("Unknown Error");
        }

        return eprRelatedPerson;
    }

    private void referenceMissingWarn(Resource resource, String reference) {
        String errMsg = "Unable to resolve reference: "+reference+" In resource "+resource.getClass().getSimpleName()+" id "+resource.getId();
        log.warn(errMsg);

    }
    private void referenceMissing(Resource resource, String reference) {
        String errMsg = "Unable to resolve reference: "+reference+" In resource "+resource.getClass().getSimpleName()+" id "+resource.getId();
        log.error(errMsg);
        OperationOutcome outcome = new OperationOutcome();
        outcome.addIssue()
                .setCode(OperationOutcome.IssueType.BUSINESSRULE)
                .setSeverity(OperationOutcome.IssueSeverity.FATAL)
                .setDiagnostics(errMsg)
                .setDetails(
                        new CodeableConcept().setText("Invalid Reference")
                );
        setOperationOutcome(outcome);
        OperationOutcomeFactory.convertToException(outcome);
    }

    private void processOperationOutcome(OperationOutcome operationOutcome) throws OperationOutcomeException {
        this.operationOutcome = operationOutcome;

        log.debug("Server Returned Operation Outcome: " + ctx.newJsonParser().encodeResourceToString(operationOutcome));

        throw new OperationOutcomeException(operationOutcome);
    }
    private void setResourceMap(String referenceId, Resource resource) {
        if (resourceMap.get(referenceId) != null) {
            resourceMap.replace(referenceId, resource);
        } else {
            resourceMap.put(referenceId,resource);

        }
        String id = resource.getResourceType().toString() + '/' +resource.getIdElement().getIdPart();

        log.debug("setResourceMap = " +resource.getId());
        if (resourceMap.get(resource.getId()) != null) {
            resourceMap.replace(resource.getId(),resource);
        } else {
            resourceMap.put(resource.getId(),resource);
        }
        if (!id.equals(resource.getId())) {
            if (resourceMap.get(id) != null) {
                //resourceMap.replace(id,resource);
            } else {
                log.debug("setResourceMapElement = " + id);
                resourceMap.put(id,resource);
            }
        }
    }
    private boolean checkNotInternalReference(Reference reference) {

        if (reference.getReference() != null) {
            log.debug("Checking reference "+reference.getReference());

            if (!reference.getReference().matches("\\w+/\\d+")) {
                return true;
            } else {
                return false;
            }
        } else {
            return false;
        }
    }
}
