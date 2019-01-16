package uk.nhs.careconnect.ri.messaging.dao;

import org.hl7.fhir.dstu3.model.*;
import org.hl7.fhir.utilities.xhtml.XhtmlDocument;
import org.hl7.fhir.utilities.xhtml.XhtmlNode;
import org.hl7.fhir.utilities.xhtml.XhtmlParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.thymeleaf.TemplateEngine;
import org.thymeleaf.context.Context;

import java.util.ArrayList;

public class FhirDocUtil {

    public static final String SNOMEDCT = "http://snomed.info/sct";

    Context ctxThymeleaf = new Context();

    private TemplateEngine templateEngine;

    private XhtmlParser xhtmlParser = new XhtmlParser();

    private static final Logger log = LoggerFactory.getLogger(FhirDocUtil.class);

    FhirDocUtil(TemplateEngine templateEngine) {
        this.templateEngine = templateEngine;
    }

    public Composition.SectionComponent getConditionSection(Bundle bundle) {
        Composition.SectionComponent section = new Composition.SectionComponent();

        ArrayList<Condition> conditions = new ArrayList<>();

        section.getCode().addCoding()
                .setSystem("http://snomed.info/sct")
                .setCode("887151000000100")
                .setDisplay("Problems and issues");
        section.setTitle("Problems and issues");

        for (Bundle.BundleEntryComponent entry : bundle.getEntry()) {
            if (entry.getResource() instanceof Condition) {
                Condition condition = (Condition) entry.getResource();
                section.getEntry().add(new Reference("urn:uuid:"+condition.getId()));
                conditions.add(condition);
            }
        }
        ctxThymeleaf.clearVariables();
        ctxThymeleaf.setVariable("conditions", conditions);

        section.getText().setDiv(getDiv("condition")).setStatus(Narrative.NarrativeStatus.GENERATED);

        return section;
    }



    public Composition.SectionComponent getAdvanceTreatmentPreferencesSection(Bundle bundle) {
        Composition.SectionComponent section = new Composition.SectionComponent();


        section.getCode().addCoding()
                .setSystem("http://snomed.info/sct")
                .setCode("854851000000107")
                .setDisplay("Advanced Treatment Preferences");
        section.setTitle("Advanced Treatment Preferences");



        ArrayList<CarePlan> plans = new ArrayList<>();
        for (Bundle.BundleEntryComponent entry : bundle.getEntry()) {
            if (entry.getResource() instanceof CarePlan) {
                CarePlan carePlan = (CarePlan) entry.getResource();
                section.getEntry().add(new Reference("urn:uuid:"+carePlan.getId()));
                plans.add(carePlan);

            }
        }
        ctxThymeleaf.clearVariables();
        ctxThymeleaf.setVariable("careplans", plans);
        section.getText().setDiv(getDiv("careplans")).setStatus(Narrative.NarrativeStatus.GENERATED);

        return section;
    }

    public Composition.SectionComponent getFunctionalStatusSection(Bundle bundle) {
        Composition.SectionComponent section = new Composition.SectionComponent();


        section.getCode().addCoding()
                .setSystem("http://snomed.info/sct")
                .setCode("854851000000107")
                .setDisplay("Functional Status");
        section.setTitle("Functional Status");

        ArrayList<QuestionnaireResponse> questionnaireResponses = new ArrayList<>();
        for (Bundle.BundleEntryComponent entry : bundle.getEntry()) {
            if (entry.getResource() instanceof QuestionnaireResponse) {
                QuestionnaireResponse questionnaireResponse = (QuestionnaireResponse) entry.getResource();
                section.getEntry().add(new Reference("urn:uuid:"+questionnaireResponse.getId()));
                questionnaireResponses.add(questionnaireResponse);
            }
        }
        ctxThymeleaf.clearVariables();
        ctxThymeleaf.setVariable("questionnaireResponse", questionnaireResponses);
        section.getText().setDiv(getDiv("functionalstatus")).setStatus(Narrative.NarrativeStatus.GENERATED);


        return section;
    }

    public Composition.SectionComponent getPreferencesSection(Bundle bundle) {
        Composition.SectionComponent section = new Composition.SectionComponent();


        section.getCode().addCoding()
                .setSystem("http://snomed.info/sct")
                .setCode("854851000000107")
                .setDisplay("Preferences");
        section.setTitle("Preferences");

        ArrayList<QuestionnaireResponse> questionnaireResponses = new ArrayList<>();
        for (Bundle.BundleEntryComponent entry : bundle.getEntry()) {
            if (entry.getResource() instanceof QuestionnaireResponse) {
                QuestionnaireResponse questionnaireResponse = (QuestionnaireResponse) entry.getResource();
                section.getEntry().add(new Reference("urn:uuid:"+questionnaireResponse.getId()));
                questionnaireResponses.add(questionnaireResponse);
            }
        }
        ctxThymeleaf.clearVariables();
        ctxThymeleaf.setVariable("questionnaireResponse", questionnaireResponses);
        section.getText().setDiv(getDiv("preferences")).setStatus(Narrative.NarrativeStatus.GENERATED);


        return section;
    }


    public Composition.SectionComponent getDisabilitySection(Bundle bundle) {
        Composition.SectionComponent section = new Composition.SectionComponent();

        ArrayList<Condition> conditions = new ArrayList<>();

        section.getCode().addCoding()
                .setSystem("http://snomed.info/sct")
                .setCode("854851000000107")
                .setDisplay("Disability");
        section.setTitle("Disability");

        for (Bundle.BundleEntryComponent entry : bundle.getEntry()) {
            if (entry.getResource() instanceof Condition) {
                Condition condition = (Condition) entry.getResource();
                section.getEntry().add(new Reference("urn:uuid:"+condition.getId()));
                conditions.add(condition);
            }
        }
        ctxThymeleaf.clearVariables();
        ctxThymeleaf.setVariable("conditions", conditions);

        section.getText().setDiv(getDiv("condition")).setStatus(Narrative.NarrativeStatus.GENERATED);

        return section;
    }

    public Composition.SectionComponent getPrognosis(Bundle bundle) {
        Composition.SectionComponent section = new Composition.SectionComponent();

        ArrayList<RiskAssessment> risks = new ArrayList<>();
        ArrayList<ClinicalImpression> impressions = new ArrayList<>();

        section.getCode().addCoding()
                .setSystem("http://snomed.info/sct")
                .setCode("854851000000107")
                .setDisplay("Prognosis");
        section.setTitle("Prognosis");

        for (Bundle.BundleEntryComponent entry : bundle.getEntry()) {
            if (entry.getResource() instanceof RiskAssessment) {
                RiskAssessment riskAssessment = (RiskAssessment) entry.getResource();
                section.getEntry().add(new Reference("urn:uuid:"+riskAssessment.getId()));
                risks.add(riskAssessment);
            }
            if (entry.getResource() instanceof ClinicalImpression) {
                ClinicalImpression impression = (ClinicalImpression) entry.getResource();
                section.getEntry().add(new Reference("urn:uuid:"+impression.getId()));
                impressions.add(impression);

            }
        }
        ctxThymeleaf.clearVariables();
        ctxThymeleaf.setVariable("risks", risks);
        ctxThymeleaf.setVariable("impressions", impressions);

        section.getText().setDiv(getDiv("prognosis")).setStatus(Narrative.NarrativeStatus.GENERATED);

        return section;
    }

    public Composition.SectionComponent getConsentSection(Bundle bundle) {
        Composition.SectionComponent section = new Composition.SectionComponent();

        ArrayList<Consent> consents = new ArrayList<>();

        section.getCode().addCoding()
                .setSystem("http://snomed.info/sct")
                .setCode("854851000000107")
                .setDisplay("Consent");
        section.setTitle("Consent");

        for (Bundle.BundleEntryComponent entry : bundle.getEntry()) {
            if (entry.getResource() instanceof Consent) {
                Consent consent = (Consent) entry.getResource();
                section.getEntry().add(new Reference("urn:uuid:"+consent.getId()));
                consents.add(consent);

            }
        }
        ctxThymeleaf.clearVariables();
        ctxThymeleaf.setVariable("consents", consents);

        section.getText().setDiv(getDiv("consents")).setStatus(Narrative.NarrativeStatus.GENERATED);

        return section;
    }

    public Composition.SectionComponent getCareTeamSection(Bundle bundle) {
        Composition.SectionComponent section = new Composition.SectionComponent();

        ArrayList<CareTeam> teams = new ArrayList<>();

        section.getCode().addCoding()
                .setSystem("http://snomed.info/sct")
                .setCode("854851000000107")
                .setDisplay("Contacts");
        section.setTitle("Contacts");

        for (Bundle.BundleEntryComponent entry : bundle.getEntry()) {
            if (entry.getResource() instanceof CareTeam) {
                CareTeam careTeam = (CareTeam) entry.getResource();
                section.getEntry().add(new Reference("urn:uuid:"+careTeam.getId()));
                teams.add(careTeam);
            }
        }
        ctxThymeleaf.clearVariables();
        ctxThymeleaf.setVariable("teams", teams);

        section.getText().setDiv(getDiv("contacts")).setStatus(Narrative.NarrativeStatus.GENERATED);

        return section;
    }

    public Composition.SectionComponent getMedicationStatementSection(Bundle bundle) {
        Composition.SectionComponent section = new Composition.SectionComponent();

        ArrayList<MedicationStatement>  medicationStatements = new ArrayList<>();

        section.getCode().addCoding()
                .setSystem("http://snomed.info/sct")
                .setCode("933361000000108")
                .setDisplay("Medications and medical devices");
        section.setTitle("Medications and medical devices");

        for (Bundle.BundleEntryComponent entry : bundle.getEntry()) {
            if (entry.getResource() instanceof MedicationStatement) {
                MedicationStatement medicationStatement = (MedicationStatement) entry.getResource();
                section.getEntry().add(new Reference("urn:uuid:"+medicationStatement.getId()));
                medicationStatements.add(medicationStatement);

            }
        }
        ctxThymeleaf.clearVariables();
        ctxThymeleaf.setVariable("medicationStatements", medicationStatements);

        section.getText().setDiv(getDiv("medicationStatement")).setStatus(Narrative.NarrativeStatus.GENERATED);

        return section;
    }

    public Composition.SectionComponent getAllergySection(Bundle bundle) {
        Composition.SectionComponent section = new Composition.SectionComponent();

        ArrayList<AllergyIntolerance>  allergyIntolerances = new ArrayList<>();

        section.getCode().addCoding()
                .setSystem("http://snomed.info/sct")
                .setCode("886921000000105")
                .setDisplay("Allergies and adverse reactions");
        section.setTitle("Allergies and adverse reactions");

        for (Bundle.BundleEntryComponent entry : bundle.getEntry()) {
            if (entry.getResource() instanceof AllergyIntolerance) {
                AllergyIntolerance allergyIntolerance = (AllergyIntolerance) entry.getResource();
                section.getEntry().add(new Reference("urn:uuid:"+allergyIntolerance.getId()));
                allergyIntolerances.add(allergyIntolerance);
            }
        }
        ctxThymeleaf.clearVariables();

        ctxThymeleaf.setVariable("allergies", allergyIntolerances);

        section.getText().setDiv(getDiv("allergy")).setStatus(Narrative.NarrativeStatus.GENERATED);

        return section;
    }

    public Composition.SectionComponent getEncounterSection(Bundle bundle) {
        Composition.SectionComponent section = new Composition.SectionComponent();
        // TODO Get Correct code.
        ArrayList<Encounter>  encounters = new ArrayList<>();

        section.getCode().addCoding()
                .setSystem("http://snomed.info/sct")
                .setCode("713511000000103")
                .setDisplay("Encounter administration");
        section.setTitle("Encounters");

        for (Bundle.BundleEntryComponent entry : bundle.getEntry()) {
            if (entry.getResource() instanceof Encounter) {
                Encounter encounter = (Encounter) entry.getResource();
                section.getEntry().add(new Reference("urn:uuid:"+encounter.getId()));
                encounters.add(encounter);
            }
        }
        ctxThymeleaf.clearVariables();
        ctxThymeleaf.setVariable("encounters", encounters);

        section.getText().setDiv(getDiv("encounter")).setStatus(Narrative.NarrativeStatus.GENERATED);

        return section;
    }

    public Composition.SectionComponent getMedicationRequestSection(Bundle bundle) {
        Composition.SectionComponent section = new Composition.SectionComponent();

        ArrayList<MedicationRequest>  medicationRequests = new ArrayList<>();

        section.getCode().addCoding()
                .setSystem(SNOMEDCT)
                .setCode("933361000000108")
                .setDisplay("Medications and medical devices");
        section.setTitle("Medications and medical devices");

        for (Bundle.BundleEntryComponent entry : bundle.getEntry()) {
            if (entry.getResource() instanceof MedicationRequest) {
                MedicationRequest medicationRequest = (MedicationRequest) entry.getResource();
                //medicationStatement.getMedicationReference().getDisplay();
                section.getEntry().add(new Reference("urn:uuid:"+medicationRequest.getId()));
                medicationRequest.getAuthoredOn();
                medicationRequests.add(medicationRequest);

            }
        }
        ctxThymeleaf.clearVariables();
        ctxThymeleaf.setVariable("medicationRequests", medicationRequests);

        section.getText().setDiv(getDiv("medicationRequest")).setStatus(Narrative.NarrativeStatus.GENERATED);

        return section;
    }

    public Composition.SectionComponent getObservationSection(Bundle bundle) {
        Composition.SectionComponent section = new Composition.SectionComponent();

        ArrayList<Observation>  observations = new ArrayList<>();

        section.getCode().addCoding()
                .setSystem(SNOMEDCT)
                .setCode("425044008")
                .setDisplay("Physical exam section");
        section.setTitle("Physical exam section");

        for (Bundle.BundleEntryComponent entry : bundle.getEntry()) {
            if (entry.getResource() instanceof Observation) {
                Observation observation= (Observation) entry.getResource();

                section.getEntry().add(new Reference("urn:uuid:"+observation.getId()));

                observations.add(observation);
            }
        }
        ctxThymeleaf.clearVariables();

        ctxThymeleaf.setVariable("observations", observations);

        section.getText().setDiv(getDiv("observation")).setStatus(Narrative.NarrativeStatus.GENERATED);

        return section;
    }

    public Composition.SectionComponent getProcedureSection(Bundle bundle) {
        Composition.SectionComponent section = new Composition.SectionComponent();

        ArrayList<Procedure>  procedures = new ArrayList<>();

        section.getCode().addCoding()
                .setSystem(SNOMEDCT)
                .setCode("887171000000109")
                .setDisplay("Procedues");
        section.setTitle("Procedures");

        for (Bundle.BundleEntryComponent entry : bundle.getEntry()) {
            if (entry.getResource() instanceof Procedure) {
                Procedure procedure = (Procedure) entry.getResource();

                section.getEntry().add(new Reference("urn:uuid:"+procedure.getId()));
                procedures.add(procedure);
            }
        }
        ctxThymeleaf.clearVariables();

        ctxThymeleaf.setVariable("procedures", procedures);

        section.getText().setDiv(getDiv("procedure")).setStatus(Narrative.NarrativeStatus.GENERATED);

        return section;
    }

    public Patient generatePatientHtml(Patient patient, Bundle fhirDocument) {
        if (!patient.hasText()) {

            ctxThymeleaf.clearVariables();
            ctxThymeleaf.setVariable("patient", patient);
            for (Bundle.BundleEntryComponent entry : fhirDocument.getEntry()) {
                if (entry.getResource() instanceof Practitioner) ctxThymeleaf.setVariable("gp", entry.getResource());
                if (entry.getResource() instanceof Organization) ctxThymeleaf.setVariable("practice", entry.getResource());
                Practitioner practice;

            }

            patient.getText().setDiv(getDiv("patient")).setStatus(Narrative.NarrativeStatus.GENERATED);
            log.debug(patient.getText().getDiv().getValueAsString());
        }
        return patient;
    }

    private XhtmlNode getDiv(String template) {
        XhtmlNode xhtmlNode = null;
        String processedHtml = templateEngine.process(template, ctxThymeleaf);
        try {
            XhtmlDocument parsed = xhtmlParser.parse(processedHtml, null);
            xhtmlNode = parsed.getDocumentElement();
            log.debug(processedHtml);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return xhtmlNode;
    }



}
