package uk.nhs.careconnect.ri.messaging.providers;

import ca.uhn.fhir.context.FhirContext;

import ca.uhn.fhir.validation.FhirValidator;
import ca.uhn.fhir.validation.SingleValidationMessage;
import ca.uhn.fhir.validation.ValidationResult;
import org.hl7.fhir.dstu3.hapi.validation.DefaultProfileValidationSupport;
import org.hl7.fhir.dstu3.hapi.validation.FhirInstanceValidator;
import org.hl7.fhir.dstu3.hapi.validation.ValidationSupportChain;
import org.hl7.fhir.dstu3.model.BaseResource;
import org.hl7.fhir.dstu3.model.Bundle;
import org.hl7.fhir.dstu3.model.OperationOutcome;
import uk.org.hl7.fhir.core.Dstu2.CareConnectSystem;
import uk.org.hl7.fhir.validation.stu3.CareConnectProfileValidationSupport;
import uk.org.hl7.fhir.validation.stu3.SNOMEDUKMockValidationSupport;


public class ValidationFactory {
    private ValidationFactory() {

    }



    public static OperationOutcome validateResource(BaseResource resource) {


        OperationOutcome outcome = new OperationOutcome();

        FhirContext ctxValidator = FhirContext.forDstu3();
        FhirValidator validator  = ctxValidator.newValidator();
        FhirInstanceValidator instanceValidator = new FhirInstanceValidator();

        validator.registerValidatorModule(instanceValidator);
        ValidationSupportChain support = new ValidationSupportChain(
                new DefaultProfileValidationSupport()
                ,new CareConnectProfileValidationSupport(ctxValidator)
                ,new SNOMEDUKMockValidationSupport() // This is to disable SNOMED CT Warnings. Mock validation to return ok for SNOMED Concepts
        );
        instanceValidator.setValidationSupport(support);

        if (resource instanceof Bundle) {
            Bundle bundle = (Bundle) resource;
            for (Bundle.BundleEntryComponent  entry : bundle.getEntry()) {
                outcome = doValidation(validator, outcome, entry.getResource());
            }
        } else {
            outcome = doValidation(validator, outcome, resource);
        }


       // outcome.addIssue().setSeverity(OperationOutcome.IssueSeverity.WARNING).setDiagnostics("DUMMY One minor issue detected");
        return outcome;
    }
    private static OperationOutcome doValidation( FhirValidator validator, OperationOutcome outcome, BaseResource resource) {
        FhirContext context = FhirContext.forDstu3();

        String resourceStr = context.newXmlParser().encodeResourceToString(resource);

        System.out.println(resourceStr);
    try {
        ValidationResult result = validator.validateWithResult(resourceStr);
        for (SingleValidationMessage next : result.getMessages()) {
            // Disabling SNOMED warnings for valuesets. (ValueSets with include queries or references require a terminology service)
            if (next.getMessage().contains("and a code from this value set is required") && next.getMessage().contains(CareConnectSystem.SNOMEDCT)) {
                //	System.out.println("match **");
            } else if (next.getMessage().contains("a code is required from this value set") && next.getMessage().contains(CareConnectSystem.SNOMEDCT)) {
                //	System.out.println("match ** ** ");
            } else if (next.getMessage().contains("and a code is recommended to come from this value set") && next.getMessage().contains(CareConnectSystem.SNOMEDCT)) {
                //	System.out.println("match ** ** **" );
            } else if (next.getMessage().contains("path Patient.name (fhirPath = true and (use memberOf")) {
                //System.out.println("** ** ** Code Issue ValueSet expansion not implemented in instanceValidator" );
            } else if (next.getMessage().contains("Error Multiple filters not handled yet")) {
                //System.out.println("** ** ** multiple filters in ValueSet not implemented" );
            } else {
                OperationOutcome.OperationOutcomeIssueComponent issue = outcome.addIssue();
                issue.setDiagnostics(next.getLocationString() + " - " + next.getMessage()).addLocation(resource.getClass().getSimpleName() + "/" + resource.getIdElement().getIdPart());
                switch (next.getSeverity()) {
                    case ERROR:
                        issue.setSeverity(OperationOutcome.IssueSeverity.ERROR);
                        break;
                    case FATAL:
                        issue.setSeverity(OperationOutcome.IssueSeverity.FATAL);
                        break;
                    case WARNING:
                        issue.setSeverity(OperationOutcome.IssueSeverity.WARNING);
                        break;
                    case INFORMATION:
                        issue.setSeverity(OperationOutcome.IssueSeverity.INFORMATION);
                        break;
                }
                outcome.addIssue(issue);
            }
        }
    } catch (Exception ex) {
        outcome.addIssue().setSeverity(OperationOutcome.IssueSeverity.FATAL).setDiagnostics(ex.getMessage()).addLocation(resource.getId().toString());
    }
        return outcome;
    }
}
