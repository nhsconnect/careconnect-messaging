package uk.nhs.careconnect.ri.messaging.support;

import org.hl7.fhir.convertors.VersionConvertorAdvisor30;
import org.hl7.fhir.convertors.VersionConvertor_10_30;
import org.hl7.fhir.dstu3.model.*;
import org.hl7.fhir.exceptions.FHIRException;
import org.hl7.fhir.instance.model.Bundle;
import org.hl7.fhir.instance.model.Resource;

import java.util.*;

public class CareConnectDSTU2toSTU3 {


    VersionConvertorAdvisor30 advisor = new VersionConvertorAdvisor30() {
        @Override
        public boolean ignoreEntry(org.hl7.fhir.dstu3.model.Bundle.BundleEntryComponent bundleEntryComponent) {
            System.out.println("ignoreentry");
            return false;
        }

        @Override
        public Resource convert(org.hl7.fhir.dstu3.model.Resource resource) throws FHIRException {
            System.out.println(resource.getId());
            return null;
        }

        @Override
        public void handleCodeSystem(CodeSystem codeSystem, ValueSet valueSet) throws FHIRException {
            System.out.println("convertio");
        }

        @Override
        public CodeSystem getCodeSystem(ValueSet valueSet) throws FHIRException {
            System.out.println("getCodeSystem");
            return null;
        }
    };

    public org.hl7.fhir.dstu3.model.Resource convert(Bundle bundleDSTU2){
        VersionConvertor_10_30 convertor = new VersionConvertor_10_30(advisor);

        org.hl7.fhir.dstu3.model.Resource resource = convertor.convertResource(bundleDSTU2);
        processResources(resource);
        return resource;
    }

    private void processResources(org.hl7.fhir.dstu3.model.Resource resource) {
        if (resource instanceof org.hl7.fhir.dstu3.model.Bundle) {
            for(org.hl7.fhir.dstu3.model.Bundle.BundleEntryComponent entry : ((org.hl7.fhir.dstu3.model.Bundle) resource).getEntry()) {
                entry.setResource(processEntry(entry.getResource()));
            }
        }
    }

    private org.hl7.fhir.dstu3.model.Resource processEntry(org.hl7.fhir.dstu3.model.Resource resource) {
        // Override profile to prevent validation

        resource.setMeta(new Meta());
        if (resource instanceof Patient) {
            Patient patient = (Patient) resource;

            for (Extension extension : patient.getExtension()) {
                extension = processExtension(extension);
            }

            for (Identifier identifier : patient.getIdentifier()) {
                for (Extension subExtension : identifier.getExtension()) {
                    subExtension = processExtension(subExtension);
                }
            }
            if (patient.hasContact()) {
                for (Patient.ContactComponent contact : patient.getContact()) {
                    for (CodeableConcept concept : contact.getRelationship()) {
                        processCodeableConcept(concept);
                    }
                }
            }
        }
        if (resource instanceof Encounter) {
            Encounter encounter = (Encounter) resource;
            encounter.setType(new ArrayList<>());
            encounter.setPriority(null);
        }
        if (resource instanceof Condition) {
            Condition condition = (Condition) resource;
            // TODO this is hard coded to Order. Possible error in dstu2 to stu3 conversion
            condition.setClinicalStatus(Condition.ConditionClinicalStatus.ACTIVE);
            condition.setVerificationStatus(null);
        }

        if (resource instanceof ReferralRequest) {
            ReferralRequest referralRequest = (ReferralRequest) resource;
            for(Identifier identifier : referralRequest.getIdentifier()) {
                if (!identifier.hasSystem()) identifier.setSystem("urn:ietf:rfc:3986");
            }
            // TODO this is hard coded to Order. Possible error in dstu2 to stu3 conversion
            referralRequest.setIntent(ReferralRequest.ReferralCategory.ORDER);
        }

        if (resource instanceof QuestionnaireResponse) {
        QuestionnaireResponse questionnaireResponse = (QuestionnaireResponse) resource;
        if (questionnaireResponse.hasIdentifier() && !questionnaireResponse.getIdentifier().hasSystem()) {
            questionnaireResponse.getIdentifier().setSystem("urn:ietf:rfc:3986");
        }
        Integer linkId = 1;
            for (QuestionnaireResponse.QuestionnaireResponseItemComponent item : questionnaireResponse.getItem()) {
                if (!item.hasLinkId()) {
                    item.setLinkId("conv-"+linkId);
                    linkId++;
                }
                Integer sublinkId = 1;
                for (QuestionnaireResponse.QuestionnaireResponseItemComponent subitem : item.getItem()) {
                    if (!subitem.hasLinkId()) {
                        subitem.setLinkId("conv-"+linkId+"-"+sublinkId);
                        sublinkId++;
                    }
                }
            }
        }
        return resource;
    }

    private void processCodeableConcept(CodeableConcept concept) {
        for (Coding code : concept.getCoding()) {
            code.setSystem(getReplaceSystem(code.getSystem()));
        }
    }

    private Extension processExtension(Extension extension) {
        extension.setUrl(getReplaceExtensionUrl(extension.getUrl()));
        for(Extension subExtension : extension.getExtension()) {
            processExtension(subExtension);
        }
        if (extension.getValue() instanceof CodeableConcept) {
            CodeableConcept concept = (CodeableConcept) extension.getValue();
            for (Coding code : concept.getCoding()) {
                code.setSystem(getReplaceSystem(code.getSystem()));
            }
            extension.setValue(concept);
        }
        return extension;
    }

    private String getReplaceExtensionUrl(String url) {
        if (url.contains("https://fhir.hl7.org.uk/StructureDefinition/Extension-CareConnect")) {
            url = url.replace("https://fhir.hl7.org.uk/StructureDefinition/Extension-CareConnect","https://fhir.hl7.org.uk/STU3/StructureDefinition/Extension-CareConnect");
        }
        return url;
    }

    private String getReplaceSystem(String system) {
        // Significant changes
        switch(system) {
            case "https://fhir.hl7.org.uk/CareConnect-PersonRelationshipType-1":
                return "https://fhir.nhs.uk/STU3/CodeSystem/PersonRelationshipType-1";
            default:
        }

        if (system.contains("https://fhir.hl7.org.uk/CareConnect")) {
            system = system.replace("https://fhir.hl7.org.uk/CareConnect","https://fhir.hl7.org.uk/STU3/CodeSystem/CareConnect");
        }
        return system;
    }

}
