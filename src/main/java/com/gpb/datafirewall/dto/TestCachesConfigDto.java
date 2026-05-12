package com.gpb.datafirewall.dto;

import java.util.List;
import java.util.Map;

public class TestCachesConfigDto {

    private String version;
    private Map<String, String> dataset2ControlArea;
    private Map<String, Map<String, List<String>>> controlAreaRules;
    private Map<String, String> errorMessages;
    private Map<String, List<String>> datasetExclusion;
    private Map<String, Boolean> filterFlag;

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public Map<String, String> getDataset2ControlArea() {
        return dataset2ControlArea;
    }

    public void setDataset2ControlArea(Map<String, String> dataset2ControlArea) {
        this.dataset2ControlArea = dataset2ControlArea;
    }

    public Map<String, Map<String, List<String>>> getControlAreaRules() {
        return controlAreaRules;
    }

    public void setControlAreaRules(Map<String, Map<String, List<String>>> controlAreaRules) {
        this.controlAreaRules = controlAreaRules;
    }

    public Map<String, String> getErrorMessages() {
        return errorMessages;
    }

    public void setErrorMessages(Map<String, String> errorMessages) {
        this.errorMessages = errorMessages;
    }

    public Map<String, List<String>> getDatasetExclusion() {
        return datasetExclusion;
    }

    public void setDatasetExclusion(Map<String, List<String>> datasetExclusion) {
        this.datasetExclusion = datasetExclusion;
    }

    public Map<String, Boolean> getFilterFlag() {
        return filterFlag;
    }

    public void setFilterFlag(Map<String, Boolean> filterFlag) {
        this.filterFlag = filterFlag;
    }
}
