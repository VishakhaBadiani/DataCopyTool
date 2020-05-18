package com.example.model;

import java.util.List;

public class SyntheticDataWrapper {
        List<SyntheticJoins> syntheticJoins ;
        List<SyntheticCriteria> syntheticCriteria;
        int synDataCreationCount;


        public SyntheticDataWrapper(){}
        public SyntheticDataWrapper(List<SyntheticJoins> synjn, List<SyntheticCriteria> syncr, int synDataCreationCount){
            this.syntheticJoins = synjn;
            this.syntheticCriteria = syncr;
            this.synDataCreationCount=synDataCreationCount;
        }
        public List<SyntheticJoins> getSyntheticJoins() {
            return syntheticJoins;
        }

        public void setSyntheticJoins(List<SyntheticJoins> syntheticJoins) {
            this.syntheticJoins = syntheticJoins;
        }

        public List<SyntheticCriteria> getSyntheticCriteria() {
            return syntheticCriteria;
        }

        public void setSyntheticCriteria(List<SyntheticCriteria> syntheticCriteria) {
            this.syntheticCriteria = syntheticCriteria;
        }

    public int getSynDataCreationCount() {
        return synDataCreationCount;
    }

    public void setSynDataCreationCount(int synDataCreationCount) {
        this.synDataCreationCount = synDataCreationCount;
    }
}