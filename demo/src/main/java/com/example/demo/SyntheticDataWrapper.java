package com.example.demo;

import java.util.List;

public class SyntheticDataWrapper {
        List<SyntheticJoins> syntheticJoins ;
        List<SyntheticCriteria> syntheticCriteria;


        public SyntheticDataWrapper(){}
        public SyntheticDataWrapper(List<SyntheticJoins> synjn, List<SyntheticCriteria> syncr){
            this.syntheticJoins = synjn;
            this.syntheticCriteria = syncr;
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

}