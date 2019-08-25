package edu.buffalo.cse.cse486586.simpledynamo;

public class DynamoModel {

    private String ownPort;
    private String successor1;
    private String successor2;
    private String predecessor;

    private String data_own;
    private String data_succ1;
    private String data_succ2;

    private String ownHash;
    private String succ1Hash;
    private String succ2Hash;
    private String predHash;

    public DynamoModel() {

    }

    public String getOwnPort() {
        return ownPort;
    }

    public void setOwnPort(String ownPort) {
        this.ownPort = ownPort;
    }

    public String getSuccessor1() {
        return successor1;
    }

    public void setSuccessor1(String successor1) {
        this.successor1 = successor1;
    }

    public String getSuccessor2() {
        return successor2;
    }

    public void setSuccessor2(String successor2) {
        this.successor2 = successor2;
    }

    public String getPredecessor() {
        return predecessor;
    }

    public void setPredecessor(String predecessor) {
        this.predecessor = predecessor;
    }

    public String getData_own() {
        return data_own;
    }

    public void setData_own(String data_own) {
        this.data_own = data_own;
    }

    public String getData_succ1() {
        return data_succ1;
    }

    public void setData_succ1(String data_succ1) {
        this.data_succ1 = data_succ1;
    }

    public String getData_succ2() {
        return data_succ2;
    }

    public void setData_succ2(String data_succ2) {
        this.data_succ2 = data_succ2;
    }

    public String getOwnHash() {
        return ownHash;
    }

    public void setOwnHash(String ownHash) {
        this.ownHash = ownHash;
    }

    public String getSucc1Hash() {
        return succ1Hash;
    }

    public void setSucc1Hash(String succ1Hash) {
        this.succ1Hash = succ1Hash;
    }

    public String getSucc2Hash() {
        return succ2Hash;
    }

    public void setSucc2Hash(String succ2Hash) {
        this.succ2Hash = succ2Hash;
    }

    public String getPredHash() {
        return predHash;
    }

    public void setPredHash(String predHash) {
        this.predHash = predHash;
    }
}
