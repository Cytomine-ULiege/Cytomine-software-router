package be.cytomine.software

import be.cytomine.client.collections.Collection
import be.cytomine.client.models.ProcessingServer
import org.json.simple.JSONObject

class ProcessingServerSelectionAlgorithms {

    static basicAlgorithm(Map< ProcessingServer,JSONObject> mapOfProcessingServers)
    {

    }

    static basicAlgorithmRandomChoice(Map< ProcessingServer,JSONObject> mapOfProcessingServers)
    {
        Collection<ProcessingServer> processingServers = Collection.fetch(ProcessingServer.class)
        int number=(Math.random() * (((processingServers.size()-1) - 0) + 1)) + 0

        ProcessingServer goodPs=new ProcessingServer().fetch(processingServers.get(number).id)
        return goodPs.id
    }
}