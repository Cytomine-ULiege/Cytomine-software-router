package be.cytomine.software.algorithms

import be.cytomine.client.collections.Collection
import be.cytomine.client.models.ProcessingServer
import org.json.simple.JSONObject

import groovy.util.logging.Log4j

@Log4j
class BasicAlgorithmRandomChoice extends AbstractProcessingServerSelectionAlgorithm
{

    BasicAlgorithmRandomChoice(Map< ProcessingServer, JSONObject> mapOfProcessingServers)
    {
        this.mapOfProcessingServers=mapOfProcessingServers
    }

    @Override
    def getBestProcessingServer()
    {
        try
        {
            Collection<ProcessingServer> processingServers = Collection.fetch(ProcessingServer.class)
            int number=(Math.random() * (((processingServers.size()-1) - 0) + 1)) + 0
            ProcessingServer goodPs=new ProcessingServer().fetch(processingServers.get(number).id)

            return goodPs.id
        }
        catch(Exception ex)
        {
            log.info("error  ${ex.printStackTrace()}")
        }

    }
}
