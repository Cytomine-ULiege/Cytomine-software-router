package be.cytomine.software.algorithms

import be.cytomine.client.models.ProcessingServer
import org.json.simple.JSONObject

abstract class AbstractProcessingServerSelectionAlgorithm {

    Map< ProcessingServer, JSONObject> mapOfProcessingServers

    def abstract getBestProcessingServer()
}
