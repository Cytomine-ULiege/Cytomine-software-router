package be.cytomine.software

import be.cytomine.client.collections.Collection
import be.cytomine.client.models.ProcessingServer
import be.cytomine.software.communication.Communication
import be.cytomine.software.communication.SSH
import be.cytomine.software.consumer.Main
import com.google.common.base.Splitter
import com.google.common.collect.Lists

import groovy.util.logging.Log4j
import org.json.simple.JSONObject

@Log4j
class CheckingLoadSlurmProcessingServer {

    static Communication sshForCommunication
    static ProcessingServer processingServer

    static initiateTheSSHConnection(ProcessingServer ps)
    {
        this.processingServer=ps
        def keyFilePath = """${Main.configFile.cytomine.software.sshKeysFile}/${ps.getStr("host")}/${ps.getStr("host")}"""
        sshForCommunication=new SSH(ps.getStr("host"),ps.getInt("port"),ps.getStr("username"),keyFilePath)
        //sshForCommunication=new SSH(ps.getStr("host"),ps.getInt("port"),ps.getStr("username"),"/data/ssh/10.19.99.64/10.19.99.64")

    }
    static def getFullInformation(ProcessingServer ps)
    {
        if(ps!=null)
        {
            List<String> listNamePartition= CheckingLoadSlurmProcessingServer.getListNamesOfPartitions("sinfo -o %R --noheader")
            ArrayList<Map> listOfAllPartitions=CheckingLoadSlurmProcessingServer.getListAllOfPartitions("scontrol show partition ",listNamePartition)
            ArrayList<Map> listOfAllNodes = CheckingLoadSlurmProcessingServer.getListAllOfNodes("scontrol show node ",listOfAllPartitions)
            JSONObject jsonOfTheCurrentPS= CheckingLoadSlurmProcessingServer.makeAFullInformationJSonFromList(listOfAllPartitions,listOfAllNodes)
            return jsonOfTheCurrentPS
        }
    }

    static def getListAllOfNodes(String cmd, def listOfAllPartitions)
    {
        ArrayList<Map> listOfAllNodes = new ArrayList()
        int nbNode=0
        for(int i=0;i<listOfAllPartitions.size();i++)
        {
            Map<String,String> mapTmp=listOfAllPartitions.get(i)
            String nodeTmp=mapTmp.get("Nodes")
            String cmdPartition=cmd+nodeTmp
            def responseForNodeInfo = sshForCommunication.executeCommandWithoutCreateNewSession(cmdPartition)
            responseForNodeInfo=responseForNodeInfo.replaceAll("(?m)^   OS=.*", "")
            String adjusted2 = responseForNodeInfo.replaceAll("\n", "")
            List<String> listOfNodes= Lists.newArrayList(Splitter.on(" ").split(adjusted2))
            Map<String,String> mapTest=new HashMap<String, String>()
            for(int j=0;j<listOfNodes.size();j++)
            {
                if(listOfNodes.get(j)!="")
                {
                    String tmp=listOfNodes.get(j).toString()
                    if(tmp!="AllocTRES=")
                    {
                        String[] parts = tmp.split("=")
                        if(parts[0]!="" && parts[1]!="")
                        {
                            if(parts[0]=="NodeName")//why >0? because ifnot we'll add the first node just after the first field
                            {
                                if(j!=0)//why?because we'll add the first node just after the first field if we don't put this condition
                                {
                                    //new node so, we've to save the map first
                                    listOfAllNodes.add(nbNode,mapTest)
                                    nbNode++
                                    mapTest=new HashMap<String, String>()
                                }
                            }
                            mapTest.put(parts[0], parts[1])
                        }
                    }
                }
            }
            listOfAllNodes.add(nbNode,mapTest)
        }
        return listOfAllNodes
    }

    static def getListAllOfPartitions(String cmd, def listNamesOfPartitions)
    {
        ArrayList<Map> listOfAllPartitions=new ArrayList<>()
        for (int i = 0; i < listNamesOfPartitions.size(); i++) {

            //for each line, we'll retrieve the info of the partition
            if(listNamesOfPartitions.get(i).size()>0)
            {
                String cmdPartition=cmd+listNamesOfPartitions.get(i)
                def responseForPartitionInfo = sshForCommunication.executeCommandWithoutCreateNewSession(cmdPartition)
                String adjusted2 = responseForPartitionInfo.replaceAll("\n", "")
                List<String>  partition= Lists.newArrayList(Splitter.on(" ").split(adjusted2))
                Map<String,String> mapTest=new HashMap<String, String>()
                for(int j=0;j<partition.size();j++)
                {
                    if(partition.get(j)!="")
                    {
                        String tmp=partition.get(j).toString()
                        String[] parts = tmp.split("=")
                        mapTest.put(parts[0], parts[1])
                    }
                }
                listOfAllPartitions.add(i,mapTest)
            }
        }
        return listOfAllPartitions
    }

    static def getListNamesOfPartitions(String cmd)
    {
        def response = sshForCommunication.executeCommandWithoutCreateNewSession(cmd)
        List<String> listNamePartition = Lists.newArrayList(Splitter.on("\n").split(response))
        listNamePartition.remove(listNamePartition.size()-1)
        log.info("executeCommandWithoutCreateNewSession done")
        return listNamePartition
    }

    static void displayMap(def listOfAllPartition, def listOfAllNodes)
    {
        for(int i=0;i<listOfAllPartition.size();i++)
        {
            Map<String,String> mapTest=listOfAllPartition.get(i)
            JSONObject json= new JSONObject(mapTest)
            log.info("$json")
        }
        for(int i=0;i<listOfAllNodes.size();i++)
        {

            Map<String,String> mapTmp=listOfAllNodes.get(i)
            JSONObject json= new JSONObject(mapTmp)
            log.info("$json")
        }
    }

    static def makeAFullInformationJSonFromList(def listOfAllPartition,def listOfAllNodes)
    {
        //we'll create 2 jsonlist. Each list will contains some json files about partitions/nodes
        ArrayList<JSONObject> jsonListPartitions= new ArrayList<>()
        ArrayList<JSONObject> jsonListNodes= new ArrayList<>()
        for(int i=0;i<listOfAllPartition.size();i++)
        {
            Map<String,String> mapTest=listOfAllPartition.get(i)
            JSONObject jsonPartitions= new JSONObject(mapTest)
            jsonListPartitions.add(jsonPartitions)
        }
        for(int i=0;i<listOfAllNodes.size();i++)
        {

            Map<String,String> mapTmp=listOfAllNodes.get(i)
            JSONObject jsonNodes= new JSONObject(mapTmp)
            jsonListNodes.add(jsonNodes)
        }

        JSONObject jsonToReturn= new JSONObject()
        jsonToReturn.put("partitions",jsonListPartitions)
        jsonToReturn.put("nodes",jsonListNodes)

        log.info("To return: $jsonToReturn")
        return jsonToReturn
    }

    static def convertAFullInformationJsonToLists(JSONObject json)
    {
        ArrayList<JSONObject> listOfAllNodes = new ArrayList()
        ArrayList<JSONObject> listOfAllPartitions = new ArrayList()

        listOfAllPartitions=json.get("partitions")
        listOfAllNodes=json.get("nodes")


        for(int i=0;i<listOfAllPartitions.size();i++)
            log.info("${listOfAllPartitions.get(i)}")
        for(int i=0;i<listOfAllNodes.size();i++)
            log.info("${listOfAllNodes.get(i)}")
    }

    static def getMostSuitablePS()
    {
        //for each processing server,we'll create 2 json file... 1 for the nodes and 1 for the partitions.
        // We'll put these files in one JSon and these JSon will be put on a map
        Collection<ProcessingServer> processingServerCollection = Collection.fetch(ProcessingServer.class)
        Map<ProcessingServer,JSONObject> mapOfJSONs= new HashMap<ProcessingServer,JSONObject>()

        for(int i=0;i< processingServerCollection.size();i++)
        {
            ProcessingServer ps=new ProcessingServer()
            ps.fetch(new Long(processingServerCollection.get(i).id))
            initiateTheSSHConnection(ps)
            //we'll retrieve the 3 information about the current PS
            mapOfJSONs.put(ps,getFullInformation(ps))
        }
        log.info("Number of processingServer: ${mapOfJSONs.size()}")
        List<ProcessingServer> listOfKeys = new ArrayList<ProcessingServer>(mapOfJSONs.keySet())
        for(int i=0;i<listOfKeys.size();i++)
        {
            log.info("Processing server: ${listOfKeys.get(i).id}")
            JSONObject jsonTMP=new JSONObject(mapOfJSONs.get(listOfKeys.get(i)))
            log.info("       ${jsonTMP}")
        }

        Long idOfTheChosenPS=ProcessingServerSelectionAlgorithms.basicAlgorithm(mapOfJSONs)
        ProcessingServer chosenPS= new ProcessingServer().fetch(idOfTheChosenPS)
        log.info("Chosen PS: $chosenPS")
        return chosenPS
    }
}

