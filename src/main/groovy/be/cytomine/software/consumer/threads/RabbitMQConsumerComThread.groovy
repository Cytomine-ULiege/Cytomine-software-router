package be.cytomine.software.consumer.threads

import be.cytomine.client.models.Job
import be.cytomine.client.models.ProcessingServer
import be.cytomine.software.CheckingLoadSlurmProcessingServer
import be.cytomine.software.repository.SoftwareManager
import be.cytomine.software.repository.threads.RepositoryManagerThread
import com.google.common.base.Stopwatch
import com.rabbitmq.client.AMQP
import com.rabbitmq.client.Channel
import com.rabbitmq.client.Consumer
import com.rabbitmq.client.Envelope
import com.rabbitmq.client.ShutdownSignalException
import groovy.json.JsonSlurper
import groovy.util.logging.Log4j
import org.json.simple.JSONObject

import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors

@Log4j
class RabbitMQConsumerComThread implements Consumer {

    private JsonSlurper jsonSlurper = new JsonSlurper()
    private Channel channel
    private RepositoryManagerThread repositoryManagerThread

    RabbitMQConsumerComThread(Channel chan, RepositoryManagerThread repo)
    {
        channel=chan
        repositoryManagerThread=repo
    }

    @Override
    void handleConsumeOk(String s) {

    }

    @Override
    void handleCancelOk(String s) {

    }

    @Override
    void handleCancel(String s) throws IOException {

    }

    @Override
    void handleShutdownSignal(String s, ShutdownSignalException e) {

    }

    @Override
    void handleRecoverOk(String s) {

    }

    @Override
    void handleDelivery(String s, Envelope envelope, AMQP.BasicProperties basicProperties, byte[] body) throws IOException {

        if(envelope)
        {
            String message = new String(body, "UTF-8")
            def mapMessage = jsonSlurper.parseText(message)
            log.info(" Received message: ${mapMessage}")

            switch (mapMessage["requestType"]) {

                //this case will check the loads and will redirect the request to execute the job
                case "checkAllLoad":
                    log.info("[Communication] Request checking all load")

                    ProcessingServer chosenPS
                    if(mapMessage["automaticChoiceOfServerEnabled"])
                    {
                        //TODO put the job info in the getMostSuitablePS function... As a result, the algo will use the information!
                        chosenPS=CheckingLoadSlurmProcessingServer.getMostSuitablePS()
                    }
                    else
                    {
                        //in this condition, we'll redirect the request in the good queue
                        //we retrieve the good queue thanks to the jobId
                        def jobId = mapMessage["jobId"]
                        Job jobTmp=new Job().fetch(new Long(jobId))
                        if(jobTmp!=null && jobTmp.getStr("processingServer")!=null)
                        {
                            chosenPS=new ProcessingServer().fetch(new Long(jobTmp.getStr("processingServer")))
                        }
                    }

                    log.info("Processing server chosen: ${chosenPS.getStr("name")} ${chosenPS.getStr("username")}")
                    def mapOfChosenPS = jsonSlurper.parseText(chosenPS.getStr("amqpQueue"))
                    String queueToRedirect=mapOfChosenPS["exchange"]

                    //we inject the request into the queue of the good ps
                    JSONObject requestToSend= new JSONObject(mapMessage)
                    requestToSend.put("requestType","execute" )

                    log.info("Request before sending: ${requestToSend}")
                    channel.basicPublish(queueToRedirect,"", null, requestToSend.toString().getBytes())

                    break

                //this case will check the load for a given processingserver
                case "checkLoadOnePS":
                    log.info("[Communication] Request checkLoadOnePS")

                    def psTmp=mapMessage["processingServerID"]
                    ProcessingServer ps=new ProcessingServer()
                    ps.fetch(new Long(psTmp))

                    Stopwatch timer = Stopwatch.createUnstarted()
                    timer.start()

                    CheckingLoadSlurmProcessingServer.initiateTheSSHConnection(ps)
                    //we'll retrieve the 3 information about the current PS
                    JSONObject jsonToReturn=CheckingLoadSlurmProcessingServer.getFullInformation(ps)

                    //create a message to send to the core
                    jsonToReturn.put("requestType","responseCheckLoadForOnePS" )
                    String exchangeName="exchangeCommunicationRetrieve"
                    channel.basicPublish(exchangeName,"", null, jsonToReturn.toString().getBytes())
                    timer.stop()
                    log.info("execution time of checkLoadOnePS: $timer")

                    break

                case "addProcessingServer":
                    log.info("[Communication] Add a new processing server : " + mapMessage["name"])

                    ProcessingServer processingServer=new ProcessingServer()
                    processingServer.fetch(new Long(mapMessage["processingServerId"] as Long))

                    // Launch the processingServerThread associated to the upon processingServer
                    Runnable processingServerThread = new ProcessingServerThread(channel, mapMessage, processingServer)
                    ExecutorService executorService = Executors.newSingleThreadExecutor()
                    executorService.execute(processingServerThread)
                    break
                case "addSoftwareUserRepository":
                    log.info("[Communication] Add a new software user repository")
                    log.info("============================================")
                    log.info("username          : ${mapMessage["username"]}")
                    log.info("dockerUsername    : ${mapMessage["dockerUsername"]}")
                    log.info("prefix            : ${mapMessage["prefix"]}")
                    log.info("============================================")

                    def softwareManager = new SoftwareManager(mapMessage["username"], mapMessage["dockerUsername"], mapMessage["prefix"], mapMessage["id"])

                    def repositoryManagerExist = false
                    for (SoftwareManager elem : repositoryManagerThread.repositoryManagers) {

                        // Check if the software manager already exists
                        if (softwareManager.gitHubManager.getClass().getName() == elem.gitHubManager.getClass().getName() &&
                                softwareManager.gitHubManager.username == elem.gitHubManager.username &&
                                softwareManager.dockerHubManager.username == elem.dockerHubManager.username) {

                            repositoryManagerExist = true

                            // If the repository manager already exists and doesn't have the prefix yet, add it
                            if (!elem.prefixes.containsKey(mapMessage["prefix"])) {
                                elem.prefixes << [(mapMessage["prefix"]): mapMessage["id"]]
                            }
                            break
                        }
                    }

                    // If the software manager doesn't exist, add it
                    if (!repositoryManagerExist) {
                        synchronized (repositoryManagerThread.repositoryManagers) {
                            repositoryManagerThread.repositoryManagers.add(softwareManager)
                        }
                    }

                    // Refresh all after add
                    repositoryManagerThread.refreshAll()

                    break
                case "refreshRepositories":

                    log.info("[Communication] Refresh all software user repositories")
                    def returnString=repositoryManagerThread.refreshAll()

                    JSONObject jsonToReturn= new JSONObject()

                    //create a message to send to the core
                    jsonToReturn.put("requestType","responseRefreshAllRepositories" )
                    jsonToReturn.put("response",returnString)
                    String exchangeName="exchangeCommunicationRetrieve"
                    channel.basicPublish(exchangeName,"", null, jsonToReturn.toString().getBytes())

                    break
            }
        }
    }
}
