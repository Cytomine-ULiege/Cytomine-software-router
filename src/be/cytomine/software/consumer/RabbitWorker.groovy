package src.be.cytomine.software.consumer

import be.cytomine.client.Cytomine
import be.cytomine.client.CytomineException
import be.cytomine.client.collections.AmqpQueueCollection
import com.rabbitmq.client.ConnectionFactory
import com.rabbitmq.client.Channel
import com.rabbitmq.client.Connection
import groovy.util.logging.Log4j

import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
/*
 * Copyright (c) 2009-2015. Authors: see NOTICE file.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
@Log4j
class RabbitWorker {

    static def configFile = new ConfigSlurper().parse(new File("config.groovy").toURI().toURL())
    static Connection connection
    static Channel channel
    static def listQueues = []
    static Cytomine cytomine

    public static void main(String[] args) {

        log.info "GROOVY_HOME : " + System.getenv("GROOVY_HOME")
        log.info "PATH : " + System.getenv("PATH")

        // Create the directory for logs
        def logsDirectory = new File((String) configFile.logsDirectory)
        if (!logsDirectory.exists()) {
            logsDirectory.mkdirs()
        }

        // Create the directory for software data
        def dataDirectory = new File((String) configFile.dataDirectory)
        if (!dataDirectory.exists()) {
            dataDirectory.mkdirs()
        }

        // Cytomine connection
        cytomine = new Cytomine(configFile.cytomineCoreURL as String, configFile.publicKey as String, configFile.privateKey as String)

        ping()

        // Retrieve all the software existing on the core
        getAlreadyExistingSoftwares()

        createRabbitConnection()

        launchThreadQueueCommunication()

        launchThreadsForAlreadyExistingSoftwares()
    }

    static void ping()  {
        int limit = 5
        int i=0
        while (i < limit){
            try {
                log.info("Connected as " + cytomine.getCurrentUser().get("username"))
                break
            } catch (CytomineException e) {
                log.error("Connection not established. Retry : "+i)
                i++
                sleep(30*1000)
            }
        }
    }


    static getAlreadyExistingSoftwares() {
        AmqpQueueCollection amqpCollection = cytomine.getAmqpQueue()
        for (int i = 0; i < amqpCollection.size(); i++) {
            if (!amqpCollection.get(i).getStr("name").equals("queueCommunication")) {
                listQueues << [name: amqpCollection.get(i).getStr("name"), host: amqpCollection.get(i).getStr("host"), exchange: amqpCollection.get(i).getStr("exchange")]
            }
        }
    }

    static createRabbitConnection() {
        ConnectionFactory factory = new ConnectionFactory()
        factory.setHost(configFile.rabbitAddress as String)
        channel = null
        try {
            factory.setUsername(configFile.rabbitUsername as String)
            factory.setPassword(configFile.rabbitPassword as String)
            connection = factory.newConnection()
            channel = connection.createChannel()
        } catch (IOException e) {
            e.printStackTrace()
        }
    }

    static launchThreadQueueCommunication() {
        Runnable threadQueueCommunication = new ThreadQueueCommunication(channel: channel, queueName: configFile.queueCommunication as String, exchangeName: configFile.exchangeCommunication as String)
        ExecutorService execute = Executors.newSingleThreadExecutor()
        execute.execute(threadQueueCommunication)
    }

    static launchThreadsForAlreadyExistingSoftwares() {

        listQueues.each { queue ->
            Runnable rabbitWorkerThread = new RabbitWorkerThread(mapMessage: queue, channel: channel)
            ExecutorService execute = Executors.newSingleThreadExecutor()
            execute.execute(rabbitWorkerThread)
        }
    }

}