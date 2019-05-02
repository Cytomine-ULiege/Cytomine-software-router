package be.cytomine.software.processingmethod

import be.cytomine.client.models.ProcessingServer

/*
 * Copyright (c) 2009-2018. Authors: see NOTICE file.
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

import be.cytomine.software.communication.Communication
import be.cytomine.software.consumer.Main
import groovy.util.logging.Log4j

@Log4j
abstract class AbstractProcessingMethod {

    private static final def DEFAULT_RETRY = 3
    protected static final def RETRY_ON_ERROR

    static {
        def result = Main.configFile.cytomine.software.ssh.maxRetries
        if (result.getClass().getName() == "groovy.util.ConfigObject" && result.isEmpty()) {
            RETRY_ON_ERROR = DEFAULT_RETRY
        } else {
            RETRY_ON_ERROR = result
        }

        log.info("Retries : ${RETRY_ON_ERROR}")
    }

    Communication communication

    static def newInstance(String classname) {
        if (classname == null) {
            throw new Exception("The class name [${classname}] doesn't refer to a class !")
        }

        def instance = Class
                .forName("be.cytomine.software.processingmethod." + classname)
                .newInstance()

        return instance
    }

    def abstract executeJob(def command, def serverParameters, def workingDirectory)

    def abstract isAlive(def jobId)

    def abstract retrieveLogs(def jobId, def outputFile, def workingDirectory)

    def abstract killJob(def jobId)

    def abstract getFullInformation(ProcessingServer ps)
    def abstract initiateTheSSHConnection(ProcessingServer ps)
    def abstract checkValidityOfProcessingServer(ProcessingServer ps)
}
