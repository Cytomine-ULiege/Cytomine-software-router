package be.cytomine.software.repository.threads

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

import be.cytomine.software.consumer.Main
import be.cytomine.software.repository.SoftwareManager
import groovy.util.logging.Log4j

@Log4j
class RepositoryManagerThread implements Runnable {

    private final DEFAULT_REFRESH_RATE = 300

    def refreshRate
    def index = 0
    def repositoryManagers = []

    @Override
    void run() {
        def result = Main.configFile.cytomine.software.repositoryManagerRefreshRate
        if (result.getClass().getName() == "groovy.util.ConfigObject" && result.isEmpty()) {
            refreshRate = DEFAULT_REFRESH_RATE
        } else  {
            refreshRate = result
        }

        log.info("Refresh rate (seconds) : ${refreshRate}")

        while (true) {
            log.info("Amount of repository managers : ${repositoryManagers.size()}")

            if (repositoryManagers.size() > 0) {
                (repositoryManagers.get(index) as SoftwareManager).updateSoftware()

                index = ++index % repositoryManagers.size()
            }

            sleep((refreshRate as Long) * 1000)
        }
    }

    def refresh(def username) {
        log.info("Refreshing the repository manager ${username} !")
        def result = repositoryManagers.find { manager -> manager.gitHubManager.username == username }
        (result as SoftwareManager).updateSoftware()
    }

    def refreshAll() {
        log.info("Refreshing all the repository managers !")
        repositoryManagers.each { manager -> manager.updateSoftware() }
    }

}
