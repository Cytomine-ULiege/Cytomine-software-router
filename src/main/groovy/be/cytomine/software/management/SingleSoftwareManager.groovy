package be.cytomine.software.management

/*
 * Copyright (c) 2009-2020. Authors: see NOTICE file.
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
import be.cytomine.software.boutiques.Interpreter
import be.cytomine.software.consumer.Main
import be.cytomine.software.util.Utils
import groovy.util.logging.Log4j
import org.apache.commons.lang.RandomStringUtils

@Log4j
class SingleSoftwareManager extends AbstractSoftwareManager {

    File source

    SingleSoftwareManager(Long softwareId, String release, File source) throws ClassNotFoundException {
        this.softwareId = softwareId
        this.release = release
        if(source.exists()){

            this.source = new File(Main.configFile.cytomine.software.path.softwareImages,
                    RandomStringUtils.random(13,  (('A'..'Z') + ('0'..'0')).join().toCharArray()))
            this.source.mkdir()

            def process = Utils.executeProcess("unzip "+source.path, this.source)
            if (process.exitValue() == 0) {
                log.info("The source code has successfully been unzipped !")
            } else {
                log.info("The source code has not been unzipped !")
                log.error(process.text)
            }
        }
    }

    @Override
    protected File retrieveDescriptor() {
        File descriptor
        source.traverse(type: groovy.io.FileType.FILES) {
            if(it.name == Main.configFile.cytomine.software.descriptorFile) descriptor = it
        }

        return descriptor
    }
    @Override
    protected String generateSingularityBuildingCommand(Interpreter interpreter){
        def imageName = interpreter.getImageName() + "-" + release as String

        File dockerOrigin

        if(Main.configFile.cytomine.software.allowDockerfileCompilation as Boolean) {
            source.traverse(type: groovy.io.FileType.FILES) {
                if(it.name == "Dockerfile") dockerOrigin = it
            }
            return 'docker build -t '+interpreter.getImageName() + ':' + release+' '+dockerOrigin.parentFile.absolutePath+' && singularity pull --name ' + imageName + '.simg docker-daemon://' +
                    interpreter.getImageName() + ':' + release as String
        }

        source.traverse(type: groovy.io.FileType.FILES) {
            if(it.name == "image.tar") dockerOrigin = it
        }

        return 'singularity build ' + imageName + '.simg docker-archive:'+dockerOrigin.absolutePath
    }

    protected void checkDescriptor(Interpreter interpreter) {

    }

    protected void cleanFiles() {
        //cleanFiles(source)
    }

    protected String getSourcePath() {
        return null
    }
}
