# Reliable news dissemination over a gradient topology
Project at KTH-ICT - teachers A. Ormenisan, J. Dowling.

**[WORK IN PROGRESS]**

How to run the simulation from command line:
 
> The pom file contains the shade plugin which will create a new jar when you run the install phase. The jar is located in the target forlder and has "-shaded.jar" sufix

```sh
mvn clean install
```

> In order to run from command line you need to specify the location of the log4j.properties file and the config file.
> 
> One recurring problem is relative paths not working. Use absolute paths.

```sh
java -Dlog4j.configuration=file:${LOG4J_PROPERTIES_PATH} -Dconfig.file=${CONFIG_FILE_PATH} -jar ${JAR}
```
