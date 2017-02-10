rm -rf javadoc

mkdir  -p javadoc

project_version=$(mvn org.apache.maven.plugins:maven-help-plugin:2.1.1:evaluate -Dexpression=project.version | egrep -v '^\[|Downloading:' | sed 's/[^0-9\.]//g' | awk 1 ORS='')

javadoc -sourcepath ./src/main/java/ -subpackages com/mcafee/dxl/streaming/operations/client -d ./javadoc -classpath ./target/databus-mgmt-sdk-${project_version}-jar-with-dependencies.jar
