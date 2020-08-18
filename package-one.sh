echo "---- modules: ----"
module=${module:-"spark-examples_2.11"}
echo ${module}
mvn clean package -pl :${module} -DskipTests -Dmaven.test.skip=true
