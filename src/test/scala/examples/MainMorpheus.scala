package examples

import org.apache.spark.sql.SparkSession
import org.opencypher.morpheus.api.MorpheusSession
import org.opencypher.morpheus.api.io.MorpheusElementTable
import org.opencypher.okapi.api.io.conversion.{NodeMappingBuilder, RelationshipMappingBuilder}
import org.opencypher.okapi.api.value.CypherValue
import org.slf4j.LoggerFactory

object MainMorpheus extends App {

  private val LOGGER = LoggerFactory.getLogger(this.getClass.getName)

  LOGGER.info("Creating Spark Session")
  val spark = SparkSession
    .builder()
    .appName(s"${this.getClass.getSimpleName}")
    .config("spark.master", "local[*]")
    .config("spark.sql.legacy.allowUntypedScalaUDF", "true")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  LOGGER.info("Creating Morpheus session")
  implicit val morpheus: MorpheusSession = MorpheusSession.create(spark)

  LOGGER.debug("Reading csv files into data frames")

  def loadArangoDF(collectionName: String) = {
    spark.read
      .format("org.apache.spark.sql.arangodb.datasource")
      .options(Map(
        "collection" -> collectionName,
        "db" -> "_system",
        "user" -> "root",
        "password" -> "test",
        "endpoints" -> "172.28.3.1:8529,172.28.3.2:8529,172.28.3.3:8529"
      ))
      .load()
  }

  val moviesDF = loadArangoDF("movies")
  val personsDF = loadArangoDF("persons")
  val actsInDF = loadArangoDF("actsIn")

  LOGGER.info("Creating element mapping for movies node")
  val movieNodeMapping = NodeMappingBuilder
    .withSourceIdKey("_id")
    .withImpliedLabel("Movies")
    .withPropertyKey(propertyKey = "title", sourcePropertyKey = "title")
    .withPropertyKey(propertyKey = "tagline", sourcePropertyKey = "tagline")
    .withPropertyKey(propertyKey = "description", sourcePropertyKey = "description")
    .withPropertyKey(propertyKey = "runtime", sourcePropertyKey = "runtime")
    .build

  LOGGER.info("Creating element mapping for person node")
  val personNodeMapping = NodeMappingBuilder
    .withSourceIdKey("_id")
    .withImpliedLabel("Person")
    .withPropertyKey("name", "name")
    .build

  LOGGER.info("Creating element mapping for the edge between two nodes")
  val actedInRelationMapping = RelationshipMappingBuilder
    .withSourceIdKey("_id")
    .withSourceStartNodeKey("_from")
    .withSourceEndNodeKey("_to")
    .withRelType("ACTS_IN")
    .withPropertyKey("name", "name")
    .build

  LOGGER.info("Creating nodes and edges using mapping")
  val moviesNode = MorpheusElementTable.create(movieNodeMapping, moviesDF)
  val personsNode = MorpheusElementTable.create(personNodeMapping, personsDF)
  val actedInRelation = MorpheusElementTable.create(actedInRelationMapping, actsInDF)

  LOGGER.info("Creating Property Graph")
  val actorMovieGraph = morpheus.readFrom(personsNode, moviesNode, actedInRelation)


  LOGGER.info("Query Property Graph for the names of the actor nodes")
  val actors = actorMovieGraph.cypher(
    "MATCH (p:Person) return p.name AS Actor_Name"
  )
  actors.records.show

  LOGGER.info("Query to get titles of all Movie nodes")
  val movies = actorMovieGraph.cypher(
    "MATCH (m:Movies) return m.title AS Movie_Titles"
  )
  movies.records.show

  LOGGER.info("Query to read all actors and their respective movies")
  val actor_movies = actorMovieGraph.cypher(
    "MATCH (p:Person)-[a:ACTS_IN]->(m:Movies) RETURN p.name AS ACTOR_NAME, m.title AS MOVIE_TITLE"
  )
  actor_movies.records.show

  LOGGER.info("Query to read movie belonging to a particular actor")
  val movie = actorMovieGraph.cypher(
    "MATCH (p:Person{name:'Gloria Foster'}) -[a:ACTS_IN]->(m:Movies) RETURN m.title AS MOVIE_TITLE"
  )
  movie.records.show

  LOGGER.info("Query with Parameter Substitution")
  val param = CypherValue.CypherMap(("movie_name", "The Matrix Revolutions"))
  val actorName = actorMovieGraph.cypher(
    s"MATCH (m:Movies{title:{movie_name}})<-[a:ACTS_IN]-(p:Person) RETURN p.name AS ACTOR_NAME",
    param)
  actorName.records.show

}
