
### first we need to install NEO4J database from its website
### to connect with R we have to install RNeo4j package
### at the time of writing this RNeo4j isn't available for newest versions of R 
### so we install an older version with remotes package

# library(remotes)
# install_version('RNeo4j', '1.6.3')

library(RNeo4j)
graph <- startGraph(url = "http://localhost:7474/db/data/", 
                    username="neo4j", password="neodbase")

### get the top customers who rented the most
query <- "MATCH (c:Customer)-[r:RENTED]->()
          RETURN c.FirstName + ' ' + c.lastName AS customer,
            COUNT(r) AS total_rentals 
          ORDER BY total_rentals DESC LIMIT 10;"

(topCustomers <- cypher(graph, query))

### get the most rented movie 
query2 <- "MATCH ()-[r:RENTED]->(f:Film)
           RETURN f.Title AS film, 
             COUNT(r) AS total_rentals
           ORDER BY total_rentals DESC LIMIT 10;"

(topFilms <- cypher(graph, query2))

