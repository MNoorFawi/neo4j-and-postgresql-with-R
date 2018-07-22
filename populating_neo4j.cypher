### "///" means that the file is in the import folder in the database parent folder

USING PERIODIC COMMIT
LOAD CSV WITH HEADERS FROM "file:///actor.csv" AS row
CREATE (:Actor {actorID: row.actor_id,
		firstName: row.first_name,
		lastName: row.last_name}); 
		
USING PERIODIC COMMIT
LOAD CSV WITH HEADERS FROM "file:///customer.csv" AS row
CREATE (:Customer {customerID: row.customer_id,
		firstName: row.first_name,
		lastName: row.last_name}); 

USING PERIODIC COMMIT
LOAD CSV WITH HEADERS FROM "file:///category.csv" AS row
CREATE (:Category {categoryID: row.category_id,
		Name: row.name}); 

USING PERIODIC COMMIT
LOAD CSV WITH HEADERS FROM "file:///film.csv" AS row
CREATE (:Film {filmID: row.film_id,
		Title: row.title}); 

CREATE INDEX ON :Film(filmID);
CREATE INDEX ON :Actor(actorID);
CREATE INDEX ON :Category(categoryID);
CREATE INDEX ON :Customer(customerID);

### we use "schema await" to wait until the indexes are online

### having the nodes and the indexes we then create the relationships

# 1st the RENTED relationship between customer and film
USING PERIODIC COMMIT
LOAD CSV WITH HEADERS FROM "file:///customer_film.csv" AS row
MATCH (c:Customer {customerID: row.customer_id})
MATCH (f:Film {filmID: row.film_id})
MERGE (c)-[:RENTED]->(f);

# 2nd the ACTED_IN relationship between actor and film
USING PERIODIC COMMIT
LOAD CSV WITH HEADERS FROM "file:///film_actor.csv" AS row
MATCH (a:Actor {actorID: row.actor_id})
MATCH (f:Film {filmID: row.film_id})
MERGE (a)-[:ACTED_IN]->(f);

# 3rd the OF_GENRE relationship between film and category
USING PERIODIC COMMIT
LOAD CSV WITH HEADERS FROM "file:///film_category.csv" AS row
MATCH (f:Film {filmID: row.film_id})
MATCH (c:Category {categoryID: row.category_id})
MERGE (f)-[:OF_GENRE]->(c);

# 4th the FAN_OF relationship between customer and actor
#only when customer has rented more than two films for an actor
MATCH (c:Customer)-[r:RENTED]->()<-[]-(a:Actor)
WITH Count(r) AS rentals, a, c
WHERE rentals > 2
MERGE (c)-[f:FAN_OF]->(a);

####### QUERYING THE DATABASE ########

# get the total number of fans of Gina
MATCH (g:Actor {FirstName: "Gina"})<-[:FAN_OF]-(c:Customer)
RETURN g.FirstName +" "+ g.lastName AS actor, 
COUNT(c) AS number_of_fans;
### we can also visualize Gina with their fans
MATCH (g:Actor {FirstName: "Gina"})<-[:FAN_OF]-(c:Customer)
RETURN g, c;

# each actor and his/her best fan
MATCH (c:Customer)-[r:RENTED]->()<-[]-(a:Actor)
WITH c, a, COUNT(r) AS rentals
RETURN c.firstName + " " + c.lastName AS customer,
MAX(rentals) AS number_of_rentals,
a.firstName + " " + a.lastName AS actor
ORDER BY number_of_rentals DESC LIMIT 10;

