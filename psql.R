### loading the libraries 
library(RPostgreSQL)
library(DBI)

## connecting to the database with the credentials
pw <- "password"
drv <- dbDriver("PostgreSQL")
conn <- dbConnect(drv, dbname = "dvdrental", 
                  host = "localhost", port = 5432,
                  password = pw, user = "postgres")

## query some tables
dbGetQuery(conn, "SELECT * FROM language")
dbListTables(conn)
dbReadTable(conn, "city")

### getting the files to write it to disk
category <- dbGetQuery(conn, "SELECT * FROM category;")
customer <- dbGetQuery(conn, "SELECT * FROM customer;")
film_category <- dbGetQuery(conn, "SELECT * FROM film_category;")
film <- dbGetQuery(conn, "SELECT film_id, title, language_id, rating, rental_rate FROM film;")
actor <- dbGetQuery(conn, "SELECT * FROM actor;")
language <- dbGetQuery(conn, "SELECT * FROM language;")
film_actor <- dbGetQuery(conn, "SELECT * FROM film_actor;")

write.csv(category, "category.csv")
write.csv(customer, "customer.csv")
write.csv(film, "film.csv")
write.csv(film_category, "film_category.csv")
write.csv(actor, "actor.csv")
write.csv(language, "language.csv")
write.csv(film_actor, "film_actor.csv")

### querying the relationships
query <- "SELECT customer.customer_id, customer.first_name, customer.last_name, 
            inventory.inventory_id, film.film_id, film.title FROM customer  
            FULL JOIN rental ON customer.customer_id = rental.customer_id
            FULL JOIN inventory ON rental.inventory_id = inventory.inventory_id
            FULL JOIN film ON inventory.film_id = film.film_id;"

customer_to_film <- dbGetQuery(conn, query)

query2 <- "SELECT customer.customer_id, 
inventory.inventory_id, film.film_id FROM customer  
FULL JOIN rental ON customer.customer_id = rental.customer_id
FULL JOIN inventory ON rental.inventory_id = inventory.inventory_id
FULL JOIN film ON inventory.film_id = film.film_id;"

customer_film <- dbGetQuery(conn, query2)
head(customer_film)
write.csv(customer_film, "customer_film.csv")

dbDisconnect(conn)





