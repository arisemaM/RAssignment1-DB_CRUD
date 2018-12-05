
# environment setup
setwd("/Users/mac/Documents/assignments/big_data")

library(dict)
library(DBI)
library(RMySQL)


# database connectivity function
connect_db <- function(setup) {
	mydb <- dbConnect(MySQL(), user=config$get("db_user"), password=config$get("db_password"), dbname=config$get("db_name"), host=config$get("db_host"), port=config$get("db_port"))

	return(mydb)
}

# database connectivity setup
config <- dict()

config[["db_user"]] <- "root"
config[["db_password"]] <- "root"
config[["db_name"]] <- "r_assig"
config[["db_host"]] <- "127.0.0.1"
config[["db_port"]] <- 3306

db_instance <- connect_db(config)

# declare global table names and statements
student <- "student"
student_attrib <- "(id INT NOT NULL AUTO_INCREMENT, section_id int(11), first_name varchar(32), age int(11), PRIMARY KEY (id), FOREIGN KEY (section_id) REFERENCES section(id))"
section <- "section"
section_attrib <- "(id INT NOT NULL AUTO_INCREMENT, name varchar(32), room_no int(11), PRIMARY KEY (id))"
assesment <- "assesment"
assesment_attrib <- "(id INT NOT NULL AUTO_INCREMENT, student_id int(11), subject varchar(32), result int(11), grade char(1), PRIMARY KEY (id), FOREIGN KEY (student_id) REFERENCES student(id))"

# drop table if exists
dbExecute(db_instance, paste("DROP TABLE IF EXISTS", student, ",", section, ",", assesment, ";"))

# create table function
create_table <- function(db_instance, table_name, attributes){
	create_table <- paste("create table if not exists ", table_name, attributes, ';')
	dbExecute(db_instance, create_table)
}


# create tables
create_table(db_instance, section, section_attrib)
create_table(db_instance, student, student_attrib)
create_table(db_instance, assesment, assesment_attrib)

# insert into table
all_students <- read.csv("students.csv")
all_sections <- read.csv("sections.csv")
all_assesments <-  read.csv("assesment.csv")

#show sample of data read from csv file
print(all_students)

# execute insert into table from data frame
dbWriteTable(db_instance, value=all_sections, name=section, append=TRUE, row.names=F)
dbWriteTable(db_instance, value=all_students, name=student, append=TRUE, row.names=F)
dbWriteTable(db_instance, value=all_assesments, name=assesment, append=TRUE, row.names=F)

# read from table
select_query <- "select * from "
students <- dbGetQuery(db_instance, paste(select_query, student, ";"))
print(students)

# update tables
student_id <- 2
grade <- 'B'
update_query = paste("UPDATE ", assesment, " SET grade= '", grade, "' where student_id=", student_id, ";", sep="")
dbExecute(db_instance, update_query)

# read from table
all_assesments <- dbGetQuery(db_instance, paste(select_query, assesment, ";"))
print(all_assesments)


# delete from table
delete_query = paste("delete from ", student, "where id=3")
dbExecute(db_instance, delete_query)

# read from table
select_query <- "select * from "
students <- dbGetQuery(db_instance, paste(select_query, student, ";"))
print(students)

# read and update to table
all_assesments <- dbGetQuery(db_instance, paste(select_query, assesment, ";"))
print(all_assesments)


grader <- vector(mode="character", length=length(all_assesments$grade))
grader[all_assesments$result<40 ] <- "F"
grader[all_assesments$result>40 & all_assesments$result<= 55] <- "D"
grader[all_assesments$result>55 & all_assesments$result<= 70] <- "C"
grader[all_assesments$result>70 & all_assesments$result<= 85] <- "B"
grader[all_assesments$result>85 & all_assesments$result<= 100] <- "A"

all_assesments$grade <- grader

dbWriteTable(db_instance, value=all_assesments, name=assesment, append=FALSE, overwrite=TRUE)

# read from table
all_assesments <- dbGetQuery(db_instance, paste(select_query, assesment, ";"))
print(all_assesments)

