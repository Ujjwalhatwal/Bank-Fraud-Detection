#account service /Dockerfile

# step 1 : Use java 17 base image
FROM eclipse-temurin:17-jre-alpine

# step 2 : Set the working directory inside the container
WORKDIR /app

#Step 3 : Copy the jar file from the target directory to the working directory inside the container
COPY target/transaction-service-0.0.1-SNAPSHOT.jar app.jar

#step 4 : Expose the port on which the application will run
EXPOSE 8082

#step 5 : Define the command to run the application
ENTRYPOINT ["java", "-jar", "app.jar"]

