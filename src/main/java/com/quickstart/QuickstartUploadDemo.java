package com.quickstart;

import com.datastax.astra.client.Collection;
import com.datastax.astra.client.Database;
import com.datastax.astra.client.model.CollectionOptions;
import com.datastax.astra.client.model.Document;
import com.datastax.astra.client.model.InsertManyResult;
import com.datastax.astra.client.model.SimilarityMetric;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

public class QuickstartUploadDemo {
  /**
   * Creates a collection in the specified database with vectorization enabled.
   * The collection will use Nvidia's NV-Embed-QA embedding model to generate
   * vector embeddings for data in the collection.
   *
   * @param database the instantiated object that represents the database where
   * the collection will be created
   * @param collectionName the name of the collection to create
   * @return the collection instance that was created
   */
  public static Collection<Document> createCollection(Database database, String collectionName) {
    CollectionOptions.CollectionOptionsBuilder builder =
        CollectionOptions.builder()
            .vectorSimilarity(SimilarityMetric.COSINE)
            .vectorize("nvidia", "NV-Embed-QA");

    Collection<Document> collection = database.createCollection(collectionName, builder.build());

    System.out.println("Created collection.");

    return collection;
  }

  /**
   * Uploads data from a file containing a JSON array to the specified collection. For each piece of
   * data, a $vectorize field is added. The $vectorize value is a string from which vector
   * embeddings will be generated.
   * This function uses the Jackson library to process the JSON data.
   *
   * @param collection the collection instance to upload the data to
   * @param dataFilePath the path to a JSON file containing a JSON array
   * @param embeddingStringCreator a function to create the string for which
   *     vector embeddings will be generated
   * @throws IOException if an I/O error occurs reading from the file
   */
  public static void uploadJsonData(
      Collection<Document> collection,
      String dataFilePath,
      Function<JsonNode, String> embeddingStringCreator)
      throws IOException {

    // Initialize Jackson ObjectMapper
    ObjectMapper objectMapper = new ObjectMapper();

    // Read the JSON file and parse it into a JSON array (ArrayNode).
    String rawData = Files.readString(Paths.get(dataFilePath), StandardCharsets.UTF_8);
    ArrayNode jsonData = (ArrayNode) objectMapper.readTree(rawData);

    // Convert the data to a list of Documents, and
    // add a $vectorize field to each piece of data.
    List<Document> documents = new ArrayList<>();
    for (JsonNode data : jsonData) {
      if (data instanceof ObjectNode) {
        ((ObjectNode) data).put("$vectorize", embeddingStringCreator.apply(data));
      }

      documents.add(Document.parse(data.toString()));
    }

    // Upload the data.
    InsertManyResult result = collection.insertMany(documents);
    System.out.println("Inserted " + result.getInsertedIds().size() + " items.");
  }

  public static void main(String[] args) {
    Database database = QuickstartConnect.connectToDatabase();
    Collection<Document> collection =
        createCollection(
            database, "quickstart_collection5"
        );
    try {
      uploadJsonData(
          collection,
          "src/main/resources/quickstart_dataset.json",
          data -> {
            String summary = data.path("summary").asText("");
            String genres = String.join(", ",
                data.path("genres").findValuesAsText(""));
            return "summary: " + summary + " | genres: " + genres;
          });
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
