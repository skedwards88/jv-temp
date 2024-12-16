package com.quickstart;

import com.datastax.astra.client.DataAPIClient;
import com.datastax.astra.client.Database;

public class QuickstartConnect {

  /**
   * Connects to a DataStax Astra database. This function retrieves the database endpoint and
   * application token from the environment variables `ASTRA_DB_API_ENDPOINT` and
   * `ASTRA_DB_APPLICATION_TOKEN`.
   *
   * @return an instance of the connected database
   * @throws IllegalStateException if the environment variables `ASTRA_DB_API_ENDPOINT` or
   *     `ASTRA_DB_APPLICATION_TOKEN` are not defined
   */
  public static Database connectToDatabase() {
    String endpoint = System.getenv("ASTRA_DB_API_ENDPOINT");
    String token = System.getenv("ASTRA_DB_APPLICATION_TOKEN");

    if (endpoint == null || token == null) {
      throw new IllegalStateException(
          "Environment variables ASTRA_DB_API_ENDPOINT and ASTRA_DB_APPLICATION_TOKEN must be defined");
    }

    // Create an instance of `DataAPIClient` with your token.
    DataAPIClient client = new DataAPIClient(token);

    // Get the database specified by your endpoint.
    Database database = client.getDatabase(endpoint);

    System.out.println("Connected to database.");

    return database;
  }
}
