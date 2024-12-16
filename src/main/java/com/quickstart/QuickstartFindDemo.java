package com.quickstart;

import static com.datastax.astra.client.model.Projections.include;

import com.datastax.astra.client.Collection;
import com.datastax.astra.client.Database;
import com.datastax.astra.client.model.Document;
import com.datastax.astra.client.model.Filter;
import com.datastax.astra.client.model.Filters;
import com.datastax.astra.client.model.FindOneOptions;
import com.datastax.astra.client.model.FindOptions;

public class QuickstartFindDemo {

  public static void main(String[] args) {

    Database database = QuickstartConnect.connectToDatabase();

    Collection<Document> collection = database.getCollection(
      "quickstart_collection5"
    );

    // Find documents that match a filter
    Filter filter = Filters.gt("rating", 4.7);

    FindOptions options = new FindOptions().limit(10);

    collection
        .find(filter, options)
        .forEach(
            document -> {
              System.out.println(
                  document.getString("title") + " is rated " + document.get("rating"));
            });

    // Perform a vector search to find the closest match to a search string
    FindOneOptions options2 = new FindOneOptions().sort("A scary novel");

    collection
        .findOne(options2)
        .ifPresent(
            document -> {
              System.out.println("Here is a scary novel: " + document.getString("title"));
            });

    // Combine a filter, vector search, and projection to find the 3 books with
    // more than 400 pages that are the closest matches to a search string,
    // and just return the title and author
    Filter filter3 = Filters.gt("numberOfPages", 400);

    FindOptions options3 =
        new FindOptions()
            .limit(3)
            .sort("A book set in the arctic")
            .projection(include("title", "author"));

    System.out.println("Here are some books set in the arctic:");
    collection
        .find(filter3, options3)
        .forEach(
            document -> {
              System.out.println(document);
            });
  }
}
