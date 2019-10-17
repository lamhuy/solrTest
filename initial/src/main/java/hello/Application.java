package hello;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrQuery.ORDER;
import org.apache.solr.client.solrj.SolrRequest.METHOD;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class Application {

  static String collectionName = "catalog_index";
  static int requestSize = 1;
  static int poolSize = 128;

  public static void main(String[] args) throws IOException, SolrServerException {

    System.out.println("Starting...");

    parsingAurgument(args);

    CloudSolrClient client = newCloudSolrClient();
    client.connect();
    client.setDefaultCollection(collectionName);

    System.out.println("SUCCESSFULLY connected");

    SolrQuery query = new SolrQuery();
    query.setQuery("*:*");
    query.setSort("score ", ORDER.desc);
    query.setStart(Integer.getInteger("0"));
    query.setRows(Integer.getInteger("100"));

    ThreadFactory threadFactory = Executors.defaultThreadFactory();

    ThreadPoolExecutor queryExecutor;
    queryExecutor =
        new ThreadPoolExecutor(
            poolSize,
            poolSize,
            0L,
            TimeUnit.MILLISECONDS,
            new LinkedBlockingDeque<>(poolSize),
            threadFactory,
            new ThreadPoolExecutor.CallerRunsPolicy());

    queryExecutor.prestartAllCoreThreads();

    List<Future<QueryResponse>> futures = new ArrayList<>(128);
    CompletionService<QueryResponse> queryService = new ExecutorCompletionService<>(queryExecutor);

    System.out.println("Starting current requests");
    for (int x = 0; x < requestSize; x++) {
      Callable queryCallable = () -> client.query(query, METHOD.POST);
      futures.add(queryService.submit(queryCallable));
    }

    System.out.println("Getting results");
    for (Future<QueryResponse> completedQuery : futures) {
      try {
        QueryResponse response = completedQuery.get();
        System.out.println("Results size: " + response.getResults().size());
      } catch (InterruptedException | ExecutionException e) {
        System.out.println("Unable to get query response");
        Thread.currentThread().interrupt();
      }
    }

    System.out.println("DONE");
    client.close();
    System.out.println("CLOSED");


  }

  static CloudSolrClient newCloudSolrClient() {
    return new CloudSolrClient.Builder(
            Arrays.asList("10.0.89.11:2181,10.0.852.214:2181,10.0.93.72:2181".split(",")),
            Optional.ofNullable("/solr7"))
        .build();
  }

  static void parsingAurgument(String[] args) {
    Options options = new Options();

    Option sizeInput = new Option("s", "sizeInput", true, "Request Size");
    sizeInput.setRequired(true);
    options.addOption(sizeInput);

    Option collectionInput = new Option("c", "collectionInput", true, "Collection/Alias Name");
    collectionInput.setRequired(true);
    options.addOption(collectionInput);

    CommandLineParser parser = new DefaultParser();
    HelpFormatter formatter = new HelpFormatter();
    CommandLine cmd;

    try {
      cmd = parser.parse(options, args);
      requestSize = Integer.parseInt(cmd.getOptionValue("sizeInput"));
      collectionName = cmd.getOptionValue("collectionInput");
    } catch (ParseException e) {
      System.out.println(e.getMessage());
      formatter.printHelp("load test", options);

      System.exit(1);
    }
  }
}
