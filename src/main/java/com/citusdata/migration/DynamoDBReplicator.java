/**
 * 
 */
package com.citusdata.migration;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSCredentialsProviderChain;
import com.amazonaws.internal.StaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBStreams;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBStreamsClientBuilder;
import com.amazonaws.services.dynamodbv2.model.ListTablesResult;
import com.citusdata.migration.datamodel.NonExistingTableException;
import com.citusdata.migration.datamodel.TableEmitter;
import com.citusdata.migration.datamodel.TableExistsException;

/**
 * @author marco
 *
 */
public class DynamoDBReplicator {

	private static final Log LOG = LogFactory.getLog(DynamoDBReplicator.class);

	public static void replicate(String url, String pgSchema, boolean struct, String tables, String access, String key) throws SQLException, IOException {
		try {
			boolean replicateSchema = struct;//cmd.hasOption("schema");
			boolean replicateData = !struct;//cmd.hasOption("data");
			boolean replicateChanges = false;//cmd.hasOption("changes");

			boolean useCitus = false;//cmd.hasOption("citus");
			boolean useLowerCaseColumnNames = false;//cmd.hasOption("lower-case-column-names");
			int maxScanRate = 25;//Integer.parseInt(cmd.getOptionValue("scan-rate", "25"));
			int dbConnectionCount = 16;//Integer.parseInt(cmd.getOptionValue("num-connections", "16"));
			String tableNamesString = tables;//cmd.getOptionValue("table");
			String postgresURL = url;//cmd.getOptionValue("postgres-jdbc-url");
			String conversionModeString = ConversionMode.columns.name();//cmd.getOptionValue("conversion-mode", ConversionMode.columns.name());

			ConversionMode conversionMode;
			try {
				conversionMode = ConversionMode.valueOf(conversionModeString);
			} catch (IllegalArgumentException e) {
				throw new ParseException("invalid conversion mode: " + conversionModeString);
			}

			AWSCredentialsProvider credentialsProvider = new AWSCredentialsProviderChain(new StaticCredentialsProvider(new BasicAWSCredentials(access, key)));

			AmazonDynamoDB dynamoDBClient = AmazonDynamoDBClientBuilder.standard().
					withCredentials(credentialsProvider).
					build();

			AmazonDynamoDBStreams streamsClient = AmazonDynamoDBStreamsClientBuilder.standard().
					withCredentials(credentialsProvider).
					build();

			final TableEmitter emitter;
			if (postgresURL != null) {
				List<TableEmitter> emitters = new ArrayList<>();

				for(int i = 0; i < dbConnectionCount; i++) {
					emitters.add(new JDBCTableEmitter(postgresURL));
				}

				emitter = new HashedMultiEmitter(emitters);
			} else {
				emitter = new StdoutSQLEmitter();
			}

			Runtime.getRuntime().addShutdownHook(new Thread() {
				public void run() {
					LOG.info("Closing database connections");
					emitter.close();
				}
			});

			List<DynamoDBTableReplicator> replicators = new ArrayList<>();

			List<String> tableNames = new ArrayList<>();
			if (tableNamesString != null) {
				tableNames = Arrays.asList(tableNamesString.split(","));
			} else {
				ListTablesResult listTablesResult = dynamoDBClient.listTables();
				for(String tableName : listTablesResult.getTableNames()) {
					if (!tableName.startsWith(DynamoDBTableReplicator.LEASE_TABLE_PREFIX)) {
						tableNames.add(tableName);
					}
				}
			}

			ExecutorService executor = Executors.newCachedThreadPool();

			for(String tableName : tableNames) {
				DynamoDBTableReplicator replicator = new DynamoDBTableReplicator(
						dynamoDBClient, streamsClient, credentialsProvider, executor, emitter, tableName, pgSchema);
				replicator.setAddColumnEnabled(true);
				replicator.setUseCitus(useCitus);
				replicator.setUseLowerCaseColumnNames(useLowerCaseColumnNames);
				replicator.setConversionMode(conversionMode);

				replicators.add(replicator);
			}

			if (replicateSchema) {
				for(DynamoDBTableReplicator replicator : replicators) {
					LOG.info(String.format("Constructing table schema for table %s", replicator.dynamoTableName));
					replicator.replicateSchema();
				}
			}

			if (replicateData) {
				List<Future<Long>> futureResults = new ArrayList<Future<Long>>();

				for(DynamoDBTableReplicator replicator : replicators) {
					replicator.replicateSchema();
					LOG.info(String.format("Replicating data for table %s", replicator.dynamoTableName));
					Future<Long> futureResult = replicator.startReplicatingData(maxScanRate);
					futureResults.add(futureResult);
				}

				for(Future<Long> futureResult : futureResults) {
					futureResult.get();
				}
			}

			if (replicateChanges) {
				for(DynamoDBTableReplicator replicator : replicators) {
					LOG.info(String.format("Replicating changes for table %s", replicator.dynamoTableName));
					replicator.startReplicatingChanges();
				}
			} else {
				executor.shutdown();
			}

		} catch (ParseException e) {
			LOG.error(e.getMessage());
			System.exit(3);
		} catch (TableExistsException|NonExistingTableException e) {
			LOG.error(e.getMessage());
			System.exit(1);
		} catch (ExecutionException e) {
			Throwable cause = e.getCause();

			if (cause.getCause() != null) {
				LOG.error(cause.getCause().getMessage());
			} else {
				LOG.error(cause.getMessage());
			}
			System.exit(1);
		} catch (EmissionException e) {
			if (e.getCause() != null) {
				LOG.error(e.getCause().getMessage());
			} else {
				LOG.error(e.getMessage());
			}
			System.exit(1);
		} catch (RuntimeException e) {
			e.printStackTrace();
			System.exit(2);
		} catch (Exception e) {
			LOG.error(e);
			System.exit(1);
		}
	}

}
