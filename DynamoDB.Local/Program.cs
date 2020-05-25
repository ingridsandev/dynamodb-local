using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Amazon;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;

namespace AwsDynamoDbReplicateSchema
{
    class Program
    {
        static async Task Main()
        {
            var args = GetArguments();

            // Create server client
            var serverClient = new AmazonDynamoDBClient(args.AwsAccessKeyId, args.AwsSecretAccessKey,
                new AmazonDynamoDBConfig
                {
                    RegionEndpoint = RegionEndpoint.EUWest1
                });
            
            // Create local client
            var localClient = new AmazonDynamoDBClient(
                new AmazonDynamoDBConfig
                {
                    RegionEndpoint = RegionEndpoint.EUWest1,
                    ServiceURL = args.ServiceUrl
                });

            try
            {
                foreach (var table in args.Tables)
                {
                    // Issue describeTable request and retrieve the table description, name and fields
                    var serverResponse = await serverClient.DescribeTableAsync(new DescribeTableRequest
                    {
                        TableName = table
                    });
                
                    // Use describe table result from the server (aws) to create the tables with the same structure locally
                    var localResponse = await localClient.CreateTableAsync(new CreateTableRequest
                    {
                        AttributeDefinitions = serverResponse.Table.AttributeDefinitions,
                        ProvisionedThroughput = new ProvisionedThroughput
                        {
                            ReadCapacityUnits = serverResponse.Table.ProvisionedThroughput.ReadCapacityUnits,
                            WriteCapacityUnits = serverResponse.Table.ProvisionedThroughput.WriteCapacityUnits
                        },
                        TableName = serverResponse.Table.TableName,
                        KeySchema = serverResponse.Table.KeySchema
                    });
                
                    Console.WriteLine($"Table: {localResponse.TableDescription.TableName} HttpStatusCode: {localResponse.HttpStatusCode}");
                }
            }
            catch (Exception e)
            {
                Console.WriteLine($"something occurred while creating local tables: Exception: {e}");
                throw;
            }
        }

        private static Arguments GetArguments()
        {
            var result = new Arguments
            {
                AwsAccessKeyId = Environment.GetEnvironmentVariable("AWS_ACCESS_KEY_ID"),
                AwsSecretAccessKey = Environment.GetEnvironmentVariable("AWS_SECRET_ACCESS_KEY"),
                ServiceUrl = Environment.GetEnvironmentVariable("SERVICE_URL"),
                Tables = Environment.GetEnvironmentVariable("TABLES")?.Split(";")
            };

            var missingEnvVars = new StringBuilder();

            missingEnvVars.Append(null == result.AwsAccessKeyId ? "[AWS_ACCESS_KEY_ID]" : null);
            missingEnvVars.Append(null == result.AwsSecretAccessKey ? "[AWS_SECRET_ACCESS_KEY]" : null);
            missingEnvVars.Append(null == result.ServiceUrl ? "[SERVICE_URL]" : null);
            missingEnvVars.Append(null == result.Tables ? "[TABLES]" : null);

            if (missingEnvVars.Length > 0)
            {
                throw new Exception($"Missing following environment variables: {missingEnvVars.ToString()}");
            }

            return result;
        }

        private class Arguments
        {
            public string AwsAccessKeyId { get; set; }
            public string AwsSecretAccessKey { get; set; }
            public string ServiceUrl { get; set; }
            public IEnumerable<string> Tables { get; set; }
        }
    }
}