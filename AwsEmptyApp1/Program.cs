using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

using Amazon;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using Amazon.DynamoDBv2.DocumentModel;

// Add using statements to access AWS SDK for .NET services. 
// Both the Service and its Model namespace need to be added 
// in order to gain access to a service. For example, to access
// the EC2 service, add:
// using Amazon.EC2;
// using Amazon.EC2.Model;

namespace AwsEmptyApp1
{
    class Program
    {
        public static void Main(string[] args)
        {
            //Class1.TableQuery("Student");
            //Class1.TableScan("Student");
            Class1.UpdateConditionally("Student");
            Console.ReadKey();
            Console.WriteLine();

            Console.Read();
        }

        public static void DisplayMovieItem(AmazonDynamoDBClient client, string year, string title)
        {
            // Create Primitives for the HASH and RANGE portions of the primary key
            Primitive hash = new Primitive(year, true);
            Primitive range = new Primitive(title, false);

            Table table = null;
            try { table = Table.LoadTable(client, "Movies"); }
            catch (Exception ex) {
                Console.WriteLine("\n Error: failed to load the 'Movies' table; " + ex.Message);
                return;
            }
            Document document = table.GetItem(hash, range);
            Console.WriteLine("\n The movie record looks like this: \n" + document.ToJsonPretty());
        }

        public static void CreateTable()
        {
            string tableName = "ProductCatalog";
            // 表结构
            List<KeySchemaElement> keySchema = new List<KeySchemaElement>();
            keySchema.Add(new KeySchemaElement() { AttributeName = "Id", KeyType = "HASH" });//Partition key(分区键)
            keySchema.Add(new KeySchemaElement() { AttributeName = "Title", KeyType = "RANGE" });//Sort key(排序键)
            // 属性描述
            List<AttributeDefinition> attributeDefinition = new List<AttributeDefinition>();
            attributeDefinition.Add(new AttributeDefinition() { AttributeName = "Id", AttributeType = "N" });
            attributeDefinition.Add(new AttributeDefinition() { AttributeName = "Title", AttributeType = "S" });
            // 预留吞吐量
            ProvisionedThroughput provisionedThroughput = new ProvisionedThroughput() { ReadCapacityUnits = 1, WriteCapacityUnits = 1 };

            DynamoDBHelper.CreateTable(tableName, keySchema, attributeDefinition, provisionedThroughput);
        }
    }
}