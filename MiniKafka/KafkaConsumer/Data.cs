using MongoDB.Driver;
using System;
using System.Collections.Generic;
using System.Text;

using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;
using System.Threading;

namespace KafkaConsumer
{
    public class Data
    {
        private readonly IMongoDatabase mongoDatabase;
        public Data(MongoDBConfig config)
        {
            //MongoClientSettings mongoClientSettings = new MongoClientSettings
            //{
            //    Credential = new MongoCredential("SCRAM-SHA-1", new MongoInternalIdentity(config.Database, config.User), new PasswordEvidence(config.Password))
            //};
            //MongoServerAddress mongoServerAddress = new MongoServerAddress(config.Host, config.Port);
            //mongoClientSettings.Server = mongoServerAddress;
            //MongoClient client = new MongoClient(config.ConnectionString);

            //mongoDatabase = client.GetDatabase(config.Database);
            var client = new MongoClient(config.ConnectionString);
            mongoDatabase = client.GetDatabase(config.Database);
        }
        public IMongoCollection<Message> Msgs => mongoDatabase.GetCollection<Message>("Msgs");

        public async void Insert(string message, CancellationToken cancellationToken)
        {
            try
            {
                await Msgs.InsertOneAsync(new Message() { InternalId = ObjectId.GenerateNewId(), Content = message }, new InsertOneOptions() { BypassDocumentValidation = true }, cancellationToken);

            }
            catch(Exception exception)
            {
                Console.WriteLine(exception.Message);
            }
        }
    }

    public class Message
    {
        [BsonId]
        public ObjectId InternalId { get; set; }
        public string Content { get; set; }
    }

    public class MongoDBConfig
    {
        public string Database { get; set; }
        public string Host { get; set; }
        public int Port { get; set; }
        public string User { get; set; }
        public string Password { get; set; }
        public string ConnectionString
        {
            get
            {
                if (string.IsNullOrEmpty(User) || string.IsNullOrEmpty(Password))
                    return $@"mongodb://{Host}:{Port}";
                return $@"mongodb://mongodb:{Port}";
            }
        }
    }
}
